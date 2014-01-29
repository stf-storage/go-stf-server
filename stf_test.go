package stf

import (
  "bufio"
  "fmt"
  "github.com/lestrrat/go-test-mysqld"
  "io"
  "io/ioutil"
  "log"
  "os"
  "os/exec"
  "path/filepath"
  "math/rand"
  "net"
  "net/http"
  "net/http/httptest"
  "runtime"
  "runtime/debug"
  "strings"
  "testing"
  "time"
)

type TestEnv struct {
  Test        *testing.T
  Guards      []func()
  WorkDir     string
  ConfigFile  *os.File
  Mysqld      *mysqltest.TestMysqld
  MysqlConfig *DatabaseConfig
  MemdPort    int
  QueueConfig *QueueConfig
}

type TestDatabase struct {
  Config  *DatabaseConfig
  Socket  string
  DataDir string
  PidFile string
  TmpDir  string
}

func NewTestEnv (t *testing.T) (*TestEnv) {
  env := &TestEnv {}
  env.Test = t
  return env
}

func (self *TestEnv) Setup () {
  self.createTemporaryDir()
  self.startDatabase()
  self.startQueue()
  self.startMemcached()
  self.createTemporaryConfig()
  self.startTemporaryStorageServers()
  self.startWorkers()
}

func (self *TestEnv) Release () {
  for _, f := range self.Guards {
    f()
  }
}

func (self *TestEnv) Errorf(format string, args ...interface {}) {
  self.Test.Errorf(format, args...)
}

func (self *TestEnv) FailNow(format string, args ...interface {}) {
  self.Test.Errorf(format, args...)
  debug.PrintStack()
  self.Test.FailNow()
}

func (self *TestEnv) Logf(format string, args ...interface {}) {
  self.Test.Logf(format, args...)
}

func AssertDir(dir string) {
  _, err := os.Stat(dir)
  if err == nil {
    return // XXX not checking if dir is a directory
  }

  if ! os.IsNotExist(err) {
    panic(fmt.Sprintf("Error while asserting directory %s: %s", dir, err))
  }

  err = os.MkdirAll(dir, 0777)
  if err != nil {
    panic(fmt.Sprintf("Failed to create directory %s: %s", dir, err))
  }
}

func (self *TestEnv) AddGuard(cb func()) {
  self.Guards = append(self.Guards, cb)
}

func (self *TestEnv) startMemcached()  {
  port := 0
  for p := 50000 + rand.Intn(1000); p < 60000; p++ {
    l, err := net.Listen("tcp", fmt.Sprintf(":%d", p))
    if err == nil {
      l.Close()
      port = p
      break
    }
  }

  if port == 0 {
    self.FailNow("Could not find a port to start memcached")
  }

  self.MemdPort = port
  self.startBackground("memcached", "-p", fmt.Sprintf("%d", port))
}

func (self *TestEnv) startBackground(cmdname string, args ...string) {
  path, err := exec.LookPath(cmdname)
  if err != nil {
    self.FailNow("Failed to find %s executable: %s", cmdname, err)
  }

  cmd := exec.Command(path, args...)

  stderrpipe, err := cmd.StderrPipe()
  if err != nil {
    self.FailNow("Failed to open pipe to stderr")
  }
  stdoutpipe, err := cmd.StdoutPipe()
  if err != nil {
    self.FailNow("Failed to open pipe to stdout")
  }
  pipes := []struct {
    Out *os.File
    Rdr *bufio.Reader
  } {
    { os.Stdout, bufio.NewReader(stdoutpipe) },
    { os.Stderr, bufio.NewReader(stderrpipe) },
  }

  self.Logf("Starting command %v", cmd.Args)
  err = cmd.Start()
  if err != nil {
    self.FailNow("Failed to start %s: %s", cmdname, err)
  }
  killed := false
  for _, p := range pipes {
    go func(out *os.File, in *bufio.Reader) {
      for !killed {
        str, err := in.ReadBytes('\n')
        if str != nil {
          out.Write(str)
          out.Sync()
        }

        if err != nil {
          break
        }
      }
    }(p.Out, p.Rdr)
  }

  go func() {
    err := cmd.Wait()
    if !killed && err != nil {
      self.Logf("Failed to wait for %s: %s", cmdname, err)
    }
  }()

  self.AddGuard(func() { cmd.Process.Kill(); killed = true })
}

func (self *TestEnv) startDatabase()  {
  mycnf := mysqltest.NewConfig()
  mysqld, err := mysqltest.NewMysqld(mycnf)
  if err != nil {
    self.FailNow("Failed to start mysqld: %s", err)
  }
  self.Mysqld = mysqld

  time.Sleep(2 * time.Second)
  self.MysqlConfig = &DatabaseConfig {
    "mysql",
    "root",
    "",
    mysqld.ConnectString(0),
    "test",
  }

  self.Guards = append(self.Guards, func() {
    if mysqld := self.Mysqld; mysqld != nil {
      mysqld.Stop()
    }
  })

  _, err = ConnectDB(self.MysqlConfig)
  if err != nil {
    self.FailNow("Failed to connect to database: %s", err)
  }

  self.Logf("Database files in %s", mysqld.BaseDir())
  self.createDatabase()
}

func (self *TestEnv) createDatabase() {
  self.Logf("Creating database...")

  // Read from DDL file, each statement (delimited by ";")
  // then execute each statement via db.Exec()
  db, err := ConnectDB(self.MysqlConfig)
  if err != nil {
    self.FailNow("Failed to connect to database: %s", err)
  }

  file, err := os.Open("stf.sql")
  if err != nil {
    self.FailNow("Failed to read DDL: %s", err)
  }

  fi, err := file.Stat()
  if err != nil {
    self.FailNow("Failed to stat file: %s", err)
  }

  buf := make([]byte, fi.Size())
  _, err = io.ReadFull(file, buf)
  strbuf := string(buf)

  for {
    i := strings.Index(strbuf, ";")
    if i < 0 {
      break
    }
    stmt := strbuf[0:i]
    _, err = db.Exec(stmt)
    if err != nil {
      self.Logf("Failed to create database!")
      self.Logf("    SQL: %s", stmt)
      self.FailNow("Failed to create database SQL: %s", err)
    }
    strbuf = strbuf[i+1:len(strbuf)-1]
  }
}

func (self *TestEnv) createTemporaryDir() {
  tempdir, err := ioutil.TempDir("", "stf-test");
  if err != nil {
    self.FailNow("Failed to create a temporary directory: %s", err)
  }

  self.WorkDir = tempdir
  self.Guards  = append(self.Guards, func() {
    self.Logf("Releasing work dir %s", tempdir)
    os.RemoveAll(tempdir)
  })
}

func (self *TestEnv) createTemporaryConfig() {
  tempfile, err := ioutil.TempFile(self.WorkDir, "test.gcfg")
  if err != nil {
    self.FailNow("Failed to create tempfile: %s", err)
  }

  tempfile.WriteString(fmt.Sprintf(
`
[MainDB]
Username=%s
ConnectString=%s
Dbname=%s

[Memcached]
Servers = 127.0.0.1:%d

`,
    self.MysqlConfig.Username,
    self.MysqlConfig.ConnectString,
    self.MysqlConfig.Dbname,
    self.MemdPort,
  ))


  self.writeQueueConfig(tempfile)

  tempfile.Sync()

  self.Logf("Created config file %s", tempfile.Name())

  self.ConfigFile = tempfile
  self.Guards = append(self.Guards, func() {
    self.Logf("Removing config file %s", tempfile.Name())
    os.Remove(tempfile.Name())
  })
}

func (self *TestEnv) startTemporaryStorageServer(id int, dir string) {
  ss := NewStorageServer("dummy", dir)
  dts := httptest.NewServer(ss)

  // Register ourselves in the database
  db, err := ConnectDB(self.MysqlConfig)
  if err != nil {
    log.Fatalf("Failed to connect to database: %s", err)
  }
  _, err = db.Exec("INSERT INTO storage (id, uri, mode, cluster_id) VALUES (?, ?, 1, 1)", id, dts.URL)
  if err != nil {
    log.Fatalf("Failed to insert storage into DB: %s", err)
  }

  self.Guards = append(self.Guards, func() {
    dts.Close()
  })
}

func (self *TestEnv) startTemporaryStorageServers() {
  // Register ourselves in the database
  db, err := ConnectDB(self.MysqlConfig)
  if err != nil {
    log.Fatalf("Failed to connect to database: %s", err)
  }
  _, err = db.Exec("INSERT INTO storage_cluster (id, name, mode) VALUES (1, 1, 1)")
  if err != nil {
    log.Fatalf("Failed to create storage cluster: %s", err)
  }
  for i := 1; i <= 3; i++ {
    mydir := filepath.Join(self.WorkDir, fmt.Sprintf("storage%03d", i))
    self.startTemporaryStorageServer(i, mydir)
  }
}

func (self *TestEnv) startWorkers() {
  self.startBackground(
    "bin/stf-worker",
    fmt.Sprintf("--config=%s", self.ConfigFile.Name()),
  )
}

func TestBasic(t *testing.T) {
  env := NewTestEnv(t)
  defer env.Release()

  env.Setup()

  os.Setenv("STF_CONFIG", env.ConfigFile.Name())
  config, err := BootstrapConfig()
  if err != nil {
    t.Errorf("%s", err)
  }

  d := NewDispatcher(config)
  dts := httptest.NewServer(d)
  defer dts.Close()

  client := &http.Client {}

  t.Logf("Test server ready at %s", dts.URL)

  bucketUrl := fmt.Sprintf("%s/test", dts.URL)
  url := fmt.Sprintf("%s/test.txt", bucketUrl)
  res, _ := client.Get(url)
  if res.StatusCode != 404 {
    t.Errorf("GET on non-existent URL %s: want 404, got %d", url, res.StatusCode)
  }

  req, err := http.NewRequest("PUT", bucketUrl, nil)
  res, _ = client.Do(req)
  if res.StatusCode != 201 {
    t.Errorf("PUT %s: want 201, got %d", bucketUrl, res.StatusCode)
  }

  _, filename, _, _ := runtime.Caller(1)
  file, err := os.Open(filename)
  if err != nil {
    t.Errorf("Failed to open %s: %s", filename, err)
  }
  fi, err := file.Stat()
  if err != nil {
    t.Errorf("Failed to stat %s: %s", filename, err)
  }

  req, _ = http.NewRequest("PUT", url, file)
  req.ContentLength = fi.Size()
  res, _ = client.Do(req)
  if res.StatusCode != 201 {
    t.Errorf("PUT %s: want 201, got %d", url, res.StatusCode)
  }

  req, _ = http.NewRequest("GET", url, nil)
  res, _ = client.Do(req)
  if res.StatusCode != 200 {
    t.Errorf("GET %s: want 200, got %d", url, res.StatusCode)
  }

  if res.Header.Get("X-Reproxy-URL") == "" {
    t.Errorf("GET %s: want X-Reproxy-URL, got empty", url)
  }

  req, _ = http.NewRequest("DELETE", url, nil)
  res, _ = client.Do(req)
  if res.StatusCode != 204 {
    t.Errorf("DELETE %s: want 204, got %d", url, res.StatusCode)
  }

  req, _ = http.NewRequest("GET", url, nil)
  res, _ = client.Do(req)
  if res.StatusCode != 404 {
    t.Errorf("GET %s (after delete): want 404, got %d", url, res.StatusCode)
  }
}