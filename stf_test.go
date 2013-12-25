package stf

import (
  "fmt"
  "github.com/lestrrat/go-test-mysqld"
  "io"
  "io/ioutil"
  "log"
  "os"
  "path/filepath"
  "net/http"
  "net/http/httptest"
  "runtime"
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
  self.createTemporaryConfig()
}

func (self *TestEnv) Release () {
  for _, f := range self.Guards {
    f()
  }
}

func (self *TestEnv) Errorf(format string, args ...interface {}) {
  self.Test.Errorf(format, args...)
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

func (self *TestEnv) startDatabase()  {
  mycnf := mysqltest.NewConfig()
  mycnf.SkipNetworking = false
  mycnf.Port = 3306
  mysqld, err := mysqltest.NewMysqld(mycnf)
  if err != nil {
    t := self.Test
    t.Errorf("Failed to start mysqld: %s", err)
    t.FailNow()
  }
  self.Mysqld = mysqld

  time.Sleep(2 * time.Second)
  config := &DatabaseConfig {
    "mysql",
    "root",
    "",
    fmt.Sprintf("tcp(%s:%d)", mysqld.Config.BindAddress, mysqld.Config.Port),
    "test",
  }
  self.MysqlConfig = config

  self.Guards = append(self.Guards, func() {
    if mysqld := self.Mysqld; mysqld != nil {
      mysqld.Stop()
    }
  })

  // Sanity check to make sure the database can be connected
  _, err = ConnectDB(config)
  if err != nil {
    t := self.Test
    t.Errorf("Failed to connect to database: %s", err)
    t.FailNow()
  }

  self.Test.Logf("Database files in %s", mysqld.BaseDir())

  self.createDatabase()
}

func (self *TestEnv) createDatabase() {
  // Read from DDL file, each statement (delimited by ";")
  // then execute each statement via db.Exec()
  t := self.Test
  db, err := ConnectDB(self.MysqlConfig)
  if err != nil {
    t.Errorf("Failed to connect to database: %s", err)
    t.FailNow()
  }

  file, err := os.Open("stf.sql")
  if err != nil {
    t.Errorf("Failed to read DDL: %s", err)
    t.FailNow()
  }

  fi, err := file.Stat()
  if err != nil {
    t.Errorf("Failed to stat file: %s", err)
    t.FailNow()
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
      t.Errorf("Failed to execute SQL: %s", err)
      t.FailNow()
    }
    strbuf = strbuf[i+1:len(strbuf)-1]
  }
}

func (self *TestEnv) createTemporaryDir() {
  tempdir, err := ioutil.TempDir("", "stf-test");
  if err != nil {
    self.Errorf("Failed to create a temporary directory: %s", err)
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
    self.Errorf("Failed to create tempfile: %s", err)
  }

  tempfile.WriteString(fmt.Sprintf(
`
[MainDB]
Username=%s
ConnectString=%s
Dbname=%s
`,
    self.MysqlConfig.Username,
    self.MysqlConfig.ConnectString,
    self.MysqlConfig.Dbname,
  ))
  tempfile.Sync()

  self.Logf("Created config file %s", tempfile.Name())

  self.ConfigFile = tempfile
  self.Guards = append(self.Guards, func() {
    self.Logf("Removing config file %s", tempfile.Name())
    os.Remove(tempfile.Name())
  })
}

func (self *TestEnv) startTemporaryStorageServer(dir string) {
  ss := NewStorageServer("dummy", dir)
  dts := httptest.NewServer(ss)

  // Register ourselves in the database
  db, err := ConnectDB(self.MysqlConfig)
  if err != nil {
    log.Fatalf("Failed to connect to database: %s", err)
  }
  db.Exec("INSERT INTO storage (uri, mode) VALUES (?, 1)", dts.URL)

  self.Guards = append(self.Guards, func() {
    dts.Close()
  })
}

func (self *TestEnv) startTemporaryStorageServers() {
  for i := 1; i <= 3; i++ {
    mydir := filepath.Join(self.WorkDir, fmt.Sprintf("storage%03d", i))
    self.startTemporaryStorageServer(mydir)
  }
}

func TestBasic(t *testing.T) {
  env := NewTestEnv(t)
  defer env.Release()

  env.Setup()

  os.Setenv("STF_CONFIG", env.ConfigFile.Name())
  ctx, err := BootstrapContext()
  if err != nil {
    t.Errorf("%s", err)
  }

  defer ctx.Destroy()

  d, err := BootstrapDispatcher(ctx)
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
  req, err = http.NewRequest("PUT", url, file)
  res, _ = client.Do(req)
  if res.StatusCode != 204 {
    t.Errorf("PUT %s: want 204, got %d", url, res.StatusCode)
  }
}