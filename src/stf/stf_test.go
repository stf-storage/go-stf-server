package stf

import (
  "fmt"
  "io/ioutil"
  "log"
  "os"
  "os/exec"
  "path/filepath"
  "net/http"
  "net/http/httptest"
  "runtime"
  "syscall"
  "testing"
)

type TestEnv struct {
  Test        *testing.T
  Guards      []func()
  WorkDir     string
  ConfigFile  *os.File
  Database    *TestDatabase
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

func (self *TestEnv) startDatabase()  {
  fullpath, err := exec.LookPath("mysqld")
  if err != nil {
    log.Fatalf("Failed to find mysqld in path: %s", err)
  }

  basedir := filepath.Join(self.WorkDir, "mysql")
  defaultsFile := filepath.Join(basedir, "etc", "my.cnf")
  cmd := exec.Command(
    fullpath,
    fmt.Sprintf("--defaults-file=%s", defaultsFile),
  )

  err = cmd.Start()
  if err != nil {
    log.Fatalf("Failed to execute command %s: %s", cmd, err)
  }

  tmpdir := filepath.Join(basedir, "tmp")
  td := &TestDatabase {
    nil,
    filepath.Join(tmpdir, "mysql.sock"), // Socket
    filepath.Join(basedir, "var"), // DataDir
    filepath.Join(tmpdir, "mysqld.pid"), // PidFile
    tmpdir,
  }

  // td.WriteMycnf(filepath.Join(basedir, "etc", "my.cnf"))

  config := &DatabaseConfig {
    "mysql",
    "root",
    "",
    fmt.Sprintf("unix(%s)", td.Socket),
    "test",
  }

  td.Config = config
  self.Database = td

  self.Logf("Started database listening at %s", td.Socket)

  go cmd.Wait()

  self.Guards = append(self.Guards, func() {
    if cmd.Process != nil {
      cmd.Process.Signal(syscall.SIGTERM)
    }
  })
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
  db, err := ConnectDB(self.Database.Config)
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

  url := fmt.Sprintf("%s/test/test.txt", dts.URL)
  res, _ := client.Get(url)
  if res.StatusCode != 404 {
    t.Errorf("GET on non-existent URL %s: want 404, got %d", url, res.StatusCode)
  }

  _, filename, _, _ := runtime.Caller(1)
  file, err := os.Open(filename)
  req, err := http.NewRequest("PUT", url, file)
  res, _ = client.Do(req)
  if res.StatusCode != 204 {
    t.Errorf("PUT %s: want 204, got %d", url, res.StatusCode)
  }
}