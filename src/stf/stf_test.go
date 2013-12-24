package stf

import (
  "fmt"
  "io/ioutil"
  "os"
  "net/http"
  "net/http/httptest"
  "runtime"
  "testing"
)

type TestEnv struct {
  WorkDir     string
  ConfigFile  *os.File
}

func setupEnvironment (t *testing.T) (*TestEnv, func ()) {
  env := &TestEnv {}
  workdir, workdirGuard := createTemporaryDir(t)
  configfile, configfileGuard := createTemporaryConfig(t, workdir)

  env.WorkDir = workdir
  env.ConfigFile = configfile

  guard := func() {
    configfileGuard()
    workdirGuard()
  }

  return env, guard
}

func createTemporaryDir (t *testing.T) (string, func ()) {
  tempdir, err := ioutil.TempDir("", "stf-test");
  if err != nil {
    t.Errorf("Failed to create a temporary directory: %s", err)
  }
  return tempdir, func() {
    t.Logf("Releasing work dir %s", tempdir)
    os.RemoveAll(tempdir)
  }
}

func createTemporaryConfig(t *testing.T, dir string) (*os.File, func()) {
  tempfile, err := ioutil.TempFile(dir, "test.gcfg")
  if err != nil {
    t.Errorf("Failed to create tempfile: %s", err)
  }

  t.Logf("Created config file %s", tempfile.Name())

  return tempfile, func () {
    t.Logf("Removing config file %s", tempfile.Name())
    os.Remove(tempfile.Name())
  }
}

func startTemporaryStorageServer() func() {
  for i := 1; i <= 3; i++ {
    dir := 
  dts := httptest.NewServer(d)


  return func() {}
}

func TestBasic(t *testing.T) {
  env, guard := setupEnvironment(t)
  defer guard()

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