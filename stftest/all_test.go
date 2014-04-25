package stftest

import (
	"fmt"
	"github.com/lestrrat/go-tcptest"
	"github.com/lestrrat/go-test-mysqld"
	"github.com/stf-storage/go-stf-server"
	"github.com/stf-storage/go-stf-server/api"
	"github.com/stf-storage/go-stf-server/config"
	"github.com/stf-storage/go-stf-server/dispatcher"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type TestEnv struct {
	Test           *testing.T
	Ctx            *api.Context
	Guards         []func()
	WorkDir        string
	ConfigFile     *os.File
	Mysqld         *mysqltest.TestMysqld
	MysqlConfig    *config.DatabaseConfig
	MemdPort       int
	QueueConfig    *config.QueueConfig
	StorageServers []*stf.StorageServer
}

type TestDatabase struct {
	Config  *config.DatabaseConfig
	Socket  string
	DataDir string
	PidFile string
	TmpDir  string
}

func NewTestEnv(t *testing.T) *TestEnv {
	env := &TestEnv{}
	env.Test = t
	return env
}

func (self *TestEnv) Setup() {
	if home := os.Getenv("STF_HOME"); home != "" {
		oldpath := os.Getenv("PATH")
		newpath := path.Join(home, "bin")
		self.Test.Logf("Adding %s to path", newpath)
		os.Setenv("PATH", strings.Join([]string{newpath, oldpath}, ":"))
	}

	self.createTemporaryDir()
	self.startDatabase()
	self.startQueue()
	self.startMemcached()
	self.createTemporaryConfig()
	self.startTemporaryStorageServers()
	self.startWorkers()

	os.Setenv("STF_CONFIG", self.ConfigFile.Name())
	config, err := config.BootstrapConfig()
	if err != nil {
		self.Test.Fatalf("%s", err)
	}

	self.Ctx = api.NewContext(config)
}

func (self *TestEnv) Release() {
	for _, f := range self.Guards {
		f()
	}
}

func (self *TestEnv) Errorf(format string, args ...interface{}) {
	self.Test.Errorf(format, args...)
}

func (self *TestEnv) FailNow(format string, args ...interface{}) {
	self.Test.Errorf(format, args...)
	debug.PrintStack()
	self.Test.FailNow()
}

func (self *TestEnv) Logf(format string, args ...interface{}) {
	self.Test.Logf(format, args...)
}

func AssertDir(dir string) {
	_, err := os.Stat(dir)
	if err == nil {
		return // XXX not checking if dir is a directory
	}

	if !os.IsNotExist(err) {
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

func (self *TestEnv) startMemcached() {
	var cmd *exec.Cmd
	var server *tcptest.TCPTest
	var err error
	for i := 0; i < 5; i++ {
		server, err = tcptest.Start(func(port int) {
			out, err := os.OpenFile("memcached.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

			cmd = exec.Command("memcached", "-vv", "-p", fmt.Sprintf("%d", port))
			cmd.SysProcAttr = &syscall.SysProcAttr{
				Setpgid: true,
			}
			stderrpipe, err := cmd.StderrPipe()
			if err != nil {
				self.FailNow("Failed to open pipe to stderr")
			}
			stdoutpipe, err := cmd.StdoutPipe()
			if err != nil {
				self.FailNow("Failed to open pipe to stdout")
			}

			go io.Copy(out, stderrpipe)
			go io.Copy(out, stdoutpipe)
			cmd.Run()
		}, time.Minute)
		if err == nil {
			break
		}
		self.Logf("Failed to start memcached: %s", err)
	}

	if server == nil {
		self.FailNow("Failed to start memcached")
	}

	self.MemdPort = server.Port()

	self.AddGuard(func() {
		if cmd != nil && cmd.Process != nil {
			self.Logf("Killing memcached")
			cmd.Process.Signal(syscall.SIGTERM)
		}
		server.Wait()
	})
}

type backgroundproc struct {
	cmdname string
	args    []string
	logfile string
}

func (self *TestEnv) startBackground(p *backgroundproc) {
	cmdname := p.cmdname
	args := p.args
	logfile := p.logfile
	path, err := exec.LookPath(cmdname)
	if err != nil {
		self.Test.Fatalf("Failed to find %s executable: %s", cmdname, err)
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

	self.Logf("Starting command %v", cmd.Args)
	err = cmd.Start()
	if err != nil {
		self.FailNow("Failed to start %s: %s", cmdname, err)
	}
	killed := false

	if logfile == "" {
		go io.Copy(os.Stdout, stdoutpipe)
		go io.Copy(os.Stderr, stderrpipe)
	} else {
		out, err := os.OpenFile(logfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			self.FailNow("Could not open logfile: %s", err)
		}
		go io.Copy(out, stdoutpipe)
		go io.Copy(out, stderrpipe)
	}

	go func() {
		err := cmd.Wait()
		if !killed && err != nil {
			self.Logf("Failed to wait for %s: %s", cmdname, err)
		}
	}()

	self.AddGuard(func() { cmd.Process.Signal(syscall.SIGTERM); killed = true })
}

func (self *TestEnv) startDatabase() {
	mycnf := mysqltest.NewConfig()
	mysqld, err := mysqltest.NewMysqld(mycnf)
	if err != nil {
		self.FailNow("Failed to start mysqld: %s", err)
	}
	self.Mysqld = mysqld

	self.MysqlConfig = &config.DatabaseConfig{
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

	_, err = stf.ConnectDB(self.MysqlConfig)
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
	db, err := stf.ConnectDB(self.MysqlConfig)
	if err != nil {
		self.FailNow("Failed to connect to database: %s", err)
	}

	file, err := os.Open("../stf.sql")
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
		strbuf = strbuf[i+1 : len(strbuf)-1]
	}
}

func (self *TestEnv) createTemporaryDir() {
	tempdir, err := ioutil.TempDir("", "stf-test")
	if err != nil {
		self.FailNow("Failed to create a temporary directory: %s", err)
	}

	self.WorkDir = tempdir
	self.Guards = append(self.Guards, func() {
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

func (self *TestEnv) startTemporaryStorageServer(id int, dir string) *stf.StorageServer {
	ss := stf.NewStorageServer("dummy", dir)
	dts := httptest.NewServer(ss)

	// Register ourselves in the database
	db, err := stf.ConnectDB(self.MysqlConfig)
	if err != nil {
		self.Test.Fatalf("Failed to connect to database: %s", err)
	}
	_, err = db.Exec("INSERT INTO storage (id, uri, mode, cluster_id) VALUES (?, ?, 1, 1)", id, dts.URL)
	if err != nil {
		self.Test.Fatalf("Failed to insert storage into DB: %s", err)
	}

	self.Guards = append(self.Guards, func() {
		dts.Close()
	})

	return ss
}

func (self *TestEnv) startTemporaryStorageServers() {
	// Register ourselves in the database
	db, err := stf.ConnectDB(self.MysqlConfig)
	if err != nil {
		self.Test.Fatalf("Failed to connect to database: %s", err)
	}
	_, err = db.Exec("INSERT INTO storage_cluster (id, name, mode) VALUES (1, 1, 1)")
	if err != nil {
		self.Test.Fatalf("Failed to create storage cluster: %s", err)
	}

	max := 3
	servers := make([]*stf.StorageServer, max)
	for i := 1; i <= max; i++ {
		mydir := filepath.Join(self.WorkDir, fmt.Sprintf("storage%03d", i))
		servers[i-1] = self.startTemporaryStorageServer(i, mydir)
	}

	self.StorageServers = servers
}

func (self *TestEnv) startWorkers() {
	self.startBackground(&backgroundproc{
		cmdname: "stf-worker",
		args:    []string{fmt.Sprintf("--config=%s", self.ConfigFile.Name())},
		logfile: "worker.log",
	})
}

func TestBasic(t *testing.T) {
	env := NewTestEnv(t)
	defer env.Release()

	env.Setup()

	dts, err := env.startDispatcher()
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer dts.Close()

	client := &http.Client{}

	t.Logf("Test server ready at %s", dts.URL)

	uri := dts.MakeURL("test", "test.txt")
	res, err := client.Get(uri)
	if err != nil {
		t.Fatalf("Request to '%s' failed: %s", uri, err)
	}
	if res.StatusCode != 404 {
		t.Errorf("GET on non-existent URL %s: want 404, got %d", uri, res.StatusCode)
	}

	stfclient := NewTestSTFClient(t, dts)

	stfclient.ObjectGetExpect("test/test.txt", 404, "Fetch before creating an object should be 404")
	env.Test.Logf("Create bucket:")
	stfclient.BucketCreate("test")

	env.Test.Logf("Create new object:")
	_, filename, _, _ := runtime.Caller(1)
	stfclient.FilePut("test/test.txt", filename)

	env.Test.Logf("Fetch after create new object:")
	res = stfclient.ObjectGet("test/test.txt")
	reproxy_uri := res.Header.Get("X-Reproxy-URL")
	if reproxy_uri == "" {
		t.Errorf("GET %s: want X-Reproxy-URL, got empty", uri)
	}

	time.Sleep(5 * time.Second)
	env.checkEntityCountForObject("test/test.txt", 3)

	parsedUri, err := url.Parse(reproxy_uri)
	if err != nil {
		t.Errorf("Failed to parse reproxy uri: %s", err)
	}
	internalName := parsedUri.Path
	internalName = strings.TrimPrefix(internalName, "/")

	// The object has already been deleted, but make sure that the entity
	// in the backend storage has been properly deleted
	for _, ss := range env.StorageServers {
		localPath := path.Join(ss.Root(), internalName)
		_, err := os.Stat(localPath)
		if err == nil {
			env.Test.Logf("Path %s properly created", localPath)
		} else {
			env.Test.Errorf("Path %s should have been created: %s", localPath, err)
		}
	}

	stfclient.ObjectDelete("test/test.txt")
	stfclient.ObjectGetExpect("test/test.txt", 404, "Fetch after DELETE should be 404")

	// Give it a few more seconds
	time.Sleep(5 * time.Second)

	// The object has already been deleted, but make sure that the entity
	// in the backend storage has been properly deleted
	for _, ss := range env.StorageServers {
		localPath := path.Join(ss.Root(), internalName)
		_, err := os.Stat(localPath)
		if err == nil {
			env.Test.Errorf("Path %s should have been deleted: %s", localPath, err)
		} else {
			env.Test.Logf("Path %s properly deleted", localPath)
		}
	}
}

func (self *TestEnv) checkEntityCountForObject(path string, expected int) {
	var bucketName string
	var objectName string
	i := strings.Index(path, "/")
	if i == -1 {
		self.Errorf("Failed to parse uri")
		return
	} else {
		bucketName = path[0:i]
		objectName = path[i+1 : len(path)]
	}

	rollback, err := self.Ctx.TxnBegin()
	if err != nil {
		self.Errorf("Failed to start transaction")
		return
	}
	defer rollback()

	bucketId, err := self.Ctx.BucketApi().LookupIdByName(bucketName)
	if err != nil {
		self.FailNow("Failed to find id for bucket %s: %s", bucketName, err)
	}
	bucket, err := self.Ctx.BucketApi().Lookup(bucketId)
	if err != nil {
		self.FailNow("Failed to load bucket %d: %s", bucketId, err)
	}

	objectId, err := self.Ctx.ObjectApi().LookupIdByBucketAndPath(bucket, objectName)
	if err != nil {
		self.Test.Fatalf("Failed to find id for object %s/%s: %s", bucketName, objectName, err)
	}

	// Find the entities mapped to this object
	entities, err := self.Ctx.EntityApi().LookupForObject(objectId)
	if err != nil {
		self.FailNow("Failed to find entities for object %d: %s", objectId, err)
	}
	if len(entities) != expected {
		self.Errorf("Expected entity count = %d, got = %d", expected, len(entities))
		self.Logf("Note: if count != %d, then replicate worker isn't being fired", expected)
	} else {
		self.Logf("Entity count = %d, got %d", expected, len(entities))
	}
}

type TestDispatcherServer struct {
	*httptest.Server
}

func (env *TestEnv) startDispatcher() (*TestDispatcherServer, error) {
	config := env.Ctx.Config()
	d := dispatcher.New(config)
	dts := httptest.NewServer(d)

	return &TestDispatcherServer{dts}, nil
}

func (t *TestDispatcherServer) MakeURL(args ...string) string {
	return fmt.Sprintf("%s/%s", t.URL, strings.Join(args, "/"))
}

func TestCreateID(t *testing.T) {
	env := NewTestEnv(t)
	defer env.Release()

	env.Setup()

	dts, err := env.startDispatcher()
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer dts.Close()

	t.Logf("Test server ready at %s", dts.URL)
	bucketUrl := dts.MakeURL("test_id")

	client := &http.Client{}
	req, err := http.NewRequest("PUT", bucketUrl, nil)
	if err != nil {
		t.Fatalf("Failed to create new request: %s", err)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed to send request to '%s': %s", bucketUrl, err)
	}

	if res.StatusCode != 201 {
		t.Fatalf("PUT %s: want 201, got %d", bucketUrl, res.StatusCode)
	}

	wg := &sync.WaitGroup{}
	ready := make(chan bool)

	_, filename, _, _ := runtime.Caller(1)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		x := i
		go func(filename string, x int) {
			defer wg.Done()
			file, err := os.Open(filename)
			if err != nil {
				t.Fatalf("Failed to open %s: %s", filename, err)
			}
			fi, err := file.Stat()
			if err != nil {
				t.Errorf("Failed to stat %s: %s", filename, err)
			}
			uri := fmt.Sprintf("%s/test%03d.txt", bucketUrl, x)
			req, _ := http.NewRequest("PUT", uri, file)
			req.ContentLength = fi.Size()
			client := &http.Client{}

			<-ready

			res, err := client.Do(req)
			if err != nil {
				t.Logf("Failed to send request: %s", err)
			} else if res.StatusCode != 201 {
				t.Errorf("Failed to create %s: %s", uri, res.Status)
			}
		}(filename, x)
	}

	for i := 0; i < 10; i++ {
		ready <- true
	}

	wg.Wait()
}

/*
func TestMove(t *testing.T) {
  env := NewTestEnv(t)
  defer env.Release()

  env.Setup()

  dts, err := env.startDispatcher()
  if err != nil {
    t.Fatalf("%s", err)
  }
  defer dts.Close()
}
*/
