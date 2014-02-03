package dispatcher

import (
  "errors"
  "fmt"
  "log"
  "net/http"
  "regexp"
  "runtime"
  "runtime/debug"
  "strings"
  "github.com/braintree/manners"
  "github.com/stf-storage/go-stf-server"
  "github.com/lestrrat/go-apache-logformat"
  "github.com/lestrrat/go-file-rotatelogs"
  "github.com/lestrrat/go-server-starter-listener"
)

type Dispatcher struct {
  config          *stf.Config
  Address         string
  Ctx             *stf.Context
  ResponseWriter  *http.ResponseWriter
  Request         *http.Request
  logger          *apachelog.ApacheLog
  idgen           *UUIDGen
}

type DispatcherContext struct {
  *stf.Context
  ResponseWriter  http.ResponseWriter
  request         *http.Request
}

type DispatcherContextWithApi interface {
  stf.ContextWithApi
  Request() *http.Request
}

func New(config *stf.Config) *Dispatcher {
  d := &Dispatcher {
    config: config,
    idgen: NewIdGenerator(config.Dispatcher.ServerId),
  }

  d.logger = apachelog.CombinedLog.Clone()

  if filename := config.Dispatcher.AccessLog; filename != "" {
    rl := rotatelogs.NewRotateLogs(filename)
    if linkname := config.Dispatcher.AccessLogLink; linkname != "" {
      rl.LinkName = linkname
    }
    d.logger.SetOutput(rl)
  }

  return d
}

func (ctx *DispatcherContext) Request() *http.Request {
  return ctx.request
}

func (self *Dispatcher) IdGenerator() (*UUIDGen) {
  return self.idgen
}

func (self *Dispatcher) Start () {
  ctx := stf.NewContext(self.config)
  ncpu := runtime.NumCPU()
  nmaxprocs := runtime.GOMAXPROCS(-1)
  if ncpu != nmaxprocs {
    ctx.Debugf("Setting GOMAXPROCS to %d (was %d)", ncpu, nmaxprocs)
    runtime.GOMAXPROCS(ncpu)
  }

  // Work with Server::Stareter
  baseListener, err := ss.NewListenerOrDefault("tcp", self.config.Dispatcher.Listen)
  if err != nil {
    panic(fmt.Sprintf("Failed to listen at %s: %s", self.config.Dispatcher.Listen, err))
  }
  ctx.Debugf("Listening on %s", baseListener.Addr())

  s := manners.NewServer()
  l := manners.NewListener(baseListener, s)
  err = http.Serve(l, self)
  if err != nil {
    log.Fatal(
      fmt.Sprintf("Error from server's ListenAndServe: %s\n", err),
    )
  }
}

func (self *Dispatcher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  ctx := &DispatcherContext{
    stf.NewContext(self.config),
    w,
    r,
  }
  closer := ctx.LogMark("[%s %s]", r.Method, r.URL.Path)
  defer closer()

  lw := apachelog.NewLoggingWriter(w, r, self.logger)
  defer lw.EmitLog()

  // Generic catch-all handler
  defer func() {
    if err := recover(); err != nil {
      debug.PrintStack()
      ctx.Debugf("Error while serving request: %s", err)
      http.Error(w, http.StatusText(500), 500)
    }
  } ()

  // First see if we have a proper URL that STF understands
  bucketName, objectName, err := parseObjectPath(r.URL.Path)
  if err != nil {
    http.Error(w, http.StatusText(404), 404)
    return
  }
  ctx.Debugf(
    "Parsed bucketName = '%s', objectName = '%s'\n",
    bucketName,
    objectName,
  )

  var resp *HTTPResponse
  switch r.Method {
  case "GET":
    resp = self.FetchObject(ctx, bucketName, objectName)
  case "DELETE":
    if objectName == "" {
      resp = self.DeleteBucket(ctx, bucketName)
    } else {
      resp = self.DeleteObject(ctx, bucketName, objectName)
    }
  case "PUT":
    // If the Content-Length is 0, then attempt to create the
    // Bucket Otherwise, try the bucket
    if cl := r.ContentLength; cl > 0 {
      resp = self.CreateObject(ctx, bucketName, objectName)
    } else {
      resp = self.CreateBucket(ctx, bucketName, objectName)
    }
  case "POST":
    resp = self.ModifyObject(ctx, bucketName, objectName)
  case "MOVE":
    dest := r.Header.Get("X-STF-Move-Destination")
    if objectName == "" {
      resp = self.RenameBucket(ctx, bucketName, dest)
    } else {
      resp = self.RenameObject(ctx, bucketName, objectName, dest)
    }
  default:
    resp = HTTPMethodNotAllowed
    return
  }

  if resp == nil {
    panic("Did not get a response object?!")
  }
  resp.Write(ctx.ResponseWriter)
}

func parseObjectPath(path string) (string, string, error) {
  precedingSlashRegexp := regexp.MustCompile(`^/`)

  // The path starts with a "/", but it's really not necessary
  path = precedingSlashRegexp.ReplaceAllLiteralString(path, "")

  len   := len(path)
  index := strings.Index(path, "/")

  if index == 0 {
    // Whoa, found a slash as the first thing?
    return "", "", errors.New("No bucket name could be extracted")
  }

  if index == -1 {
    // No slash? is this all bucket names?
    if len > 1 {
      return path, "", nil
    } else {
      return "", "", errors.New("No bucket name could be extracted")
    }
  }

  // If we got here, least 1 "/" was found.

  bucketName := path[0:index]
  objectName := path[index + 1:len]

  index = strings.Index(objectName, "/")
  if index == 0 {
    // Duplicate slashes. Fuck you
    return "", "", errors.New("Illegal object name")
  }

  return bucketName, objectName, nil
}

