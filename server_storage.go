package stf

import (
  "fmt"
  "io"
  "io/ioutil"
  "log"
  "net/http"
  "os"
  "path"
  "strconv"
  "time"
)

type StorageServer struct {
  root    string
  fserver http.Handler
  tempdir string
  listen  string
}

func NewStorageServer(listen string, root string) (*StorageServer) {
  pwd, err := os.Getwd()
  if err != nil {
    log.Fatalf("Could not determine current working directory")
  }

  if ! path.IsAbs(root) {
    root = path.Join(pwd, root)
  }

  tempdir, err := ioutil.TempDir("", "stf-storage");
  if err != nil {
    log.Fatalf("Failed to create a temporary directory: %s", err)
  }

  ss := &StorageServer {
    fserver: http.FileServer(http.Dir(root)),
    tempdir: tempdir,
    root:    root,
    listen:  listen,
  }

  return ss
}

func (self *StorageServer) Root() string {
  return self.root
}

func (self *StorageServer) Close() {
  // Make sure to clean up
  os.RemoveAll(self.tempdir)
}

func (self *StorageServer) Start() {
  server    := &http.Server{
    Addr:     self.listen,
    Handler:  self,
  }
  defer self.Close()

  log.Printf("Server starting at %s, files in %s", server.Addr, self.root)
  err := server.ListenAndServe()
  if err != nil {
    log.Fatal(
      fmt.Sprintf("Error from server's ListenAndServe: %s\n", err),
    )
  }
}

func (self *StorageServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  switch r.Method {
  case "GET", "HEAD":
    self.fserver.ServeHTTP(w, r)
  case "PUT":
    self.PutObject(w, r)
  case "DELETE":
    self.DeleteObject(w, r)
  }
}

func (self *StorageServer) httpError(w http.ResponseWriter, message string) {
  w.Header().Set("Content-Type", "text/plain")
  w.WriteHeader(500)
  w.Write([]byte(message))
}

func (self *StorageServer) PutObject (w http.ResponseWriter, r *http.Request) {
  // Final destination of this file
  dest := path.Join(self.root, r.URL.Path)
  dir  := path.Dir(dest)
  if _, err := os.Stat(dir); err != nil {
    if ! os.IsNotExist(err) {
      log.Printf("Failed to stat directory %s: %s", dir, err)
      self.httpError(w, "Unknown error")
      return
    }
    // If it doesn't exist, create this guy
    err = os.MkdirAll(dir, 0744)
    if err != nil {
      log.Printf("Failed to create directory: %s", err)
      self.httpError(w, "Failed to create directory")
      return
    }
  }

  // Slurp the contents to a temporary file
  tempdest, err := ioutil.TempFile(self.tempdir, "object")
  if err != nil {
    log.Printf("Failed to create tempfile: %s", err)
    self.httpError(w, "Failed to create file")
    return
  }

  defer func() {
    os.Remove(tempdest.Name())
  }()

  if _, err = io.Copy(tempdest, r.Body); err != nil {
    log.Printf("Failed to copy contents of request to temporary file %s: %s", tempdest, err)
    self.httpError(w, "Failed to create file")
    return
  }

  // Now move this sucker to the actual location
  if err = os.Rename(tempdest.Name(), dest); err != nil {
    log.Printf("Failed to rename %s to %s: %s", tempdest.Name(), dest, err)
    self.httpError(w, "Failed to create file")
    return
  }

  if hdr := r.Header.Get("X-STF-Object-Timestamp"); hdr != "" {
    timestamp, err := strconv.ParseInt(hdr, 10, 64)
    if err != nil {
      log.Printf("Failed to parse timestamp (%s) of file %s (ignored): %s", hdr, dest, err)
    } else {
      t := time.Unix(timestamp, 0)
      err = os.Chtimes(dest, t, t)
      if err != nil {
        log.Printf("Failed to set timestamp (%s) of file %s (ignored): %s", t.String(), dest, err)
      }
    }
  }

  log.Printf("Successfully created %s", dest)

  w.WriteHeader(201)
}

func (self *StorageServer) DeleteObject (w http.ResponseWriter, r *http.Request) {
  dest := path.Join(self.root, r.URL.Path)
  if _, err := os.Stat(dest); err != nil {
    if ! os.IsNotExist(err) {
      log.Printf("Failed to stat file %s: %s", dest, err)
      self.httpError(w, "Unknown error")
      return
    }
    w.WriteHeader(404)
    return
  }

  err := os.Remove(dest)
  if err != nil {
    log.Printf("Failed to remove file %s: %s", dest, err)
    self.httpError(w, "Failed to remove file")
    return
  }

  w.WriteHeader(204)
}