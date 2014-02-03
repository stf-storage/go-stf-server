package stftest

import (
  "fmt"
  "io"
  "os"
  "net/http"
  "testing"
)

type TestClient struct {
  t       *testing.T
  server  *TestDispatcherServer
  client  *http.Client
}

func NewTestSTFClient(t *testing.T, server *TestDispatcherServer) (*TestClient) {
  return &TestClient {
    t,
    server,
    &http.Client {},
  }
}

func (c *TestClient) MakeRequest(method string, uri string, body io.Reader) *http.Request {
  req, err := http.NewRequest(method, uri, body)
  if err != nil {
    c.t.Fatalf("Failed to create request: %s", err)
  }
  return req
}

func (c *TestClient) SendRequestExpect(req *http.Request, expected int, title string) (*http.Response) {
  res, err := c.client.Do(req)
  if err != nil {
    c.t.Fatalf("Failed to send request: %s", err)
  }

  if res.StatusCode != expected {
    c.t.Logf(title)
    c.t.Fatalf(`Request "%s %s": Expected response status code %d, got %d`, req.Method, req.URL, expected, res.StatusCode)
  }

  return res
}

func (c *TestClient) BucketCreate(path string) (*http.Response) {
  uri := c.server.MakeURL(path)
  req := c.MakeRequest("PUT", uri, nil)
  res := c.SendRequestExpect(req, 201, "Create bucket should succeed")

  return res
}

func (c *TestClient) FilePut(path string, filename string) *http.Response {
  file, err := os.Open(filename)
  if err != nil {
    c.t.Fatalf("Failed to open %s: %s", filename, err)
  }
  fi, err := file.Stat()
  if err != nil {
    c.t.Fatalf("Failed to stat %s: %s", filename, err)
  }

  uri := c.server.MakeURL(path)
  req := c.MakeRequest("PUT", uri, file)
  req.ContentLength = fi.Size()
  res := c.SendRequestExpect(req, 201, fmt.Sprintf("File upload (%s) should success", filename))

  return res
}

func (c *TestClient) ObjectGet(path string) (*http.Response) {
  return c.ObjectGetExpect(path, 200, "Fetch object should succeed")
}

func (c *TestClient) ObjectGetExpect(path string, expectedStatus int, title string) (*http.Response) {
  uri := c.server.MakeURL(path)
  req := c.MakeRequest("GET", uri, nil)
  res := c.SendRequestExpect(req, expectedStatus, title)
  return res
}

func (c *TestClient) ObjectDelete(path string) (*http.Response) {
  uri := c.server.MakeURL(path)
  req := c.MakeRequest("DELETE", uri, nil)
  res := c.SendRequestExpect(req, 204, "Delete object should succeed")
  return res
}
