package dispatcher

import (
  "net/http"
)

/* In our case, we don't really need a full-fledged response
   object as we never return a content-body ourselves
*/
type HTTPResponse struct {
  Code int
  Message string
  Header http.Header
}

func NewResponse(code int) *HTTPResponse {
  h := &HTTPResponse {
    Code: code,
    Header: map[string][]string {},
  }
  return h
}

func (self *HTTPResponse) Write(rw http.ResponseWriter) {
  hdrs := self.Header
  if hdrs != nil {
    if ct := hdrs.Get("Content-Type"); ct == "" {
      hdrs.Set("Content-Type", "text/plain")
    }
  }

  outHeader := rw.Header()
  for k, list := range self.Header {
    for _, v := range list {
      outHeader.Add(k, v)
    }
  }
  rw.WriteHeader(self.Code)

  if self.Message != "" {
    rw.Write([]byte(self.Message))
  }
}

var HTTPCreated             = NewResponse(201)
var HTTPNoContent           = NewResponse(204)
var HTTPNotModified         = NewResponse(304)
var HTTPBadRequest          = NewResponse(400)
var HTTPNotFound            = NewResponse(404)
var HTTPMethodNotAllowed    = NewResponse(405)
var HTTPInternalServerError = NewResponse(500)


