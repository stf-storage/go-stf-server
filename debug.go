package stf

import (
  "fmt"
  "io"
  "log"
  "os"
  "strconv"
  "strings"
)

func init () {
  dbgEnabled := false

  dbgOutput := os.Stderr
  if dbg := os.Getenv("STF_DEBUG"); dbg != "" {
    // STF_DEBUG='1:path/to/log'
    if strings.Index(dbg, ":") > -1 {
      list := strings.Split(dbg, ":")
      dbg = list[0]
      if len(list) > 1 {
        file, err := os.OpenFile(list[1], os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
        if err != nil {
          log.Fatalf("Failed to open debug log output %s: %s", list[1], err)
        }
        dbgOutput = file
      }
    }
    x, err := strconv.ParseBool(dbg)
    if err == nil {
      dbgEnabled = x
    }
  }

  if dbgEnabled {
    global = NewDebugLog()
    global.SetOutput(dbgOutput)
  }
}

const INDENT_PATTERN string = "  "
type DebugLog struct {
  Prefix string
  Indent string
  out     io.Writer
}

func NewDebugLog() *DebugLog {
  return &DebugLog {
    "",
    "",
    os.Stderr,
  }
}

var global *DebugLog

func (self *DebugLog) SetOutput(out io.Writer) {
  self.out = out
}

func (self *DebugLog) Printf(format string, args ...interface {}) {
  message := fmt.Sprintf(format, args...)
  log.Printf("%s %s %s", self.Prefix, self.Indent, message)
}

func (self *DebugLog) LogIndent() func() {
  self.Indent = INDENT_PATTERN + self.Indent
  return func () {
    self.Indent = strings.TrimPrefix(self.Indent, INDENT_PATTERN)
  }
}

func Debugf(format string, args ...interface {}) {
  if global != nil {
    global.Printf(format, args...)
  }
}

func LogIndent() {
  if global != nil {
    global.LogIndent()
  }
}

func LogMark(format string, args ...interface{}) func() {
  if global == nil {
    return func() {}
  }

  return global.LogMark(format, args...)
}

func DebugEnabled() bool {
  return global != nil
}

func (self *DebugLog) LogMark(format string, args ...interface{}) func () {
  marker := fmt.Sprintf(format, args...)

  self.Printf("%s START", marker)
  closer := self.LogIndent()
  return func () {
    err := recover()
    if err != nil {
      self.Printf("Encoundered panic during '%s': %s", marker, err)
    }
    closer()
    self.Printf("%s END", marker)
    if err != nil {
      panic(err)
    }
  }
}


