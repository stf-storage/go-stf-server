package stf

import (
  "fmt"
  "log"
  "os"
  "strings"
)

const INDENT_PATTERN string = "  "
type DebugLog struct {
  Output *log.Logger
  Prefix string
  Indent string
}

func NewDebugLog() *DebugLog {
  return &DebugLog {
    log.New(os.Stderr, "", log.LstdFlags),
    "",
    "",
  }
}

func (self *DebugLog) Printf(format string, args ...interface {}) {
  message := fmt.Sprintf(format, args...)
  self.Output.Printf("%s %s %s", self.Prefix, self.Indent, message)
}

func (self *DebugLog) LogIndent() func() {
  self.Indent = INDENT_PATTERN + self.Indent
  return func () {
    self.Indent = strings.TrimPrefix(self.Indent, INDENT_PATTERN)
  }
}


