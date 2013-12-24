package stf

import (
  "fmt"
  "log"
  "strings"
)

const INDENT_PATTERN string = "  "
type DebugLog struct {
  Prefix string
  Indent string
}

func NewDebugLog() *DebugLog {
  return &DebugLog {
    "",
    "",
  }
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


