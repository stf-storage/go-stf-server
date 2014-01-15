package stf

import(
  "log"
  "os"
  "path"
  "path/filepath"
  "code.google.com/p/gcfg"
)

type GlobalConfig struct {
  Debug     bool
}

type DispatcherConfig struct {
  ServerId      uint64
  Listen        string
  AccessLog     string // /path/to/accesslog.%Y%m%d
  AccessLogLink string // /path/to/accesslog
}

type DatabaseConfig struct {
  Dbtype        string
  Username      string
  Password      string
  // tcp(127.0.0.1:3306)
  // unix(/path/to/sock)
  ConnectString string
  Dbname        string
}

type MemcachedConfig struct {
  Servers   []string
}

type Config struct {
  FileName    string
  Dispatcher  DispatcherConfig
  Global      GlobalConfig
  MainDB      DatabaseConfig
  Memcached   MemcachedConfig
  QueueDB     map[string]*DatabaseConfig
  QueueDBList []*DatabaseConfig
}

func LoadConfig (home string) (*Config, error) {
  cfg   := &Config {}
  file  := os.Getenv("STF_CONFIG")
  if file == "" {
    file = path.Join("etc", "config.gcfg")
  }
  if ! filepath.IsAbs(file) {
    file = path.Join(home, file)
  }
  log.Printf("Loading config file %s", file)

  err := gcfg.ReadFileInto(cfg, file)
  if err != nil {
    return nil, err
  }

  list := []*DatabaseConfig {}
  for k, _ := range cfg.QueueDB {
    list = append(list, cfg.QueueDB[k])
  }
  cfg.QueueDBList = list
  cfg.FileName = file

  return cfg, nil
}
