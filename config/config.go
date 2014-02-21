package config

import(
  "errors"
  "fmt"
  "log"
  "os"
  "os/user"
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
  QueueDB     map[string]*QueueConfig
  QueueDBList []*QueueConfig
}

func BootstrapConfig() (*Config, error) {
  home := os.Getenv("STF_HOME")
  if home == "" {
    var err error
    home, err = os.Getwd()
    if err != nil {
      log.Fatalf("Failed to get home from env and Getwd: %s", err)
    }
  }

  cfg   := Config {}
  file  := os.Getenv("STF_CONFIG")
  if file == "" {
    file = path.Join("etc", "config.gcfg")
  }
  if ! filepath.IsAbs(file) {
    file = path.Join(home, file)
  }

  err := gcfg.ReadFileInto(&cfg, file)
  if err != nil {
    return nil, errors.New(
      fmt.Sprintf(
        "Failed to load config file '%s': %s",
        file,
        err,
      ),
    )
  }

  cfg.FileName = file
  cfg.Prepare()
  return &cfg, nil
}

func (cfg *Config) Prepare() {
  l := len(cfg.QueueDB)
  list := make([]*QueueConfig, l)
  i := 0
  for _, v := range cfg.QueueDB {
    list[i] = v
    i++
  }
  cfg.QueueDBList = list
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
    return nil, errors.New(
      fmt.Sprintf(
        "Failed to load config file '%s': %s",
        file,
        err,
      ),
    )
  }

  cfg.FileName = file
  cfg.Prepare()


  return cfg, nil
}

func (config *DatabaseConfig) Dsn() (string, error) {
  if config.Dbtype == "" {
    config.Dbtype = "mysql"
  }

  if config.ConnectString == "" {
    switch config.Dbtype {
    case "mysql":
      config.ConnectString = "tcp(127.0.0.1:3306)"
    default:
      return "", errors.New(
        fmt.Sprintf(
          "No database connect string provided, and can't assign a default value for dbtype '%s'",
          config.Dbtype,
        ),
      )
    }
  }

  if config.Username == "" {
    u, err := user.Current()
    if err == nil {
      config.Username = u.Username
    } else {
      config.Username = "root"
    }
  }

  if config.Dbname == "" {
    config.Dbname = "stf"
  }

  dsn := fmt.Sprintf(
    "%s:%s@%s/%s?parseTime=true",
    config.Username,
    config.Password,
    config.ConnectString,
    config.Dbname,
  )

  return dsn, nil
}
