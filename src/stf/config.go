package stf

type GlobalConfig struct {
  Debug     bool
}

type DispatcherConfig struct {
  ServerId  uint64
  Listen    string
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
  Dispatcher  DispatcherConfig
  Global      GlobalConfig
  MainDB      DatabaseConfig
  Memcached   MemcachedConfig
  QueueDB     []DatabaseConfig
}
