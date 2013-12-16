package worker

import (
  "errors"
  "flag"
  "fmt"
  "log"
  "os"
  "os/signal"
  "stf"
  "sync"
  "syscall"
  "time"
)

type WorkerController struct {
  Name                string
  Config              *stf.Config
  JobChan             chan *stf.WorkerArg
  FetcherControlChan  chan bool
  SigChan             chan os.Signal
  WorkerChan          chan string
  CurrentQueueIdx     int
  QueueTableName      string
  QueueTimeout        int
  Waiter              *sync.WaitGroup
  MaxWorkers          int
  ActiveWorkers       map[string]chan bool
  StartWorker         func(*sync.WaitGroup, chan *stf.WorkerArg) chan bool
}

var ErrNothingDequeued = errors.New("Could not find any jobs")
func NewWorkerControllerFromArgv() (*WorkerController) {
  var configfile string
  var name      string
  var tablename string
  var timeout   int
  var maxWorkers int
  flag.StringVar(&configfile, "config", "etc/config.gcfg", "The path to config file")
  flag.StringVar(&name, "name", "", "The worker name")
  flag.StringVar(&tablename, "tablename", "", "The Q4M table name to wait for jobs")
  flag.IntVar(&timeout, "timeout", 5, "The timeout for each queue_wait() call")
  flag.IntVar(&maxWorkers, "max-workers", 5, "Number of workers")
  flag.Parse()

  return NewWorkerController(name, tablename, timeout, maxWorkers)
}

func NewWorkerController (
  name string,
  tablename string,
  timeout int,
  maxWorkers int,
) (*WorkerController) {
  home := stf.GetHome()
  cfg, err := stf.LoadConfig(home)
  if err != nil {
    log.Fatalf("Failed to config: %s", err)
  }

  return &WorkerController {
    name,

    cfg,

    // JobChan, used to pass jobs from fetcher to worker(s)
    make(chan *stf.WorkerArg),

    // FetcherControlChan, used to tell fetcher to stop
    make(chan bool),

    // SigChan, used to propagate signal notification
    make(chan os.Signal, 1),

    // WorkerChan, notifications from worker(s) that they have "exited"
    make(chan string),

    0,

    // This is the name of the queue to listen
    tablename,

    // This is how much we wait per queue_wait()
    timeout,

    &sync.WaitGroup {},

    // XXX DUMMY
    maxWorkers,

    map[string]chan bool {},

    NewRepairObjectWorker,
  }
}

func (self *WorkerController) Start() {
  // Handle signals. TERM kills this worker-unit, HUP tells us to
  // reload configuration from the database. The actual handling
  // is done by the controller thread
  sigChan := self.SigChan
  signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

  self.StartFetcherThread()
  self.StartControllerThread()

  self.Waiter.Wait()
  log.Printf("Exiting worker unit for %s", self.Name)
}


func (self *WorkerController) StartFetcherThread() {
  self.Waiter.Add(1)
  go func(name string, w *sync.WaitGroup, jobChan chan *stf.WorkerArg, controlChan chan bool) {
    defer w.Done()

    ticker := time.Tick(5 * time.Second)

    skipDequeue := false
    loop := true
    for loop {
      select {
      case <-controlChan:
        log.Printf("Received fetcher termination request. Exiting")
        loop = false
        break
      case <-ticker:
        skipDequeue = false
      default:
        // do nothing
      }

      if ! loop {
        break
      }

      if skipDequeue {
        time.Sleep( 500 * time.Millisecond )
        continue
      }

      // Go and dequeue
      job, err := self.Dequeue()
      switch err {
      case nil:
        jobChan <- job

      default:
        // We encountered an error. It's very likely that we are not going
        // to succeed getting the next one. In that case, go listen to the
        // controlChan, but don't fall into the dequeue clause until the
        // next "tick" arrives
        skipDequeue = true
      }
    }
    log.Printf("Fetcher for %s exiting", name)
  }(self.Name, self.Waiter, self.JobChan, self.FetcherControlChan)
}

func (self *WorkerController) StartControllerThread () {

  self.Waiter.Add(1)
  go func(w *sync.WaitGroup, sigChan chan os.Signal, workerChan chan string) {
    defer w.Done()
    defer self.KillAll()

    ticker := time.Tick(5 * time.Second)

    loop := true
    for loop {
      doRespawn := false
      doReload  := false
      select {
      case sig := <-sigChan:
        log.Printf("Received signal %s", sig)
        if sig == syscall.SIGHUP {
          doReload = true
        } else {
          loop = false
        }
        break
      case <-workerChan:
        doRespawn = true
      case <-ticker:
      }

      if ! loop {
        break
      }

      if doReload {
        self.ReloadConfig()
        doRespawn = true
      }

      if doRespawn {
        self.Respawn()
      }
    }
  }(self.Waiter, self.SigChan, self.WorkerChan)
}

func (self *WorkerController) ReloadConfig() {
}

func (self *WorkerController) Respawn() {
  // Respawn up to the number of threads specified

  curWorkers := len(self.ActiveWorkers)
  maxWorkers := self.MaxWorkers
  if curWorkers >= maxWorkers {
    // Nothing to spawn
    log.Printf("Current number of workers <= Max workers (%d <= %d).", curWorkers, maxWorkers)
    return
  }

  for i := 0; i < (maxWorkers - curWorkers); i++ {
    log.Printf("Spawning new worker")
    id := stf.GenerateRandomId(self.Name, 40)
    c := self.StartWorker(self.Waiter, self.JobChan)
    self.ActiveWorkers[id] = c
  }
}

func (self *WorkerController) KillAll() {
  // Send the fetcher a termination signal
  log.Printf("KillAll: Sending notice to fetcher")
  self.FetcherControlChan <- true

  for id, c := range self.ActiveWorkers {
    log.Printf("KillAll: Sending notice to worker %s", id)
    c <- true
  }
}

func (self *WorkerController) Dequeue() (*stf.WorkerArg, error) {
  qdbConfig := self.Config.QueueDBList

  sql := fmt.Sprintf(
    "SELECT args, created_at FROM %s WHERE queue_wait(?, ?)",
    self.QueueTableName,
  )

  max := len(qdbConfig)
  var arg stf.WorkerArg
  for i := self.CurrentQueueIdx; i < max; i++ {
log.Printf("Checking DB %d", i)
    idx := self.CurrentQueueIdx
    self.CurrentQueueIdx++
    if idx >= max {
      idx = 0
    }
    config := qdbConfig[i]
    db, err := stf.ConnectDB(config)

    if err != nil {
      continue
    }

    row := db.QueryRow(sql, self.QueueTableName, self.QueueTimeout)

    err = row.Scan(&arg.Arg, &arg.CreatedAt)
    db.Exec("SELECT queue_end()") // Call this regardless

    if err != nil {
      continue
    }

    return &arg, nil
  }

  return nil, ErrNothingDequeued
}