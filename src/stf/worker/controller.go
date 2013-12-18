package worker

import (
  "database/sql"
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

type WorkerCommChannel chan WorkerCommand
type HandlerArgs struct {
  Id      string
  MaxJobs int
  JobChan chan *stf.WorkerArg
  ControlChan WorkerCommChannel
  Waiter  *sync.WaitGroup
}

type CreateHandlerFunc func(*HandlerArgs) WorkerCommChannel
type WorkerController struct {
  Name                string
  Config              *stf.Config
  MainDB              *sql.DB
  JobChan             chan *stf.WorkerArg
  FetcherControlChan  chan bool
  SigChan             chan os.Signal
  // channel to read-in stream of commands from workers
  WorkerChan          WorkerCommChannel
  CurrentQueueIdx     int
  DroneId             string
  QueueTableName      string
  QueueTimeout        int
  Waiter              *sync.WaitGroup
  MaxWorkers          int
  MaxJobsPerWorker    int
  // channels to send worker-specific commands
  ActiveWorkers       map[string]WorkerCommChannel
  CreateHandler       CreateHandlerFunc
}

var ErrNothingDequeued = errors.New("Could not find any jobs")
func NewWorkerControllerFromArgv(
  name string,
  tablename string,
  createHandlerFunc CreateHandlerFunc,
) (*WorkerController) {
  var configfile string
  var droneId string
  var timeout   int
  var maxWorkers int
  var maxJobsPerWorker int
  flag.StringVar(&configfile, "config", "etc/config.gcfg", "The path to config file")
  flag.StringVar(&droneId, "drone", "", "drone ID that this belongs to")
  flag.IntVar(&timeout, "timeout", 5, "The timeout for each queue_wait() call")
  flag.IntVar(&maxWorkers, "max-workers", 0, "Number of workers")
  flag.IntVar(&maxJobsPerWorker, "max-jobs-per-worker", 1000, "Number of jobs that each goroutine processes until exiting")
  flag.Parse()

  return NewWorkerController(
    name,
    droneId,
    tablename,
    timeout,
    maxWorkers,
    maxJobsPerWorker,
    createHandlerFunc,
  )
}

func NewWorkerController (
  name string,
  droneId string,
  tablename string,
  timeout int,
  maxWorkers int,
  maxJobsPerWorker int,
  createHandlerFunc CreateHandlerFunc,
) (*WorkerController) {
  home := stf.GetHome()
  cfg, err := stf.LoadConfig(home)
  if err != nil {
    log.Fatalf("Failed to config: %s", err)
  }

  db, err := stf.ConnectDB(&cfg.MainDB)
  if err != nil {
    panic(fmt.Sprintf("Could not connect to main database: %s", err))
  }

  return &WorkerController {
    name,

    cfg,

    db,

    // JobChan, used to pass jobs from fetcher to worker(s)
    make(chan *stf.WorkerArg),

    // FetcherControlChan, used to tell fetcher to stop
    make(chan bool),

    // SigChan, used to propagate signal notification
    make(chan os.Signal, 1),

    // WorkerChan, notifications from worker(s) that they have "exited"
    make(WorkerCommChannel, 1),

    0,

    // Name of the drone that this belongs to
    droneId,

    // This is the name of the queue to listen
    tablename,

    // This is how much we wait per queue_wait()
    timeout,

    &sync.WaitGroup {},

    maxWorkers,

    maxJobsPerWorker,

    map[string]WorkerCommChannel {},

    createHandlerFunc,
  }
}

func (self *WorkerController) Start() {
  log.SetPrefix(fmt.Sprintf("[%s %d] ", self.Name, os.Getpid()))
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

    var skipDequeue <-chan time.Time
    loop := true
    for loop {
      select {
      case <-controlChan:
        log.Printf("Received fetcher termination request. Exiting")
        loop = false
        break
      case <-skipDequeue:
        stf.RandomSleep()
      default:
        stf.RandomSleep()
      }

      if ! loop {
        break
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
        skipDequeue = time.After(500 * time.Millisecond)
      }
    }
    log.Printf("Fetcher for %s exiting", name)
  }(self.Name, self.Waiter, self.JobChan, self.FetcherControlChan)
}

func (self *WorkerController) StartControllerThread () {

  self.Waiter.Add(1)
  go func(w *sync.WaitGroup, sigChan chan os.Signal, workerChan WorkerCommChannel) {
    defer w.Done()
    defer self.KillAll()

    // Before looping, we need to spawn workers
    self.Respawn()

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
      case cmd := <-workerChan:
        // One of our workers has sent us something through this channel
        log.Printf("Received command %v", cmd)
        switch cmd.GetType() {
        case WORKER_EXITED:
          cmdexited, ok := cmd.(*Cmd1Arg)
          if ! ok {
            log.Printf("Unknown command type:/")
          } else {
            doRespawn = true
            delete(self.ActiveWorkers, cmdexited.Arg)
            log.Printf("Worker id %s exited, need to replenish", cmdexited.Arg)
          }
        default:
          log.Printf("Unknown command type %d", cmd.GetType())
        }
      default:
        stf.RandomSleep()
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
  log.Printf(
    "Loading configurations for worker %s drone %s",
    self.Name,
    self.DroneId,
  )

  // Connect to the database and find out how many workers
  // we're supposed to spawn
  db := self.MainDB

  row := db.QueryRow(
    `SELECT instances FROM worker_instances WHERE drone_id = ? AND worker_type = ?`,
    self.DroneId,
    self.Name,
  )

  var count int
  err := row.Scan(&count)

  if err == sql.ErrNoRows {
    return
  }

  if err != nil {
    panic(fmt.Sprintf("Failed to reload config %s", err))
  }

  if self.MaxWorkers != count {
    log.Printf("Changing MaxWorkers from %d -> %d", self.MaxWorkers, count)
    self.MaxWorkers = count
  }
}

func (self *WorkerController) Respawn() {
  // Respawn up to the number of threads specified

  curWorkers := len(self.ActiveWorkers)
  maxWorkers := self.MaxWorkers
  log.Printf("Current worker status: active = %d, max = %d", curWorkers, maxWorkers)

  if curWorkers == maxWorkers {
    // No change
    return
  }

  diff := maxWorkers - curWorkers

  if diff < 0 {
    for id, c := range self.ActiveWorkers {
      if diff >= 0 {
        break
      }

      log.Printf("Killing goroutine '%s'", id)
      c <- CmdStop()
      delete(self.ActiveWorkers, id)
      diff++
    }
  }

  createCount := 0
  for i := 0; i < diff; i++ {
    id := stf.GenerateRandomId(self.Name, 40)
    args := &HandlerArgs{
      id,
      self.MaxJobsPerWorker,
      self.JobChan,
      self.WorkerChan,
      self.Waiter,
    }
    c := self.CreateHandler(args)
    self.ActiveWorkers[id] = c
    createCount++
  }
  log.Printf("Created %d new workers", createCount)
}

func (self *WorkerController) KillAll() {
  // Send the fetcher a termination signal
  log.Printf("KillAll: Sending notice to fetcher")
  self.FetcherControlChan <- true

  for id, c := range self.ActiveWorkers {
    log.Printf("KillAll: Sending notice to worker %s", id)
    c <- CmdStop()
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
  for i := 0; i < max; i++ {
    idx := self.CurrentQueueIdx
    self.CurrentQueueIdx++
    if self.CurrentQueueIdx >= max {
      self.CurrentQueueIdx = 0
    }
    config := qdbConfig[idx]
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