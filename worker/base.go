package worker

import (
	"flag"
	"github.com/stf-storage/go-stf-server"
	"github.com/stf-storage/go-stf-server/api"
	"github.com/stf-storage/go-stf-server/config"
	"log"
	"os"
	"path"
	"sync"
)

type Fetcher interface {
	Loop(Worker)
	IsRunning() bool
}

type Worker interface {
	Name() string
	Loop() bool
	ActiveSlaves() int
	Ctx() *api.Context
	SendJob(*api.WorkerArg)
}

type BaseWorker struct {
	name         string
	loop         bool
	activeSlaves int
	maxSlaves    int
	ctx          *api.Context
	waiter       *sync.WaitGroup
	fetcher      Fetcher
	CmdChan      chan WorkerCmd
	jobChan      chan *api.WorkerArg
	WorkCb       func(*api.WorkerArg) error
}

type WorkerCmd int

var (
	CmdWorkerSlaveSpawn    = WorkerCmd(1)
	CmdWorkerSlaveStop     = WorkerCmd(2)
	CmdWorkerSlaveStarted  = WorkerCmd(3)
	CmdWorkerSlaveExited   = WorkerCmd(4)
	CmdWorkerReload        = WorkerCmd(5)
	CmdWorkerFetcherSpawn  = WorkerCmd(6)
	CmdWorkerFetcherExited = WorkerCmd(7)
)

func NewBaseWorker(name string, f Fetcher) *BaseWorker {
	return &BaseWorker{
		name,
		true,
		0,
		0,
		nil,
		&sync.WaitGroup{},
		f,
		make(chan WorkerCmd, 16),
		make(chan *api.WorkerArg, 16),
		nil,
	}
}

func (w *BaseWorker) Run() {
	closer := stf.LogMark("[Worker:%s]", w.name)
	defer closer()

	var configFile string

	pwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Could not determine current working directory")
	}

	defaultConfig := path.Join(pwd, "etc", "config.gcfg")

	flag.StringVar(
		&configFile,
		"config",
		defaultConfig,
		"Path to config file",
	)
	flag.Parse()

	os.Setenv("STF_CONFIG", configFile)

	cfg, err := config.BootstrapConfig()
	if err != nil {
		stf.Debugf("Failed to load config: %s", err)
		return
	}

	w.ctx = api.NewContext(cfg)

	w.SendCmd(CmdWorkerSlaveSpawn)

	w.MainLoop()
	stf.Debugf("Worker exiting")
}

func (w *BaseWorker) SendCmd(cmd WorkerCmd) {
	if !w.Loop() {
		return
	}

	w.CmdChan <- cmd
}

func (w *BaseWorker) Name() string {
	return w.name
}

func (w *BaseWorker) Ctx() *api.Context {
	return w.ctx
}

func (w *BaseWorker) ActiveSlaves() int {
	return w.activeSlaves
}

func (w *BaseWorker) SendJob(job *api.WorkerArg) {
	if !w.Loop() {
		return
	}

	w.jobChan <- job
}

func (w *BaseWorker) Loop() bool {
	return w.loop
}

func (w *BaseWorker) MainLoop() {
	for {
		cmd := <-w.CmdChan
		w.HandleCommand(cmd)
	}
}

func (w *BaseWorker) HandleCommand(cmd WorkerCmd) {
	switch cmd {
	case CmdWorkerReload:
		w.ReloadConfig()
	case CmdWorkerFetcherSpawn:
		w.FetcherSpawn()
	case CmdWorkerFetcherExited:
		stf.Debugf("Fetcher exited")
		w.SendCmd(CmdWorkerFetcherSpawn)
	case CmdWorkerSlaveStop:
		w.SlaveStop()
	case CmdWorkerSlaveSpawn:
		w.SlaveSpawn()
	case CmdWorkerSlaveStarted:
		w.activeSlaves++
		w.SendCmd(CmdWorkerFetcherSpawn)
	case CmdWorkerSlaveExited:
		w.activeSlaves--
		w.SendCmd(CmdWorkerSlaveSpawn)
	}
}

func (w *BaseWorker) ReloadConfig() {
	// Get the number of slaves that we need to spawn.
	// If this number is different than the previous state,
	// send myself CmdWorkerSlaveStop or CmdWorkerStartSlave
	if !w.loop {
		return
	}

	oldMaxSlaves := w.maxSlaves

	diff := oldMaxSlaves - w.activeSlaves
	if diff > 0 {
		for i := 0; i < diff; i++ {
			w.SendCmd(CmdWorkerSlaveStop)
		}
	} else if diff < 0 {
		diff = diff * -1
		for i := 0; i < diff; i++ {
			w.SendCmd(CmdWorkerSlaveSpawn)
		}
	}
}

func (w *BaseWorker) FetcherSpawn() {
	if !w.loop {
		return
	}

	// do we already have a fetcher?
	if w.fetcher.IsRunning() {
		return
	}

	go func() {
		defer w.SendCmd(CmdWorkerFetcherExited)
		w.fetcher.Loop(w)
	}()
}

func (w *BaseWorker) SlaveStop() {
	if !w.loop {
		return
	}

	w.jobChan <- nil
}

func (w *BaseWorker) SlaveSpawn() {
	// Bail out if we're supposed to be exiting
	if !w.loop {
		return
	}

	go w.SlaveLoop()
}

func (w *BaseWorker) SlaveLoop() {
	w.SendCmd(CmdWorkerSlaveStarted)
	// if we exited, spawn a new one
	defer func() { w.SendCmd(CmdWorkerSlaveExited) }()

	stf.Debugf("New slave starting...")
	for {
		job := <-w.jobChan
		if job == nil {
			break // Bail out of loop
		}
		w.WorkCb(job)
	}
}
