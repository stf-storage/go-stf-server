package drone

import (
	"fmt"
	"github.com/stf-storage/go-stf-server"
	"github.com/stf-storage/go-stf-server/api"
	"github.com/stf-storage/go-stf-server/cache"
	"github.com/stf-storage/go-stf-server/config"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Drone struct {
	id               string
	ctx              *api.Context
	loop             bool
	leader           bool
	tasks            []*PeriodicTask
	minions          []*Minion
	waiter           *sync.WaitGroup
	CmdChan          chan DroneCmd
	sigchan          chan os.Signal
	lastElectionTime time.Time
}

type DroneCmd int

var (
	CmdStopDrone    = DroneCmd(-1)
	CmdAnnounce     = DroneCmd(0)
	CmdSpawnMinion  = DroneCmd(1)
	CmdReloadMinion = DroneCmd(2)
	CmdCheckState   = DroneCmd(3)
	CmdElection     = DroneCmd(4)
	CmdRebalance    = DroneCmd(5)
	CmdExpireDrone  = DroneCmd(6)
)

func (c DroneCmd) ToString() string {
	switch c {
	case CmdStopDrone:
		return "CmdStopDrone"
	case CmdAnnounce:
		return "CmdAnnounce"
	case CmdSpawnMinion:
		return "CmdSpawnMinion"
	case CmdReloadMinion:
		return "CmdReloadMinion"
	case CmdCheckState:
		return "CmdCheckState"
	case CmdElection:
		return "CmdElection"
	case CmdRebalance:
		return "CmdRebalance"
	case CmdExpireDrone:
		return "CmdExpireDrone"
	default:
		return fmt.Sprintf("UnknownCommand(%d)", c)
	}
}

func NewDrone(config *config.Config) *Drone {
	host, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %s", err)
	}
	pid := os.Getpid()
	id := fmt.Sprintf("%s.%d.%d", host, pid, rand.Int31())
	return &Drone{
		id:               id,
		ctx:              api.NewContext(config),
		loop:             true,
		leader:           false,
		tasks:            nil,
		minions:          nil,
		waiter:           &sync.WaitGroup{},
		CmdChan:          make(chan DroneCmd, 1),
		sigchan:          make(chan os.Signal, 1),
		lastElectionTime: time.Time{},
	}
}

func (d *Drone) Loop() bool {
	return d.loop
}

func (d *Drone) Run() {
	defer func() {
		d.Unregister()
		for _, m := range d.Minions() {
			m.Kill()
		}
	}()

	go d.TimerLoop()
	go d.WaitSignal()

	// We need to kickstart:
	go d.SendCmd(CmdSpawnMinion)

	d.MainLoop()
	stf.Debugf("Drone %s exiting...", d.id)
}

func (d *Drone) WaitSignal() {
	sigchan := d.sigchan
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

OUTER:
	for d.Loop() {
		sig, ok := <-sigchan
		if !ok {
			continue
		}

		stf.Debugf("Received signal %s", sig)

		switch sig {
		case syscall.SIGTERM, syscall.SIGINT:
			d.SendCmd(CmdStopDrone)
			signal.Stop(sigchan)
			break OUTER
		case syscall.SIGHUP:
			d.SendCmd(CmdReloadMinion)
		}
	}
}

func (d *Drone) makeAnnounceTask() *PeriodicTask {
	return &PeriodicTask{
		30 * time.Second,
		true,
		time.Time{},
		func() { d.SendCmd(CmdAnnounce) },
	}
}

func (d *Drone) makeCheckStateTask() *PeriodicTask {
	return &PeriodicTask{
		5 * time.Second,
		true,
		time.Time{},
		func() { d.SendCmd(CmdCheckState) },
	}
}

func (d *Drone) makeElectionTask() *PeriodicTask {
	return &PeriodicTask{
		300 * time.Second,
		false,
		time.Time{},
		func() { d.SendCmd(CmdElection) },
	}
}

func (d *Drone) makeExpireTask() *PeriodicTask {
	return &PeriodicTask{
		60 * time.Second,
		true,
		time.Time{},
		func() { d.SendCmd(CmdExpireDrone) },
	}
}

func (d *Drone) makeRebalanceTask() *PeriodicTask {
	return &PeriodicTask{
		60 * time.Second,
		true,
		time.Time{},
		func() { d.SendCmd(CmdRebalance) },
	}
}

func (d *Drone) PeriodicTasks() []*PeriodicTask {
	if d.tasks != nil {
		return d.tasks
	}

	if d.leader {
		d.tasks = make([]*PeriodicTask, 5)
	} else {
		d.tasks = make([]*PeriodicTask, 4)
	}

	d.tasks[0] = d.makeAnnounceTask()
	d.tasks[1] = d.makeCheckStateTask()
	d.tasks[2] = d.makeElectionTask()
	d.tasks[3] = d.makeExpireTask()
	if d.leader {
		d.tasks[4] = d.makeRebalanceTask()
	}

	return d.tasks
}

func (d *Drone) Minions() []*Minion {
	if d.minions != nil {
		return d.minions
	}

	cmds := []string{
		//    "stf-worker-adaptive_throttler",
		//    "stf-worker-delete_bucket",
		"stf-worker-delete_object",
		"stf-worker-replicate_object",
		"stf-worker-repair_object",
		"stf-worker-storage_health",
	}

	d.minions = make([]*Minion, len(cmds))
	for i, x := range cmds {
		// XXX x is used as an alias, so for each iteration we'd be using
		// the same variable with new value if we bind callback with x
		// in order to avoid that, we need to create a new variable, cmdname
		cmdname := x
		callback := func() *exec.Cmd {
			return exec.Command(cmdname, "--config", d.ctx.Config().FileName)
		}
		d.minions[i] = &Minion{
			drone:   d,
			cmd:     nil,
			makeCmd: callback,
		}
	}
	return d.minions
}

func (d *Drone) NotifyMinions() {
	for _, m := range d.Minions() {
		m.NotifyReload()
	}
}

func (d *Drone) TimerLoop() {
	defer func() {
		os.Stderr.WriteString("TimerLoop exiting...")
	}()
	// Periodically wake up to check if we have anything to do
	c := time.Tick(5 * time.Second)

	// fire once in the beginning
	for _, task := range d.PeriodicTasks() {
		task.Run()
	}

	for {
		t := <-c
		for _, task := range d.PeriodicTasks() {
			if task.ShouldFire(t) {
				task.Run()
			}
		}
	}
}
func (d *Drone) MainLoop() {
	for d.Loop() {
		cmd, ok := <-d.CmdChan
		if ok {
			d.HandleCommand(cmd)
		}
	}
}

func (d *Drone) HandleCommand(cmd DroneCmd) {
	switch cmd {
	case CmdStopDrone:
		stf.Debugf("Hanlding StopDrone")
		d.loop = false
	case CmdAnnounce:
		d.Announce()
	case CmdSpawnMinion:
		d.SpawnMinions()
	case CmdElection:
		d.HoldElection()
	case CmdReloadMinion:
		d.NotifyMinions()
	case CmdCheckState:
		d.CheckState()
	case CmdExpireDrone:
		d.ExpireDrones()
	}
}

func (d *Drone) Announce() error {
	db, err := d.ctx.MainDB()
	if err != nil {
		return err
	}

	stf.Debugf("Annoucing %s", d.id)
	db.Exec(`
    INSERT INTO worker_election (drone_id, expires_at)
      VALUES (?, UNIX_TIMESTAMP() + 300)
      ON DUPLICATE KEY UPDATE expires_at = UNIX_TIMESTAMP() + 300`,
		d.id,
	)

	mc := d.ctx.Cache()
	mc.Set("go-stf.worker.election", &cache.Int64Value{time.Now().Unix()}, 0)

	return nil
}

func (d *Drone) ExpireDrones() {
	db, err := d.ctx.MainDB()
	if err != nil {
		return
	}

	rows, err := db.Query(`SELECT drone_id FROM worker_election WHERE expires_at <= UNIX_TIMESTAMP()`)
	if err != nil {
		return
	}

	for rows.Next() {
		var id string
		err = rows.Scan(&id)
		if err != nil {
			return
		}
		stf.Debugf("Expiring drone %s", id)
		db.Exec(`DELETE FROM worker_election WHERE drone_id = ?`, id)
		db.Exec(`DELETE FROM worker_instances WHERE drone_id = ?`, id)
	}
}

func (d *Drone) Unregister() error {
	db, err := d.MainDB()
	if err != nil {
		return err
	}

	stf.Debugf("Unregistering drone %s from database", d.id)
	db.Exec(`DELETE FROM worker_election WHERE drone_id = ?`, d.id)
	db.Exec(`DELETE FROM worker_instances WHERE drone_id = ?`, d.id)

	mc := d.ctx.Cache()
	mc.Set("go-stf.worker.election", &cache.Int64Value{time.Now().Unix()}, 0)

	return nil
}

func (d *Drone) CheckState() {
	stf.Debugf("Checking state")

	// Have we run an election up to this point?
	lastElectionTime := d.lastElectionTime
	if lastElectionTime.IsZero() {
		stf.Debugf("First time!")
		// then we should just run the election, regardless
		go d.SendCmd(CmdElection)
		return
	}

	var v cache.Int64Value
	mc := d.ctx.Cache()
	err := mc.Get("go-stf.worker.election", &v)
	if err != nil {
		stf.Debugf("Failed to fetch election cache key: %s", err)
		return
	}

	tnano := time.Unix(v.Value, 0)
	now := time.Unix(time.Now().Unix(), 0)
	stf.Debugf("t.Now = %s", now)
	stf.Debugf("last election = %s", lastElectionTime)
	stf.Debugf("tnano = %s", tnano)
	if lastElectionTime.After(tnano) {
		stf.Debugf("last election > tnano, no election")
		return
	}

	if now.After(tnano) {
		go d.SendCmd(CmdElection)
	}
}

func (d *Drone) MainDB() (*stf.DB, error) {
	return d.ctx.MainDB()
}

func (d *Drone) HoldElection() error {
	db, err := d.MainDB()
	if err != nil {
		return err
	}

	stf.Debugf("Holding leader election")
	row := db.QueryRow(`SELECT drone_id FROM worker_election ORDER BY id ASC LIMIT 1`)

	var id string
	err = row.Scan(&id)
	if err != nil {
		return err
	}

	if d.id == id {
		stf.Debugf("Elected myself (%s) as leader", id)
		d.leader = true
	} else {
		stf.Debugf("Elected %s as leader", id)
		d.leader = false
	}
	d.lastElectionTime = time.Unix(time.Now().Unix(), 0)
	return nil
}

func (d *Drone) SendCmd(cmd DroneCmd) {
	if !d.Loop() {
		return
	}

	d.CmdChan <- cmd
}

func (d *Drone) SpawnMinions() {
	for _, m := range d.Minions() {
		m.Run()
	}
}
