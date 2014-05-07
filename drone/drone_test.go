package drone

import (
	"testing"
	"time"
)

func TestDrone(t *testing.T) {

}

func TestCommandDecl(t *testing.T) {
	thing := make(map[DroneCmd]struct {})
	consts := []DroneCmd {
		CmdStopDrone,
		CmdAnnounce,
		CmdSpawnMinion,
		CmdReloadMinion,
		CmdCheckState,
		CmdElection,
		CmdRebalance,
		CmdExpireDrone,
	}

	for _, cmd := range consts {
		t.Logf("Checking command %s", cmd)
		if _, exists := thing[cmd]; exists {
			t.Errorf("command type %s already exists ?! Duplicate value?", cmd)
		}
	}

}

func TestPeriodicTask(t *testing.T) {
	var prev time.Time
	count := 0
	c := make(chan struct{})
	task := NewPeriodicTask(time.Second, false, func (pt *PeriodicTask) {
		now := time.Now()
		if ! prev.IsZero() {
			diff := now.Sub(prev)
			if diff < time.Second {
				t.Errorf("Whoa, fired periodic task fired in %s!", diff)
			}
		}

		if count++; count > 5 {
			pt.Stop()
			c<-struct{}{}
		}
	})

	go task.Run()

	<-c

	if count != 6 {
		t.Errorf("Expected 5 iterations to be executed, got %d", count)
	}
}