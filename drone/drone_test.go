package drone

import (
	"testing"
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