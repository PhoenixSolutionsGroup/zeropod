package shim

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckpointExtraArgs(t *testing.T) {
	for version, tc := range map[string]struct {
		expectedArgs []string
	}{
		"":        {expectedArgs: []string{}},
		"invalid": {expectedArgs: []string{}},
		"1.0.0":   {expectedArgs: []string{}},
		"1.2.0":   {expectedArgs: []string{}},
		"1.2.6":   {expectedArgs: []string{}},
		"1.3.0":   {expectedArgs: []string{checkpointArgSkipTCPInFlight, checkpointArgForceIRMap}},
		"1.3.3":   {expectedArgs: []string{checkpointArgSkipTCPInFlight, checkpointArgForceIRMap}},
		"1.10.4":  {expectedArgs: []string{checkpointArgSkipTCPInFlight, checkpointArgForceIRMap}},
		"20.0.4":  {expectedArgs: []string{checkpointArgSkipTCPInFlight, checkpointArgForceIRMap}},
	} {
		t.Run(version, func(t *testing.T) {
			c := Container{runcVersion: version}
			assert.Equal(t, tc.expectedArgs, c.checkpointExtraArgs())
		})
	}
}
