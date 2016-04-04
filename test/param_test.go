package test

import (
	"github.com/nats-io/stan-server/server"
	"testing"
)

// TestServerParameters
func TestServerParameters(t *testing.T) {
	serverOpts := &server.DefaultServerOptions

	// Test passing nil options
	s := server.RunServerWithOpts(nil, nil)
	s.Shutdown()

	serverOpts.ID = "foo"
	s = server.RunServerWithOpts(serverOpts, nil)
	clusterID := s.ClusterID()
	s.Shutdown()

	if "foo" != clusterID {
		t.Fatalf("Expected cluster ID of foo, found %s\n", clusterID)
	}
	s.Shutdown()
}
