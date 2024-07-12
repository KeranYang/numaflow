package nats

import (
	"context"
	"os"
	"testing"

	"github.com/nats-io/nats.go"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	natstest "github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
)

func TestNewNATSClient(t *testing.T) {
	// Setting up environment variables for the test
	os.Setenv(dfv1.EnvISBSvcJetStreamURL, "nats://localhost:4222")
	os.Setenv(dfv1.EnvISBSvcJetStreamUser, "user")
	os.Setenv(dfv1.EnvISBSvcJetStreamPassword, "password")
	defer os.Clearenv()

	log := zap.NewNop().Sugar()

	ctx := logging.WithLogger(context.Background(), log)

	client, err := NewNATSClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	// Cleanup
	client.Close()
}

func TestNewNATSClient_Failure(t *testing.T) {
	// Simulating environment variable absence
	os.Clearenv()

	log := zap.NewNop().Sugar()
	ctx := logging.WithLogger(context.Background(), log)

	client, err := NewNATSClient(ctx)
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestSubscribe(t *testing.T) {
	s := natstest.RunJetStreamServer(t)
	defer s.Shutdown()

	client := NewTestClient(t, s.ClientURL())
	defer client.Close()

	// Create a stream
	js, err := client.nc.JetStream()
	assert.NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST_STREAM",
		Subjects: []string{"test.subject"},
	})
	assert.NoError(t, err)

	// Subscribe to a subject
	sub, err := client.Subscribe("test.subject", "TEST_STREAM")
	assert.NoError(t, err)
	assert.NotNil(t, sub)

	// Test failure case: Invalid stream
	_, err = client.Subscribe("balh", "INVALID_STREAM")
	assert.Error(t, err)
}

func TestBindKVStore(t *testing.T) {
	s := natstest.RunJetStreamServer(t)
	defer s.Shutdown()

	client := NewTestClient(t, s.ClientURL())
	defer client.Close()

	// Create a KeyValue store
	js, err := client.nc.JetStream()
	assert.NoError(t, err)
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "KV_TEST",
	})
	assert.NoError(t, err)

	// Bind to the KeyValue store
	kvStore, err := client.BindKVStore("KV_TEST")
	assert.NoError(t, err)
	assert.NotNil(t, kvStore)

	// Test failure case: Invalid KeyValue store
	_, err = client.BindKVStore("INVALID_KV")
	assert.Error(t, err)
}

func TestJetStreamContext(t *testing.T) {
	s := natstest.RunJetStreamServer(t)
	defer s.Shutdown()

	client := NewTestClient(t, s.ClientURL())
	defer client.Close()

	jsCtx, err := client.JetStreamContext()
	assert.NoError(t, err)
	assert.NotNil(t, jsCtx)
}

func TestNewTestClient(t *testing.T) {
	s := natstest.RunJetStreamServer(t)
	defer s.Shutdown()

	client := NewTestClient(t, s.ClientURL())
	assert.NotNil(t, client)
	defer client.Close()
}

func TestClose(t *testing.T) {
	s := natstest.RunJetStreamServer(t)
	defer s.Shutdown()

	client := NewTestClient(t, s.ClientURL())
	assert.NotNil(t, client)
	client.Close()
}
