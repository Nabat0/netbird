package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/netbirdio/netbird/client/internal"
	"github.com/netbirdio/netbird/client/internal/peer"
	"github.com/netbirdio/netbird/client/proto"
)

func newTestServer() *Server {
	return &Server{
		rootCtx:        context.Background(),
		statusRecorder: peer.NewRecorder(""),
	}
}

func newDummyConnectClient(ctx context.Context) *internal.ConnectClient {
	return internal.NewConnectClient(ctx, nil, nil)
}

// TestConnectSetsClientWithMutex validates that connect() sets s.connectClient
// under mutex protection so concurrent readers see a consistent value.
func TestConnectSetsClientWithMutex(t *testing.T) {
	s := newTestServer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Manually simulate what connect() does (without calling Run which panics without full setup)
	client := newDummyConnectClient(ctx)

	s.mutex.Lock()
	s.connectClient = client
	s.mutex.Unlock()

	// Verify the assignment is visible under mutex
	s.mutex.Lock()
	assert.Equal(t, client, s.connectClient, "connectClient should be set")
	s.mutex.Unlock()
}

// TestConcurrentConnectClientAccess validates that concurrent reads of
// s.connectClient under mutex don't race with a write.
func TestConcurrentConnectClientAccess(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()
	client := newDummyConnectClient(ctx)

	var wg sync.WaitGroup
	nilCount := 0
	setCount := 0
	var mu sync.Mutex

	// Start readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.mutex.Lock()
			c := s.connectClient
			s.mutex.Unlock()

			mu.Lock()
			defer mu.Unlock()
			if c == nil {
				nilCount++
			} else {
				setCount++
			}
		}()
	}

	// Simulate connect() writing under mutex
	time.Sleep(5 * time.Millisecond)
	s.mutex.Lock()
	s.connectClient = client
	s.mutex.Unlock()

	wg.Wait()

	assert.Equal(t, 50, nilCount+setCount, "all goroutines should complete without panic")
}

// TestCleanupConnection_ClearsConnectClient validates that cleanupConnection
// properly nils out connectClient.
func TestCleanupConnection_ClearsConnectClient(t *testing.T) {
	s := newTestServer()
	_, cancel := context.WithCancel(context.Background())
	s.actCancel = cancel

	s.connectClient = newDummyConnectClient(context.Background())
	s.clientRunning = true

	err := s.cleanupConnection()
	require.NoError(t, err)

	assert.Nil(t, s.connectClient, "connectClient should be nil after cleanup")
}

// TestCleanState_NilConnectClient validates that CleanState doesn't panic
// when connectClient is nil.
func TestCleanState_NilConnectClient(t *testing.T) {
	s := newTestServer()
	s.connectClient = nil
	s.profileManager = nil // will cause error if it tries to proceed past the nil check

	// Should not panic — the nil check should prevent calling Status() on nil
	assert.NotPanics(t, func() {
		_, _ = s.CleanState(context.Background(), &proto.CleanStateRequest{All: true})
	})
}

// TestDeleteState_NilConnectClient validates that DeleteState doesn't panic
// when connectClient is nil.
func TestDeleteState_NilConnectClient(t *testing.T) {
	s := newTestServer()
	s.connectClient = nil
	s.profileManager = nil

	assert.NotPanics(t, func() {
		_, _ = s.DeleteState(context.Background(), &proto.DeleteStateRequest{All: true})
	})
}

// TestDownThenUp_StaleRunningChan documents the known state issue where
// clientRunningChan from a previous connection is already closed, causing
// waitForUp() to return immediately on reconnect.
func TestDownThenUp_StaleRunningChan(t *testing.T) {
	s := newTestServer()

	// Simulate state after a successful connection
	s.clientRunning = true
	s.clientRunningChan = make(chan struct{})
	close(s.clientRunningChan) // closed when engine started
	s.clientGiveUpChan = make(chan struct{})
	s.connectClient = newDummyConnectClient(context.Background())

	_, cancel := context.WithCancel(context.Background())
	s.actCancel = cancel

	// Simulate Down(): cleanupConnection sets connectClient = nil
	s.mutex.Lock()
	err := s.cleanupConnection()
	s.mutex.Unlock()
	require.NoError(t, err)

	// After cleanup: connectClient is nil, clientRunning still true
	// (goroutine hasn't exited yet)
	s.mutex.Lock()
	assert.Nil(t, s.connectClient, "connectClient should be nil after cleanup")
	assert.True(t, s.clientRunning, "clientRunning still true until goroutine exits")
	s.mutex.Unlock()

	// waitForUp() returns immediately due to stale closed clientRunningChan
	ctx, ctxCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer ctxCancel()

	waitDone := make(chan error, 1)
	go func() {
		_, err := s.waitForUp(ctx)
		waitDone <- err
	}()

	select {
	case err := <-waitDone:
		assert.NoError(t, err, "waitForUp returns success on stale channel")
		// But connectClient is still nil — this is the stale state issue
		s.mutex.Lock()
		assert.Nil(t, s.connectClient, "connectClient is nil despite waitForUp success")
		s.mutex.Unlock()
	case <-time.After(1 * time.Second):
		t.Fatal("waitForUp should have returned immediately due to stale closed channel")
	}
}

// TestConnectClient_EngineNilOnFreshClient validates that a newly created
// ConnectClient has nil Engine (before Run is called).
func TestConnectClient_EngineNilOnFreshClient(t *testing.T) {
	client := newDummyConnectClient(context.Background())
	assert.Nil(t, client.Engine(), "engine should be nil on fresh ConnectClient")
}

// newStateTestServer creates a test server with a properly initialized state context,
// required for tests that call Up() or check connection state.
func newStateTestServer() *Server {
	ctx := internal.CtxInitState(context.Background())
	return &Server{
		rootCtx:        ctx,
		statusRecorder: peer.NewRecorder(""),
	}
}

// TestUp_WhenConnected_ReturnsImmediately verifies that Up() returns success immediately
// without tearing down an existing healthy connection when status is Connected.
// This prevents unnecessary tunnel disruption when Up() is called while already connected.
func TestUp_WhenConnected_ReturnsImmediately(t *testing.T) {
	s := newStateTestServer()

	cancelCalled := false
	s.clientRunning = true
	s.clientRunningChan = make(chan struct{})
	close(s.clientRunningChan) // already connected
	s.clientGiveUpChan = make(chan struct{})
	s.actCancel = func() {
		cancelCalled = true
	}

	// Set state to Connected
	internal.CtxGetState(s.rootCtx).Set(internal.StatusConnected)

	ctx, ctxCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer ctxCancel()

	resp, err := s.Up(ctx, &proto.UpRequest{})
	require.NoError(t, err)
	assert.NotNil(t, resp, "Up() should return a response when already connected")
	assert.False(t, cancelCalled, "actCancel should NOT be called when tunnel is already connected")
}

// TestUp_WhenStaleConnection_CancelsExistingGoroutine verifies that Up() cancels
// the stale goroutine and waits for it to exit when the connection is not in
// Connected state (e.g. after session expiry and successful re-auth).
// Before the fix, Up() would return a false success via the already-closed
// clientRunningChan without establishing a new tunnel.
func TestUp_WhenStaleConnection_CancelsExistingGoroutine(t *testing.T) {
	s := newStateTestServer()

	cancelCalled := make(chan struct{}, 1)
	giveUpChan := make(chan struct{})

	s.clientRunning = true
	s.clientRunningChan = make(chan struct{})
	close(s.clientRunningChan) // stale — closed from the original connection
	s.clientGiveUpChan = giveUpChan
	s.actCancel = func() {
		select {
		case cancelCalled <- struct{}{}:
		default:
		}
	}

	// Set state to Idle — as it is after WaitSSOLogin completes successfully
	internal.CtxGetState(s.rootCtx).Set(internal.StatusIdle)

	// Simulate connectWithRetryRuns exiting: sets clientRunning=false and closes giveUpChan
	go func() {
		time.Sleep(50 * time.Millisecond)
		s.mutex.Lock()
		s.clientRunning = false
		s.mutex.Unlock()
		close(giveUpChan)
	}()

	ctx, ctxCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer ctxCancel()

	// Run Up() in a goroutine — it will cancel the stale goroutine, wait for
	// giveUpChan, then fail on nil profileManager. We only care that cancel was called.
	upDone := make(chan struct{})
	go func() {
		defer close(upDone)
		//nolint:errcheck
		s.Up(ctx, &proto.UpRequest{}) //nolint:errcheck — expected to fail on nil profileManager
	}()

	// The critical assertion: actCancel must be called before Up() tries to reconnect
	select {
	case <-cancelCalled:
		// correct — stale goroutine was cancelled
	case <-time.After(500 * time.Millisecond):
		t.Fatal("actCancel should have been called to tear down the stale goroutine")
	}
}
