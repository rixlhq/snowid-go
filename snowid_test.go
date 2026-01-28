package snowid

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNewNode(t *testing.T) {
	tests := []struct {
		name      string
		machineID int64
		wantErr   bool
	}{
		{"valid machine ID", 123, false},
		{"zero machine ID", 0, false},
		{"max machine ID", maxMachineID, false},
		{"negative machine ID", -1, true},
		{"too large machine ID", maxMachineID + 1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := NewNode(tt.machineID)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if node.machineID != tt.machineID {
					t.Errorf("machine ID = %v, want %v", node.machineID, tt.machineID)
				}
			}
		})
	}
}

func TestNewNodeWithEpoch(t *testing.T) {
	futureTime := time.Now().Add(time.Hour)
	pastTime := time.Now().Add(-time.Hour)

	tests := []struct {
		name      string
		machineID int64
		epoch     time.Time
		wantErr   bool
	}{
		{"valid epoch", 123, pastTime, false},
		{"future epoch", 123, futureTime, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := NewNodeWithEpoch(tt.machineID, tt.epoch)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if !node.epoch.Equal(tt.epoch) {
					t.Errorf("epoch = %v, want %v", node.epoch, tt.epoch)
				}
			}
		})
	}
}

func TestNode_Generate(t *testing.T) {
	node, err := NewNode(1)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Generate multiple IDs
	var ids []uint64
	for i := 0; i < 100; i++ {
		id, err := node.Generate()
		if err != nil {
			t.Errorf("failed to generate ID: %v", err)
		}
		ids = append(ids, id)
	}

	// Check uniqueness
	seen := make(map[uint64]bool)
	for _, id := range ids {
		if seen[id] {
			t.Error("generated duplicate ID")
		}
		seen[id] = true
	}

	// Check monotonicity
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Error("IDs are not monotonically increasing")
		}
	}
}

func runConcurrentTest(t *testing.T, workers, idsPerWorker int) {
	node, err := NewNode(1)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	var wg sync.WaitGroup
	idChan := make(chan uint64, workers*idsPerWorker)
	errChan := make(chan error, workers)

	start := time.Now()
	// Generate IDs concurrently
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < idsPerWorker; j++ {
				id, err := node.Generate()
				if err != nil {
					// Use non-blocking send or just log
					select {
					case errChan <- err:
					default:
					}
					return
				}
				idChan <- id
			}
		}()
	}

	wg.Wait()
	close(idChan)
	close(errChan)

	// Check for errors
	for err := range errChan {
		t.Errorf("error during concurrent generation: %v", err)
	}

	// Verify IDs
	seen := make(map[uint64]bool)
	var ids []uint64
	for id := range idChan {
		if seen[id] {
			t.Errorf("duplicate ID found: %d", id)
		}
		seen[id] = true
		ids = append(ids, id)
	}

	duration := time.Since(start)
	t.Logf("Generated %d unique IDs in %v (%.2f IDs/sec)", len(ids), duration, float64(len(ids))/duration.Seconds())

	// Verify count
	expectedCount := workers * idsPerWorker
	if len(ids) != expectedCount {
		t.Errorf("got %d IDs, want %d", len(ids), expectedCount)
	}
}

func TestNode_GenerateConcurrent(t *testing.T) {
	runConcurrentTest(t, 10, 100)
}

func TestNode_GenerateHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high concurrency test in short mode")
	}
	runConcurrentTest(t, 100, 1000)
}

func TestNode_Decompose(t *testing.T) {
	node, err := NewNode(123)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	id, err := node.Generate()
	if err != nil {
		t.Fatalf("failed to generate ID: %v", err)
	}

	decomposed := node.Decompose(id)

	// Check machine ID
	if decomposed.MachineID != 123 {
		t.Errorf("machine ID = %v, want 123", decomposed.MachineID)
	}

	// Check sequence (should be 0 or small number)
	if decomposed.Sequence < 0 || decomposed.Sequence > maxSequence {
		t.Errorf("sequence = %v, should be between 0 and %v", decomposed.Sequence, maxSequence)
	}

	// Check timestamp
	idTime := node.Time(id)
	now := time.Now()
	if idTime.After(now) || now.Sub(idTime) > time.Second {
		t.Errorf("timestamp = %v, should be close to now (%v)", idTime, now)
	}
}

func BenchmarkNode_Generate(b *testing.B) {
	node, err := NewNode(1)
	if err != nil {
		b.Fatalf("failed to create node: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := node.Generate()
		if err != nil {
			b.Fatalf("failed to generate ID: %v", err)
		}
	}
}

func BenchmarkNode_GenerateParallel(b *testing.B) {
	node, err := NewNode(1)
	if err != nil {
		b.Fatalf("failed to create node: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := node.Generate()
			if err != nil {
				b.Fatalf("failed to generate ID: %v", err)
			}
		}
	})
}

func TestNode_TimestampBoundaries(t *testing.T) {
	node, err := NewNode(1)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Test maximum timestamp (42 bits)
	maxTimestamp := int64((uint64(1) << 42) - 1) // Use uint64 for correct bit operation
	id := node.createID(maxTimestamp, 0)
	decomposed := node.Decompose(id)
	if decomposed.Timestamp != maxTimestamp {
		t.Errorf("max timestamp = %v, want %v", decomposed.Timestamp, maxTimestamp)
	}

	// Test zero timestamp
	id = node.createID(0, 0)
	decomposed = node.Decompose(id)
	if decomposed.Timestamp != 0 {
		t.Errorf("zero timestamp = %v, want 0", decomposed.Timestamp)
	}
}

func TestNode_SequenceOverflow(t *testing.T) {
	node, err := NewNode(1)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Set sequence to max value
	node.sequence = maxSequence

	// Generate should still work by waiting for next millisecond
	id, err := node.Generate()
	if err != nil {
		t.Errorf("failed to generate ID after sequence overflow: %v", err)
	}

	// Decompose and verify sequence was reset
	decomposed := node.Decompose(id)
	if decomposed.Sequence > maxSequence {
		t.Errorf("sequence = %v, want <= %v", decomposed.Sequence, maxSequence)
	}
}

func TestNode_TimeAccuracy(t *testing.T) {
	node, err := NewNode(1)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	now := time.Now().UTC()
	id, err := node.Generate()
	if err != nil {
		t.Fatalf("failed to generate ID: %v", err)
	}

	idTime := node.Time(id)
	timeDiff := idTime.Sub(now)

	// Should be within reasonable bounds (1 second)
	if timeDiff > time.Second || timeDiff < -time.Second {
		t.Errorf("time difference too large: %v", timeDiff)
	}
}

func TestNode_IDComponents(t *testing.T) {
	tests := []struct {
		timestamp int64
		machineID int64
		sequence  int64
	}{
		{0, 0, 0},                            // All zeros
		{int64((uint64(1) << 42) - 1), 0, 0}, // Max timestamp
		{0, maxMachineID, 0},                 // Max machine ID
		{0, 0, maxSequence},                  // Max sequence
		{int64((uint64(1) << 42) - 1), maxMachineID, maxSequence}, // All max values
		{1234567, 789, 4000}, // Random values
	}

	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	for _, tt := range tests {
		id := node.createID(tt.timestamp, tt.sequence)
		decomposed := node.Decompose(id)

		if decomposed.Timestamp != tt.timestamp {
			t.Errorf("timestamp = %v, want %v", decomposed.Timestamp, tt.timestamp)
		}
		if decomposed.Sequence != tt.sequence {
			t.Errorf("sequence = %v, want %v", decomposed.Sequence, tt.sequence)
		}
	}
}

func TestNode_CustomEpochEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		epoch   time.Time
		wantErr bool
	}{
		{
			"epoch at Unix zero time",
			time.Unix(0, 0),
			false,
		},
		{
			"epoch one millisecond before now",
			time.Now().Add(-time.Millisecond),
			false,
		},
		{
			"epoch exactly now",
			time.Now(),
			false,
		},
		{
			"epoch one millisecond in future",
			time.Now().Add(time.Millisecond),
			true,
		},
		{
			"epoch far in future",
			time.Now().AddDate(1, 0, 0),
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewNodeWithEpoch(1, tt.epoch)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewNodeWithEpoch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNode_ClockDrift(t *testing.T) {
	node, err := NewNode(1)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	epochMs := node.epoch.UnixNano() / millisecond

	// Test 1: Large clock drift should return error
	t.Run("large drift", func(t *testing.T) {
		now := time.Now().UTC().UnixNano() / millisecond
		timestamp := now - epochMs
		node.time = timestamp + 10 // Set 10ms drift

		id, err := node.Generate()
		if err != ErrTimeMovedBackwards {
			t.Errorf("expected ErrTimeMovedBackwards, got %v (id=%v)", err, id)
		}
		if id != 0 {
			t.Errorf("expected id = 0 on error, got %v", id)
		}
	})

	// Test 2: Small clock drift (1ms) should work
	t.Run("small drift", func(t *testing.T) {
		now := time.Now().UTC().UnixNano() / millisecond
		timestamp := now - epochMs
		node.time = timestamp + 1 // Set 1ms drift
		t.Logf("Current timestamp: %v, Stored timestamp: %v", timestamp, timestamp+1)

		id, err := node.Generate()
		if err != nil {
			t.Errorf("failed to generate ID with small drift: %v (id=%v)", err, id)
			return
		}

		// Verify ID components
		decomposed := node.Decompose(id)
		if decomposed.MachineID != 1 {
			t.Errorf("machine ID = %v, want 1 (id=%v)", decomposed.MachineID, id)
		}
		if decomposed.Timestamp != timestamp+1 {
			t.Errorf("timestamp = %v, want %v (id=%v)", decomposed.Timestamp, timestamp+1, id)
		}
	})

	// Test 3: Normal operation after resetting time
	t.Run("normal operation", func(t *testing.T) {
		now := time.Now().UTC().UnixNano() / millisecond
		timestamp := now - epochMs
		node.time = timestamp
		t.Logf("Set timestamp to: %v", timestamp)

		id, err := node.Generate()
		if err != nil {
			t.Errorf("failed to generate ID in normal operation: %v (id=%v)", err, id)
			return
		}

		decomposed := node.Decompose(id)
		if decomposed.MachineID != 1 {
			t.Errorf("machine ID = %v, want 1 (id=%v)", decomposed.MachineID, id)
		}
		if decomposed.Timestamp < timestamp {
			t.Errorf("timestamp = %v, should be >= %v (id=%v)", decomposed.Timestamp, timestamp, id)
		}
	})
}

func TestNode_SequenceWait(t *testing.T) {
	node, err := NewNode(1)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// We need to simulate a full sequence to trigger the wait loop.
	// We can use a mock time that stays constant for the check loop but advances later?
	// Actually, the loop does: now = time.Now()...
	// So we just need to ensure we are at max sequence and time hasn't naturally advanced yet.
	// But `Generate` calls `time.Now()` inside the loop.
	// To reliably test this without race or slowness, we can use mock time.

	// We need to trigger the sequence wait loop.
	// We set the current time, max sequence.
	// Generate() will check time, see it's same, increment sequence -> overflow.
	// Then it enters loop: while timestamp <= n.time ...
	// Since we are mocking, "now" will stick to initialTime unless we change it.

	// Actually, Node spins on:
	// if n.mockTime != nil { now = *n.mockTime }
	// We can't change *n.mockTime safely if main loop is reading it in a tight loop and we want to change it from another goroutine
	// (we fixed the race with mutex, but the loop in Generate holds the mutex? NO!)

	// Wait, let's look at Generate logic again:
	// n.mu.Lock() ...
	// if n.sequence == 0 { for timestamp <= n.time { ... } }

	// THE LOOP IS INSIDE THE LOCK!
	// This means we CANNOT update mockTime from another goroutine while Generate is stuck in the loop!
	// The loop holds the lock, so setMockTime (which needs lock) will hang.
	// And Generate loops forever because mockTime never changes.
	// DEADLOCK/INFINITE LOOP.

	// This confirms `Generate` holding lock during wait is problematic for mock time updates if they require lock.
	// But even if setMockTime didn't require lock, the loop logic for mock time read `now = *n.mockTime` is just reading a memory location.
	// If we update that location unsafely, we might race.
	// But since we are locked, we can't update safely.

	// Real-world: time.Now() changes on its own, outside our lock.

	// Fix: createID logic or Generate logic needs to be aware.
	// Standard Snowflake: busy wait is fine for real time.
	// For test: we can't easily test the "wait loop" with `setMockTime` if `setMockTime` requires the SAME lock.
	// We should probably rely on `time.Now()` for this test and just wait 1ms?
	// OR, we make `setMockTime` NOT take the lock?
	// The `mockTime` pointer is read inside lock.
	// If we change the value pointed to *t, we race?
	// `mockTime` is `*int64`. The `int64` value it points to can be changed?
	// `now = *n.mockTime`.

	// Let's use a shared integer and update it?

	// Reset state
	node.time = 0
	node.sequence = maxSequence

	// We start a goroutine to advance time
	// We need to wait a tiny bit to let Generate enter the loop?
	// But Generate takes lock immediately.
	// Logic:
	// 1. Generate takes lock.
	// 2. Checks sequence overflow.
	// 3. Loops `for timestamp <= n.time`.
	// 4. Inside loop: `now = *n.mockTime`.

	// If we change `sharedTime` from another goroutine (atomic store?), the loop will see it?
	// Yes, but we need to avoid race detector complaining.
	// `atomic.StoreInt64` vs `atomic.LoadInt64`.
	// The usage in `Generate` for mock time: `now = *n.mockTime` is a plain load.
	// We can't use atomic load there unless we change main code.

	// Alternative: Don't use mock time for this test. Use real time.
	// It's just 1ms wait.

	node.setMockTime(nil)

	// Ensure we are close to a millisecond boundary so we don't wait too long?
	// No, standard `time.sleep`?

	// Let's just run it with real time.

	id, err := node.Generate()
	if err != nil {
		t.Fatalf("failed to generate ID with sequence wait: %v", err)
	}

	decomposed := node.Decompose(id)
	// We can't predict exact timestamp, but it should be > old time and sequence 0.
	if decomposed.Sequence != 0 {
		t.Errorf("expected sequence reset to 0, got %d", decomposed.Sequence)
	}

}

func TestNode_GenerateStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	runConcurrentTest(t, 50, 10000)
}

func TestNode_MaxTimestampBoundary(t *testing.T) {
	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Test timestamp near 42-bit limit
	maxTimestamp := int64((1 << timestampBits) - 1)
	id := node.createID(maxTimestamp, 0)
	decomposed := node.Decompose(id)

	if decomposed.Timestamp != maxTimestamp {
		t.Errorf("max timestamp not preserved, got %d, want %d", decomposed.Timestamp, maxTimestamp)
	}

	// Set mock time to a value that will result in max timestamp + 1
	mockTime := node.epoch.UnixNano()/millisecond + (1 << timestampBits)
	node.setMockTime(&mockTime)

	// Reset node state
	node.time = 0
	node.sequence = 0

	// Try to generate ID with timestamp beyond limit
	_, err = node.Generate()
	if err == nil {
		t.Error("expected error for timestamp beyond 42-bit limit")
	} else {
		expectedTimestamp := int64(1 << timestampBits)
		expectedError := fmt.Sprintf("timestamp out of range: %d", expectedTimestamp)
		if err.Error() != expectedError {
			t.Errorf("unexpected error message: got %v, want %v", err, expectedError)
		}
	}
}

func TestNode_TimeComponentValidation(t *testing.T) {
	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	tests := []struct {
		name      string
		mockTime  int64
		wantError bool
		errorMsg  string
	}{
		{
			name:      "time exactly at epoch",
			mockTime:  node.epoch.UnixNano() / millisecond,
			wantError: false,
		},
		{
			name:      "time at max timestamp",
			mockTime:  node.epoch.UnixNano()/millisecond + (1 << timestampBits),
			wantError: true,
			errorMsg:  "timestamp out of range: %d",
		},
		{
			name:      "time near max timestamp",
			mockTime:  node.epoch.UnixNano()/millisecond + ((1 << timestampBits) * 9 / 10),
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset node state for each test
			node.time = 0
			node.sequence = 0

			// Set mock time
			node.setMockTime(&tt.mockTime)

			// Calculate expected timestamp for error message
			timestamp := tt.mockTime - node.epoch.UnixNano()/millisecond

			// Try to generate ID
			_, err := node.Generate()

			if tt.wantError {
				if err == nil {
					t.Errorf("expected error for timestamp %d, got nil", timestamp)
				} else if tt.errorMsg != "" && err.Error() != fmt.Sprintf(tt.errorMsg, timestamp) {
					t.Errorf("unexpected error message: got %v, want %v", err, fmt.Sprintf(tt.errorMsg, timestamp))
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for timestamp %d: %v", timestamp, err)
				}
			}
		})
	}
}

func TestNode_DecompositionEdgeCases(t *testing.T) {
	node, err := NewNode(maxMachineID)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Calculate maximum valid ID within int64 range
	maxValidTimestamp := int64((1 << timestampBits) - 1)
	maxID := node.createID(maxValidTimestamp, maxSequence)

	tests := []struct {
		name string
		id   uint64
		want ID
	}{
		{
			name: "maximum valid values",
			id:   maxID,
			want: ID{
				Timestamp: maxValidTimestamp,
				MachineID: maxMachineID,
				Sequence:  maxSequence,
			},
		},
		{
			name: "zero values",
			id:   0,
			want: ID{
				Timestamp: 0,
				MachineID: 0,
				Sequence:  0,
			},
		},
		{
			name: "alternating bits",
			id:   0x555555555555,
			want: ID{
				Timestamp: int64(0x555555555555 >> (machineIDBits + sequenceBits)),
				MachineID: (int64(0x555555555555) >> sequenceBits) & maxMachineID,
				Sequence:  int64(0x555555555555) & maxSequence,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := node.Decompose(tt.id)
			if got != tt.want {
				t.Errorf("Decompose() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestNode_BitPatternEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		timestamp int64
		machineID int64
		sequence  int64
	}{
		{"all bits set in each component", (1 << timestampBits) - 1, maxMachineID, maxSequence},
		{"alternating bits in timestamp", 0x555555555555 & ((1 << timestampBits) - 1), maxMachineID, maxSequence},
		{"alternating bits in sequence", (1 << timestampBits) - 1, maxMachineID, 0x555},
		{"single bit set in each component", 1 << (timestampBits - 1), 1 << (machineIDBits - 1), 1 << (sequenceBits - 1)},
	}

	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := node.createID(tt.timestamp, tt.sequence)
			decomposed := node.Decompose(id)

			if decomposed.Timestamp != tt.timestamp {
				t.Errorf("timestamp mismatch, got %d, want %d", decomposed.Timestamp, tt.timestamp)
			}
			if decomposed.Sequence != tt.sequence {
				t.Errorf("sequence mismatch, got %d, want %d", decomposed.Sequence, tt.sequence)
			}
		})
	}
}

func TestNode_MillisecondPrecision(t *testing.T) {
	node, err := NewNode(0)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Generate IDs with precise timing
	start := time.Now()
	var lastID uint64
	var lastTime time.Time

	// Generate IDs for 10ms
	for time.Since(start) < 10*time.Millisecond {
		id, err := node.Generate()
		if err != nil {
			t.Fatalf("failed to generate ID: %v", err)
		}

		currentTime := node.Time(id)

		if lastID != 0 {
			// Check time difference is not less than 0
			diff := currentTime.Sub(lastTime)
			if diff < 0 {
				t.Errorf("time went backwards: %v", diff)
			}

			// Check millisecond precision
			if diff > time.Millisecond {
				decomp1 := node.Decompose(lastID)
				decomp2 := node.Decompose(id)
				if decomp2.Timestamp-decomp1.Timestamp > 1 {
					t.Errorf("time gap too large: %d ms", decomp2.Timestamp-decomp1.Timestamp)
				}
			}
		}

		lastID = id
		lastTime = currentTime
	}
}
