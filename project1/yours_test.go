// Your test file. Nothing here is graded. Write your own tests as you
// go. Delete or change the examples below.
//
// Useful commands:
//
//	go test .                    // run every test in this folder
//	go test -run TestSeq .       // run only tests whose name has TestSeq
//	go test -v .                 // print each test name as it runs
//	go test -race .              // catch data races (needed for Parts 2 and 3)
//
// A good habit: when you find a bug, add a test that would have
// caught it. Then you never chase the same bug twice.
package main

import "testing"

// A smoke test. When RunSeq is implemented, this passes: the returned
// slice has the right length. It does not check that the model
// actually trains.
func TestExampleSeq(t *testing.T) {
	cfg := SeqConfig{Seed: 1, Steps: 5, BatchSize: 32, LR: 0.01}
	w := RunSeq(cfg)
	if len(w) != D {
		t.Fatalf("RunSeq returned %d weights, want %d", len(w), D)
	}
}

func TestRunSeqOnStepAndWeights(t *testing.T) {
	stepsCalled := 0
	initialW := Init(1)

	var lastCopy []float64
	cfg := SeqConfig{
		Seed:      1,
		Steps:     10,
		BatchSize: 32,
		LR:        0.01,
		OnStep: func(step int, w []float64) {
			if step != stepsCalled {
				t.Errorf("OnStep step = %d, expected %d", step, stepsCalled)
			}
			stepsCalled++
			if len(w) != D {
				t.Errorf("OnStep w length = %d, expected %d", len(w), D)
			}
			lastCopy = w
		},
	}
	finalW := RunSeq(cfg)

	if stepsCalled != 10 {
		t.Fatalf("OnStep called %d times, expected 10", stepsCalled)
	}

	// Verify that OnStep received a fresh copy and that updating returned weights doesn't mutate lastCopy
	for i := 0; i < D; i++ {
		if finalW[i] == initialW[i] {
			t.Errorf("Weight w[%d] was not updated", i)
		}
		if finalW[i] != lastCopy[i] {
			t.Errorf("finalW[%d] != lastCopy[%d]", i, i)
		}
	}
}

// Template for Part 2. Uncomment when RunLocal works.

func TestExampleLocal(t *testing.T) {
	cfg := LocalConfig{Seed: 1, Steps: 5, BatchSize: 32, LR: 0.01, NRanks: 4}
	w := RunLocal(cfg)
	if len(w) != D {
		t.Fatalf("RunLocal returned %d weights, want %d", len(w), D)
	}
}

// Template for Part 3. Uncomment when RunWorker works. Starts one
// worker and shuts it down.
//
// func TestExampleWorker(t *testing.T) {
// 	addr, stop, err := RunWorker("localhost:0")
// 	if err != nil {
// 		t.Fatalf("RunWorker: %v", err)
// 	}
// 	defer stop()
// 	if addr == "" {
// 		t.Fatal("RunWorker returned an empty address")
// 	}
// }
