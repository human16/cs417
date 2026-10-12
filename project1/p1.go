// p1.go -- the file you edit for Project 1.
//
// The file has three parts, one for each part of the project.
// Read project1-desc.pdf for what each part must do.
//
// Rules:
//   - Do not change the names of the functions in this file.
//   - Do not change the arguments they take or what they return.
//   - Do not change the fields inside SeqConfig, LocalConfig, or
//     DistConfig.
//   - You may add your own helper functions and types anywhere.
//   - Do not modify sealed.go or yours_test.go.
//
// After you finish a part, you can run it by hand:
//   go run . -mode seq
//   go run . -mode local -n 4
//   go run . -mode worker -addr localhost:9001
//   go run . -mode coord  -workers localhost:9001,localhost:9002

package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	// You will need more packages as you go. Add them here when you need
	// them. Likely: "encoding/gob", "errors", "net", "net/rpc", "sync",
	// "sync/atomic".
)

/* Part 1 -- Sequential SGD (20 points). See project1-desc.pdf, Part 1. */

// SeqConfig holds the inputs to RunSeq. Do not add or remove fields.
type SeqConfig struct {
	Seed      int64
	Steps     int
	BatchSize int
	LR        float64

	// OnStep may be set by the test code. If it is not nil, call it
	// after every step with a fresh copy of the weights.
	OnStep func(step int, w []float64)
}

// RunSeq trains for cfg.Steps steps and returns the final weights.
func RunSeq(cfg SeqConfig) []float64 {
	// Write your code here.
	// See project1-desc.pdf, Part 1 for what this function must do.
	w := Init(cfg.Seed)
	b := NewBatcher(cfg.Seed, cfg.BatchSize)
	for step := 0; step < cfg.Steps; step++ {
		batch := b.Next(step)
		g := Grad(w, batch)
		for i := 0; i < len(w); i++ {
			w[i] -= cfg.LR * g[i] / float64(cfg.BatchSize)
		}
		if cfg.OnStep != nil {
			copyW := make([]float64, len(w))
			copy(copyW, w)
			cfg.OnStep(step, copyW)
		}

	}
	return w
	// panic("not implemented yet: Part 1")
}

/* Part 2 -- Parallel local trainer (15 points). See project1-desc.pdf, Part 2. */

// LocalConfig holds the inputs to RunLocal. NRanks must divide
// BatchSize. Do not add or remove fields.
type LocalConfig struct {
	Seed      int64
	Steps     int
	BatchSize int
	LR        float64
	NRanks    int
}

func GradComp(rank int, cfg LocalConfig, from []chan []float64, to []chan []float64, finalW chan<- []float64, wg *sync.WaitGroup) {
	defer wg.Done()

	w := Init(cfg.Seed)
	b := NewBatcher(cfg.Seed, cfg.BatchSize)
	for step := 0; step < cfg.Steps; step++ {
		batch := b.Next(step)
		sliceSize := cfg.BatchSize / cfg.NRanks
		sliceBatch := batch[rank*sliceSize : (rank+1)*sliceSize]
		g := Grad(w, sliceBatch)

		var totalGrad []float64

		if rank == 0 {
			totalGrad = make([]float64, len(w))

			for i := range totalGrad {
				totalGrad[i] += g[i]
			}

			for minion := 1; minion < cfg.NRanks; minion++ {
				currG := <-to[minion]
				for i := range totalGrad {
					totalGrad[i] += currG[i]
				}
			}

			for minion := 1; minion < cfg.NRanks; minion++ {
				from[minion] <- totalGrad
			}
		} else {
			to[rank] <- g

			totalGrad = <-from[rank]
		}

		for i := 0; i < len(w); i++ {
			w[i] -= cfg.LR * totalGrad[i] / float64(cfg.BatchSize)
		}
	}

	if rank == 0 {
		finalW <- w
	}
}

// RunLocal trains using NRanks goroutines and returns the final weights.
func RunLocal(cfg LocalConfig) []float64 {
	// Write your code here.
	// See project1-desc.pdf, Part 2. Sum partial gradients in rank order so
	// two runs with the same input return the same result, bit for bit.
	var wg sync.WaitGroup

	toMinions := make([]chan []float64, cfg.NRanks)
	FromMinons := make([]chan []float64, cfg.NRanks)

	for i := 0; i < cfg.NRanks; i++ {
		toMinions[i] = make(chan []float64)
		FromMinons[i] = make(chan []float64)
	}

	finalW := make(chan []float64, 1)

	for minion := 0; minion < cfg.NRanks; minion++ {
		wg.Add(1)
		go GradComp(minion, cfg, FromMinons, toMinions, finalW, &wg)
	}

	wg.Wait()
	return <-finalW

	// for step := 0; step < cfg.Steps-1; step++ {
	// 	batch := b.Next(step)
	// 	g := Grad(w, batch)
	// 	for i := 0; i < len(w); i++ {
	// 		w[i] -= cfg.LR * g[i] / float64(cfg.BatchSize)
	// 	}
	// }
	// return w

	// panic("not implemented yet: Part 2")
}

/* Part 3 -- Coordinator and workers over RPC (65 points). */

// Worker is the RPC service. You register it under the name "Worker"
// so clients call "Worker.Grad" and "Worker.Ping".
type Worker struct{}

// Grad returns the summed gradient for one slice of a batch.
func (Worker) Grad(args GradArgs, reply *GradReply) error {
	if !(len(args.W) == D && len(args.Batch) > 0) {
		return errors.New("erorr")
	}
	reply.Grad = Grad(args.W, args.Batch)
	return nil
	// Write your code here.
	// See project1-desc.pdf, Part 3.1.
	// panic("not implemented yet: Part 3.1 (Worker.Grad)")
}

// Ping is a health check. Leave it as it is.
func (Worker) Ping(_ PingArgs, reply *PingReply) error {
	reply.OK = true
	return nil
}

// RunWorker starts a worker on addr and returns its actual address
// plus a stop function that shuts it down.
func RunWorker(addr string) (actualAddr string, stop func(), err error) {
	// Write your code here.
	// See project1-desc.pdf, Part 3.1.
	// panic("not implemented yet: Part 3.1 (RunWorker)")

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return "", nil, err
	}
	server := rpc.NewServer()
	server.Register(new(Worker))
	go server.Accept(listener)

	actualAddr = listener.Addr().String()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			listener.Close()
		})
	}

	return actualAddr, stop, nil
}

// DistConfig holds the inputs to RunCoordinator. Do not add or remove
// fields.
type DistConfig struct {
	Seed      int64
	Steps     int
	BatchSize int
	LR        float64
	NSlices   int
	Workers   []string
	Timeout   time.Duration
	OnStep    func(step int, w []float64)
}

type Minion struct {
	addr    string
	client  *rpc.Client
	alive   bool
	working bool
}

type attemptResult struct {
	step, slice, worker int
	grad                []float64
	err                 error
}

type sliceState struct {
	done      bool
	onIt      int
	tried     map[int]bool
	timesdOut bool
}

type timeOut struct {
	step, sliceIndex, minion int
}

func findMinion(pool []*Minion, tried map[int]bool) (int, *Minion) {
	for i, worker := range pool {
		if worker.alive && !worker.working && !tried[i] {
			return i, worker
		}
	}
	return -1, nil
}

// RunCoordinator drives training end to end and returns the final
// weights.
func RunCoordinator(cfg DistConfig) []float64 {
	// Write your code here.
	// See project1-desc.pdf, Part 3.2 and Part 3.3.
	// You are free to add helper types and helper functions.
	// panic("not implemented yet: Part 3 (RunCoordinator)")
	if cfg.BatchSize%cfg.NSlices != 0 {
		panic("batch size not divisible by #slices")
	}

	pool := make([]*Minion, len(cfg.Workers))

	// initialize workers
	for i := 0; i < len(cfg.Workers); i++ {
		conn, err := net.DialTimeout("tcp", cfg.Workers[i], cfg.Timeout)
		if err == nil {
			pool[i] = &Minion{cfg.Workers[i], rpc.NewClient(conn), true, false}
		} else {
			// if initialization failed, just put an empty worker with that
			// address in the expected lcoation
			pool[i] = &Minion{addr: cfg.Workers[i], alive: false}
		}
	}

	defer func() {
		for _, worker := range pool {
			if worker.client != nil {
				worker.client.Close()
			}
		}
	}()

	w := Init(cfg.Seed)
	b := NewBatcher(cfg.Seed, cfg.BatchSize)
	sliceSize := cfg.BatchSize / cfg.NSlices

	results := make(chan attemptResult, cfg.NSlices*len(cfg.Workers))
	timeout := make(chan timeOut, cfg.NSlices*len(cfg.Workers))

	for step := 0; step < cfg.Steps; step++ {
		batch := b.Next(step)
		// sent := make([]bool, cfg.NSlices)
		gradients := make([][]float64, cfg.NSlices)
		slicesCompleted := 0

		sliceStates := make([]sliceState, cfg.NSlices)
		for slice := 0; slice < cfg.NSlices; slice++ {
			sliceStates[slice] = sliceState{
				tried: make(map[int]bool),
			}
		}

		for slicesCompleted < cfg.NSlices {
			for sliceIndex := 0; sliceIndex < cfg.NSlices; sliceIndex++ {
				if sliceStates[sliceIndex].done {
					continue
				}
				if sliceStates[sliceIndex].onIt == 0 || sliceStates[sliceIndex].timesdOut {
					i, worker := findMinion(pool, sliceStates[sliceIndex].tried)
					if i == -1 {
						continue
					} else {
						worker.working = true
						sliceStates[sliceIndex].tried[i] = true
						sliceStates[sliceIndex].onIt++
						sliceStates[sliceIndex].timesdOut = false

						slice := batch[sliceIndex*sliceSize : (sliceIndex+1)*sliceSize]
						args := GradArgs{step, sliceIndex, w, slice}

						currWorker := i
						currSlice := sliceIndex
						go func(workerIndex int, sliceIndex int, args GradArgs) {
							var reply GradReply
							err := pool[workerIndex].client.Call("Worker.Grad", args, &reply)
							results <- attemptResult{step, sliceIndex, workerIndex, reply.Grad, err}
						}(currWorker, currSlice, args)

						time.AfterFunc(cfg.Timeout, func() {
							timeout <- timeOut{step, currSlice, currWorker}
						})
					}
				}
			}

			select {
			case r := <-results:
				pool[r.worker].working = false

				if r.step != step {
					continue
				}

				sliceStates[r.slice].onIt--

				if r.err == nil {
					if !sliceStates[r.slice].done {
						sliceStates[r.slice].done = true
						gradients[r.slice] = r.grad
						slicesCompleted++
					}
				} else {
					pool[r.worker].alive = false
					if pool[r.worker].client != nil {
						pool[r.worker].client.Close()
						pool[r.worker].client = nil
					}
				}
			case oopsie := <-timeout:
				if oopsie.step != step || sliceStates[oopsie.sliceIndex].done {
					continue
				}
				sliceStates[oopsie.sliceIndex].timesdOut = true
			}
			// r := <-results

		}

		summedGrads := make([]float64, len(w))
		for slice := 0; slice < cfg.NSlices; slice++ {
			for i := 0; i < len(w); i++ {
				summedGrads[i] += gradients[slice][i]
			}
		}

		for i := 0; i < len(w); i++ {
			w[i] -= cfg.LR * summedGrads[i] / float64(cfg.BatchSize)
		}

		if cfg.OnStep != nil {
			copyW := make([]float64, len(w))
			copy(copyW, w)
			cfg.OnStep(step, copyW)
		}
	}

	return w
	// var sliceSize int = cfg.BatchSize / cfg.NSlices
	// for i := 0; i < cfg.NSlices; i++ {
	// 	var curr *Batcher
	// 	curr.batchSize = sliceSize
	// 	curr.perm = b.perm
	// 	curr.pool = b.pool[i*sliceSize:(i+1)*sliceSize]
	// 	batches = append(batches, curr)
	// }

}

/* main -- one binary, four modes. Leave this as it is. */

func main() {
	mode := flag.String("mode", "", "seq | local | worker | coord")
	seed := flag.Int64("seed", 2026, "seed")
	steps := flag.Int("steps", 300, "training steps")
	bs := flag.Int("bs", 64, "batch size")
	lr := flag.Float64("lr", 0.02, "learning rate")
	n := flag.Int("n", 4, "number of ranks (local) or slices (coord)")
	addr := flag.String("addr", "localhost:0", "worker listen address")
	workers := flag.String("workers", "", "comma-separated host:port list (coord)")
	timeout := flag.Duration("timeout", 500*time.Millisecond, "per-attempt timeout")
	flag.Parse()

	switch *mode {
	case "seq":
		eval := NewBatcher(*seed, 1024).Next(0)
		w := RunSeq(SeqConfig{Seed: *seed, Steps: *steps, BatchSize: *bs, LR: *lr})
		fmt.Printf("final loss = %.6f\n", Loss(w, eval))
	case "local":
		eval := NewBatcher(*seed, 1024).Next(0)
		w := RunLocal(LocalConfig{Seed: *seed, Steps: *steps,
			BatchSize: *bs, LR: *lr, NRanks: *n})
		fmt.Printf("final loss = %.6f  (%d ranks)\n", Loss(w, eval), *n)
	case "worker":
		actual, stop, err := RunWorker(*addr)
		if err != nil {
			log.Fatalf("worker start: %v", err)
		}
		fmt.Printf("worker listening on %s\n", actual)
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		stop()
	case "coord":
		if *workers == "" {
			log.Fatal("coord: provide -workers")
		}
		addrs := strings.Split(*workers, ",")
		eval := NewBatcher(*seed, 1024).Next(0)
		w := RunCoordinator(DistConfig{
			Seed: *seed, Steps: *steps, BatchSize: *bs, LR: *lr,
			NSlices: *n, Workers: addrs, Timeout: *timeout,
		})
		fmt.Printf("final loss = %.6f\n", Loss(w, eval))
	default:
		fmt.Fprintln(os.Stderr, "usage: p1 -mode {seq|local|worker|coord} [flags]")
		flag.PrintDefaults()
		os.Exit(2)
	}
}
