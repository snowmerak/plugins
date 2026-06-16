package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/snowmerak/plugins/ringbuf"
)

func main() {
	roleFlag := flag.String("role", "Host", "Role: Host or Plugin")
	pathFlag := flag.String("path", "shm_comm_cross", "Base path for shm/sockets")
	countFlag := flag.Int("count", 1, "Number of iterations (benchmark if > 1)")
	sizeFlag := flag.Int("size", 1024, "Message size for benchmark")
	flag.Parse()

	role := *roleFlag
	basePath := *pathFlag
	count := *countFlag
	size := *sizeFlag

	numSlots := 4
	slotSize := 1024
	if count > 1 {
		numSlots = 8
		slotSize = 4096
		fmt.Printf("Starting Go Ringbuffer Demo as %s (Benchmark: count=%d, size=%d)...\n", role, count, size)
	} else {
		fmt.Printf("Starting Go Ringbuffer Demo as %s...\n", role)
	}

	writer, reader, err := ringbuf.NewConnection(
		basePath,
		role,
		numSlots,
		slotSize,
		15*time.Second,
		15*time.Second,
	)
	if err != nil {
		log.Fatalf("failed to create connection: %v", err)
	}
	defer writer.Close()
	defer reader.Close()

	if count > 1 {
		payload := make([]byte, size)
		readBuf := make([]byte, size)

		start := time.Now()
		if role == ringbuf.RoleHost {
			for i := 0; i < count; i++ {
				if _, err := writer.Write(payload); err != nil {
					log.Fatalf("host write failed at iter %d: %v", i, err)
				}
				if _, err := reader.ReadFull(readBuf); err != nil {
					log.Fatalf("host read failed at iter %d: %v", i, err)
				}
			}
			elapsed := time.Since(start)
			ops := float64(count) / elapsed.Seconds()
			latency := float64(elapsed.Nanoseconds()) / float64(count) / 1000.0 // in microseconds
			fmt.Printf("BENCHMARK_RESULT: %d rounds, total time: %v, %.2f ops/sec, avg latency: %.2f us\n", count, elapsed, ops, latency)
		} else {
			for i := 0; i < count; i++ {
				if _, err := reader.ReadFull(readBuf); err != nil {
					log.Fatalf("plugin read failed at iter %d: %v", i, err)
				}
				if _, err := writer.Write(payload); err != nil {
					log.Fatalf("plugin write failed at iter %d: %v", i, err)
				}
			}
		}
	} else {
		if role == ringbuf.RoleHost {
			msg := []byte("hello from go host")
			fmt.Printf("Host writing: '%s'\n", string(msg))
			if _, err := writer.Write(msg); err != nil {
				log.Fatalf("failed to write: %v", err)
			}

			buf := make([]byte, 100)
			n, err := reader.Read(buf)
			if err != nil {
				log.Fatalf("failed to read: %v", err)
			}
			fmt.Printf("Host read: '%s'\n", string(buf[:n]))
		} else {
			buf := make([]byte, 100)
			n, err := reader.Read(buf)
			if err != nil {
				log.Fatalf("failed to read: %v", err)
			}
			fmt.Printf("Plugin read: '%s'\n", string(buf[:n]))

			msg := []byte("hello from go plugin")
			fmt.Printf("Plugin writing: '%s'\n", string(msg))
			if _, err := writer.Write(msg); err != nil {
				log.Fatalf("failed to write: %v", err)
			}
		}
	}

	fmt.Println("Go Demo finished successfully.")
}
