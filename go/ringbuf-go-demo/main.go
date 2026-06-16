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
	flag.Parse()

	role := *roleFlag
	basePath := *pathFlag

	fmt.Printf("Starting Go Ringbuffer Demo as %s...\n", role)

	writer, reader, err := ringbuf.NewConnection(
		basePath,
		role,
		4,
		1024,
		5*time.Second,
		5*time.Second,
	)
	if err != nil {
		log.Fatalf("failed to create connection: %v", err)
	}
	defer writer.Close()
	defer reader.Close()

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

	fmt.Println("Go Demo finished successfully.")
}
