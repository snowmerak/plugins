//go:build !windows

package ringbuf

import (
	"fmt"
	"os"
	"syscall"
)

func mmapFile(filePath string, totalSize int) ([]byte, interface{}, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	if err := syscall.Ftruncate(int(file.Fd()), int64(totalSize)); err != nil {
		return nil, nil, fmt.Errorf("failed to truncate file: %w", err)
	}

	data, err := syscall.Mmap(
		int(file.Fd()),
		0,
		totalSize,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to mmap file: %w", err)
	}

	return data, nil, nil
}

func unmapFile(data []byte, extra interface{}) error {
	if data != nil {
		if err := syscall.Munmap(data); err != nil {
			return fmt.Errorf("failed to munmap: %w", err)
		}
	}
	return nil
}
