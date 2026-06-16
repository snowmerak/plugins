//go:build windows

package ringbuf

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

type sysMapping struct {
	hMap syscall.Handle
	addr uintptr
}

func mmapFile(filePath string, totalSize int) ([]byte, interface{}, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	if err := file.Truncate(int64(totalSize)); err != nil {
		return nil, nil, fmt.Errorf("failed to truncate file: %w", err)
	}

	hMap, err := syscall.CreateFileMapping(
		syscall.Handle(file.Fd()),
		nil,
		syscall.PAGE_READWRITE,
		0,
		uint32(totalSize),
		nil,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to CreateFileMapping: %w", err)
	}

	addr, err := syscall.MapViewOfFile(
		hMap,
		syscall.FILE_MAP_WRITE,
		0,
		0,
		uintptr(totalSize),
	)
	if err != nil {
		syscall.CloseHandle(hMap)
		return nil, nil, fmt.Errorf("failed to MapViewOfFile: %w", err)
	}

	data := unsafe.Slice((*byte)(unsafe.Pointer(addr)), totalSize)
	return data, &sysMapping{hMap: hMap, addr: addr}, nil
}

func unmapFile(data []byte, extra interface{}) error {
	mapping, ok := extra.(*sysMapping)
	if !ok {
		return fmt.Errorf("invalid mapping type")
	}
	var errs []error
	if err := syscall.UnmapViewOfFile(mapping.addr); err != nil {
		errs = append(errs, err)
	}
	if err := syscall.CloseHandle(mapping.hMap); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
