// Package file allows reading/writing from/to a Rump file.
// Rump file protocol is key✝✝value✝✝ttl✝✝key✝✝value✝✝ttl✝✝...
package file

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/stickermule/rump/pkg/message"
)

// File can read and write, to a file Path, using the message Bus.
type File struct {
	Path   string
	Bus    message.Bus
	Silent bool
	TTL    bool
}

// splitCross is a double-cross (✝✝) custom Scanner Split.
func splitCross(data []byte, atEOF bool) (advance int, token []byte, err error) {

	// end of file
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	// Split at separator
	if i := strings.Index(string(data), "✝✝"); i >= 0 {
		// Separator is 6 bytes long
		return i + 6, data[0:i], nil
	}

	return 0, nil, nil
}

// New creates the File struct, to be used for reading/writing.
func New(path string, bus message.Bus, silent, ttl bool) *File {
	return &File{
		Path:   path,
		Bus:    bus,
		Silent: silent,
		TTL:    ttl,
	}
}

// Log read/write operations unless silent mode enabled
func (f *File) maybeLog(s string) {
	if f.Silent {
		return
	}
	fmt.Print(s)
}

// Read scans a Rump file and sends Payloads to the message bus.
func (f *File) Read(ctx context.Context) error {
	defer close(f.Bus)

	d, err := os.Open(f.Path)
	if err != nil {
		return err
	}
	defer d.Close()

	// Scan file, split by double-cross separator

	scanner := bufio.NewScanner(d)
	buf := make([]byte, 0, bufio.MaxScanTokenSize)
	scanner.Buffer(buf, 1024*1024)
	scanner.Split(splitCross)

	// Scan line by line
	// file protocol is key✝✝value✝✝ttl✝✝
	for {
		var key, value, ttl string
		// Get key
		if s1 := scanner.Scan(); s1 {
			key = scanner.Text()
		}

		// trigger next scan to get value
		if s2 := scanner.Scan(); s2 {
			value = scanner.Text()
		}

		// trigger next scan to get ttl
		if s3 := scanner.Scan(); s3 {
			ttl = scanner.Text()
		}

		select {
		case <-ctx.Done():
			fmt.Println("")
			fmt.Println("file read: exit")
			return ctx.Err()
		case f.Bus <- []*message.Payload{&message.Payload{Key: key, Value: value, TTL: ttl}}:
			f.maybeLog("r")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println(err)
		return nil
	}

	return nil
}

// Write writes to a Rump file Payloads from the message bus.
func (f *File) Write(ctx context.Context) error {
	d, err := os.Create(f.Path)
	if err != nil {
		return err
	}
	defer d.Close()

	// Buffered write to limit system IO calls
	w := bufio.NewWriter(d)

	// Flush last open buffers
	defer w.Flush()

	for f.Bus != nil {
		select {
		// Exit early if context done.
		case <-ctx.Done():
			fmt.Println("")
			fmt.Println("file write: exit")
			return ctx.Err()
		// Get Messages from Bus
		case batch, ok := <-f.Bus:
			// if channel closed, set to nil, break loop
			if !ok {
				f.Bus = nil
				continue
			}

			for _, p := range batch {
				if len(p.Key) >= bufio.MaxScanTokenSize {
					fmt.Println("ignore by key")
					continue
				}
				if len(p.Value) >= bufio.MaxScanTokenSize {
					fmt.Println("ignore by val")
					continue
				}
				if len(p.TTL) >= bufio.MaxScanTokenSize {
					fmt.Println("ignore by ttl")
					continue
				}
				_, err := w.WriteString(p.Key + "✝✝" + p.Value + "✝✝" + p.TTL + "✝✝")
				if err != nil {
					return err
				}
			}

			f.maybeLog("w")
		}
	}

	return nil
}
