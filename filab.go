package filab

import (
	"io"
	"regexp"
	"time"

	"context"

	"path/filepath"

	"github.com/orian/pbio"
	"github.com/sirupsen/logrus"
)

type DriverType *string

type StorageDriver interface {
	Type() DriverType
	Name() string
	Exist(Path) bool

	NewReader(Path) (io.ReadCloser, error)
	NewReaderContext(context.Context, Path) (io.ReadCloser, error)

	NewWriter(Path) (io.ReadCloser, error)
	NewWriterContext(context.Context, Path) (io.ReadCloser, error)

	ListAll(Path) ([]Path, string)
	Walk(Path, walkFunc filepath.WalkFunc)
}

type FileStore interface {
	// FindLastForDate searches in a give path matching regexp.
	// The path format is: <base>/YYYY/MM/dd/<pattern> with a given time t formatted
	// into it.
	// Returns a path or error.
	FindLastForDate(basePath string, pattern *regexp.Regexp, t time.Time) (string, error)

	// FindAnyForDateSharded is analogous to FindLastForDate except that it searches for
	// a sharded pattern.
	FindAnyForDateSharded(basePath string, pattern *regexp.Regexp, t time.Time) ([]string, error)

	// ReaderForLast searches for a last file matching given pattern.
	ReaderForLast(dirPath string, pattern *regexp.Regexp) io.ReadCloser

	NewReader(p string) (io.ReadCloser, error)
	NewPbReader(p string) (pbio.ReadCloser, error)

	NewWriter(p string) (io.WriteCloser, error)
	NewPbWriter(p string) (pbio.WriteCloser, error)
}

func AggregateProtoFiles(files []string, dest io.WriteCloser) error {
	for _, f := range files {
		r, err := NewFileReader(f)
		if err != nil {
			if err != io.ErrUnexpectedEOF && err != io.EOF {
				return err
			}
			continue
		}

		r1 := pbio.NewDelimitedCopier(r, DefaultProtoMaxSize) // 1MB
		n := 0
		for err = nil; err == nil; err = r1.CopyMsg(dest) {
			n++
		}
		switch err {
		case io.ErrUnexpectedEOF:
			logrus.Errorf("corrupted file: %s", f)
			fallthrough
		case io.EOF:
			err = nil
			fallthrough
		case nil:
			r = r1
		default:
			// writing may be corrupted
			r1.Close()
			return err
		}
		logrus.Debugf("file: %s DONE, read msg: %d", f, n)
		if err = r.Close(); err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			logrus.Errorf("fail on a file: %s", f)
			return err
		}
	}
	return nil
}
