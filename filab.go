package filab

import (
	"context"
	"io"
	"path/filepath"
	"regexp"
	"time"

	"github.com/orian/pbio"
)

type DriverType *string

type StorageDriver interface {
	Name() string
	Scheme() string
	Type() DriverType
	Parse(s string) (Path, error)

	Exist(context.Context, Path) (bool, error)
	NewReader(context.Context, Path) (io.ReadCloser, error)
	NewWriter(context.Context, Path) (io.WriteCloser, error)

	List(context.Context, Path) ([]Path, error)
	Walk(context.Context, Path, filepath.WalkFunc)
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

var (
	register map[string]StorageDriver
	local    StorageDriver
)

func RegisterDriver(driver StorageDriver) {
	scheme := driver.Scheme()
	if scheme != "" {
		register[scheme] = driver
	} else {
		local = driver
	}
}
