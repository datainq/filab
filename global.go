package filab

import (
	"context"
	"io"
	"path/filepath"

	"github.com/sirupsen/logrus"
)

func RegisterDriver(driver StorageDriver) error {
	return defaultStore.RegisterDriver(driver)
}

func Parse(s string) (Path, error) {
	return defaultStore.Parse(s)
}

func MustParse(s string) Path {
	p, err := Parse(s)
	if err != nil {
		logrus.Panicf("cannot parse %q: %s", s, err)
	}
	return p
}

func Exist(ctx context.Context, p Path) (bool, error) {
	return defaultStore.Exist(ctx, p)
}

func NewReader(ctx context.Context, p Path) (io.ReadCloser, error) {
	return defaultStore.NewReader(ctx, p)
}

func NewWriter(ctx context.Context, p Path) (io.WriteCloser, error) {
	return defaultStore.NewWriter(ctx, p)
}

func List(ctx context.Context, p Path) ([]Path, error) {
	return defaultStore.List(ctx, p)
}

func Walk(ctx context.Context, p Path, w filepath.WalkFunc) {
	defaultStore.Walk(ctx, p, w)
}
