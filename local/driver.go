package local

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/datainq/filab"
)

var localDisk = "local disk"

type driver struct {
	mode os.FileMode
}

func (driver) Name() string {
	return "local driver"
}

func (driver) Scheme() string {
	return ""
}

func (driver) Type() filab.DriverType {
	return filab.DriverType(&localDisk)
}

func (driver) Parse(s string) (filab.Path, error) {
	return ParseLocalPath(s)
}

func (driver) Exist(_ context.Context, p filab.Path) (bool, error) {
	_, err := os.Stat(p.String())
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (driver) NewReader(_ context.Context, p filab.Path) (io.ReadCloser, error) {
	return os.Open(p.String())
}

func (d driver) NewWriter(_ context.Context, p filab.Path) (io.WriteCloser, error) {
	return os.OpenFile(p.String(), os.O_CREATE|os.O_WRONLY, d.mode)
}

func (driver) List(_ context.Context, p filab.Path) ([]filab.Path, error) {
	var s []filab.Path
	l, err := ioutil.ReadDir(p.String())
	if err != nil {
		return nil, err
	}
	for _, v := range l {
		s = append(s, p.Join(v.Name()))
	}
	return s, nil
}

func (driver) Walk(context.Context, filab.Path, filepath.WalkFunc) {
	panic("implement me")
}

func New() driver {
	return driver{
		0640,
	}
}
