package local

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"github.com/datainq/filab"
	"path/filepath"
)

const (
	DefaultDirPerm  = 0740
	DefaultFilePerm = 0540
)

var localDisk = "local disk"

type Option interface {
	apply(*driver)
}

type newDir bool

func (newDir) apply(d *driver) {
	d.createNewDirs = true
}

// WithNewDir makes a driver to create new directories if they do not exist.
func WithNewDir() Option {
	return newDir(true)
}

type DirMode os.FileMode

func (m DirMode) apply(d *driver) {
	d.dirMode = os.FileMode(m)
}

func WithDirMode(mode os.FileMode) Option {
	return DirMode(mode)
}

type FileMode os.FileMode

func (m FileMode) apply(d *driver) {
	d.fileMode = os.FileMode(m)
}

func WithFileMode(mode os.FileMode) Option {
	return FileMode(mode)
}

type driver struct {
	fileMode      os.FileMode
	dirMode       os.FileMode
	createNewDirs bool
}

func (driver) Name() string {
	return "local driver"
}

func (driver) Scheme() string {
	return ""
}

func Type() filab.DriverType {
	return filab.DriverType(&localDisk)
}

func (driver) Type() filab.DriverType {
	return Type()
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

func (driver) Delete(_ context.Context, p filab.Path) error {
	return os.Remove(p.String())
}

func (driver) NewReader(_ context.Context, p filab.Path) (io.ReadCloser, error) {
	return os.Open(p.String())
}

func (d driver) NewWriter(_ context.Context, p filab.Path) (io.WriteCloser, error) {
	if d.createNewDirs {
		os.MkdirAll(path.Dir(p.String()), d.dirMode)
	}
	return os.OpenFile(p.String(), os.O_CREATE|os.O_WRONLY, d.fileMode)
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

func (driver) Walk(_ context.Context, p filab.Path, f filab.WalkFunc) error {
	return filepath.Walk(p.String(), func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		return f(LocalPath(path), err)
	})
}

func New(opts ...Option) *driver {
	d := &driver{
		fileMode: DefaultFilePerm,
		dirMode:  DefaultDirPerm,
	}
	for _, o := range opts {
		o.apply(d)
	}
	return d
}
