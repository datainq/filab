package local

import (
	"path"

	"github.com/datainq/filab"
)

type LocalPath string

func (l LocalPath) Join(p ...string) filab.Path {
	return LocalPath(path.Join(append([]string{string(l)}, p...)...))
}

func (l LocalPath) String() string {
	return string(l)
}

func (LocalPath) New(s string) filab.Path {
	return LocalPath(s)
}

func (l LocalPath) Copy() filab.Path {
	return l
}

func (LocalPath) Type() filab.DriverType {
	return Type()
}

func ParseLocalPath(s string) (LocalPath, error) {
	return LocalPath(s), nil
}

func (l LocalPath) Dir() filab.Path {
	return LocalPath(l.DirStr())
}

func (l LocalPath) DirStr() string {
	return path.Dir(l.String())
}

func (l LocalPath) BaseStr() string {
	return path.Base(l.String())
}
