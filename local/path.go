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

func (l LocalPath) Copy() filab.Path {
	return l
}

func ParseLocalPath(s string) (LocalPath, error) {
	return LocalPath(s), nil
}