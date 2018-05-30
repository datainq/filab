package filab

import (
	"strings"

	"github.com/sirupsen/logrus"
)

type Path interface {
	Join(p ...string) Path
	String() string
	Copy() Path
}

type ParseFunc func(string) (Path, error)

func Parse(s string) (Path, error) {
	for k, v := range register {
		if strings.HasPrefix(s, k) {
			g, err := v.Parse(s)
			if err != nil {
				return nil, err
			}
			return g, nil
		}
	}
	return local.Parse(s)
}

func MustParse(s string) Path {
	p, err := Parse(s)
	if err != nil {
		logrus.Panicf("cannot parse %q: %s", s, err)
	}
	return p
}
