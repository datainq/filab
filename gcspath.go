package filab

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
)

type GCSPath struct {
	Bucket string
	Path   string
}

func (g *GCSPath) String() string {
	return fmt.Sprintf("gs://%s/%s", g.Bucket, g.Path)
}

func (g *GCSPath) Copy() *GCSPath {
	var c GCSPath = *g
	return &c
}

func (g *GCSPath) WithBucket(b string) *GCSPath {
	var c GCSPath = *g
	c.Bucket = b
	return &c
}

func (g *GCSPath) WithPath(p string) *GCSPath {
	var c GCSPath = *g
	c.Path = p
	return &c
}

func (g *GCSPath) Join(p ...string) *GCSPath {
	p = append([]string{g.Path}, p...)
	return g.WithPath(path.Join(p...))
}

func ParseGcsPath(s string) (*GCSPath, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "gs" {
		return nil, errors.New("wrong scheme, want: gs")
	}
	if u.RawQuery != "" {
		return nil, errors.New("query must be empty")
	}
	if u.Host == "" {
		return nil, errors.New("empty host")
	}
	p := strings.TrimLeft(u.Path, "/")
	//if p == "" {
	//	return nil, errors.New("empty path")
	//}

	return &GCSPath{u.Host, p}, nil
}

func MustParseGcs(s string) *GCSPath {
	gs, err := ParseGcsPath(s)
	if err != nil {
		panic("cannot parse gcs path")
	}
	return gs
}
