package gcs

import (
	"fmt"
	"strings"
	"path"
	"net/url"
	"errors"
	"github.com/datainq/filab"
)

type GCSPath struct {
	Bucket string
	Path   string
}

func (g GCSPath) String() string {
	return fmt.Sprintf("gs://%s/%s", g.Bucket, g.Path)
}

func (g GCSPath) Copy() filab.Path {
	return g
}

func (g GCSPath) WithBucket(b string) GCSPath {
	g.Bucket = b
	return g
}

func (g GCSPath) WithPath(p string) GCSPath {
	if strings.HasPrefix(p, "/") {
		p = p[1:]
	}
	g.Path = p
	return g
}

func (g GCSPath) Join(p ...string) filab.Path {
	return g.WithPath(path.Join(append([]string{g.Path}, p...)...))
}

func ParseGcsPath(s string) (GCSPath, error) {
	var ret GCSPath
	u, err := url.Parse(s)
	if err != nil {
		return ret, err
	}
	if u.Scheme != "gs" {
		return ret, errors.New("wrong scheme, want: gs")
	}
	if u.RawQuery != "" {
		return ret, errors.New("query must be empty")
	}
	if u.Host == "" {
		return ret, errors.New("empty host")
	}
	p := strings.TrimLeft(u.Path, "/")
	//if p == "" {
	//	return nil, errors.New("empty path")
	//}

	return GCSPath{u.Host, p}, nil
}

func MustParseGcs(s string) GCSPath {
	gs, err := ParseGcsPath(s)
	if err != nil {
		panic("cannot parse gcs path")
	}
	return gs
}
