package filab

import (
	"compress/gzip"
	"compress/zlib"
	"context"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/orian/pbio"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

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

type FileHelper struct {
	timeout time.Duration
	baseCtx context.Context

	keyFile       string
	client        *storage.Client
	m             *sync.RWMutex
	createNewDirs bool
}

func WithKeyFile(f string) Option {
	return &withKeyFile{f}
}

type withKeyFile struct {
	f string
}

func (w *withKeyFile) Apply(f *FileHelper) {
	f.keyFile = w.f
}

type newDir bool

func (newDir) Apply(f *FileHelper) {
	f.createNewDirs = true
}

func WithNewDir() Option {
	return newDir(true)
}

func WithClient(c *storage.Client) Option {
	return withClient{c}
}

type withClient struct {
	c *storage.Client
}

func (w withClient) Apply(f *FileHelper) {
	f.client = w.c
}

type withTimeout struct {
	timeout time.Duration
}

func (w withTimeout) Apply(f *FileHelper) {
	f.timeout = w.timeout
}

func WithTimeout(t time.Duration) Option {
	return withTimeout{t}
}

type Option interface {
	Apply(*FileHelper)
}

func NewFileHelper(opts ...Option) *FileHelper {
	r := &FileHelper{
		m:       &sync.RWMutex{},
		baseCtx: context.Background(),
	}
	for _, o := range opts {
		o.Apply(r)
	}
	return r
}

func (f *FileHelper) getClient() (*storage.Client, error) {
	f.m.RLock()
	if f.client != nil {
		f.m.RUnlock()
		return f.client, nil
	}
	f.m.RUnlock()
	f.m.Lock()
	if f.client != nil {
		f.m.Unlock()
		return f.client, nil
	}
	opts := []option.ClientOption{option.WithGRPCDialOption(grpc.WithBlock())}
	if len(f.keyFile) > 0 {
		opts = append(opts, option.WithCredentialsFile(f.keyFile))
	}
	var err error
	ctx, canc := f.createContext()
	defer canc()
	f.client, err = storage.NewClient(ctx, opts...)
	f.m.Unlock()
	return f.client, err
}

func (f *FileHelper) createContext() (context.Context, context.CancelFunc) {
	if f.timeout > 0 {
		return context.WithTimeout(f.baseCtx, f.timeout)
	}
	return f.baseCtx, func() {}
}

func (f *FileHelper) NewReader(p string) (io.ReadCloser, error) {
	var err error
	var r io.ReadCloser
	if strings.HasPrefix(p, "gs://") {
		gs, err2 := ParseGcsPath(p)
		if err2 != nil {
			return nil, err2
		}
		c, err2 := f.getClient()
		if err2 != nil {
			return nil, err2
		}
		ctx, _ := f.createContext()
		r, err = c.Bucket(gs.Bucket).Object(gs.Path).NewReader(ctx)
	} else {
		r, err = os.Open(p)
	}
	if err != nil {
		return nil, err
	}

	if strings.HasSuffix(p, ".7z") {
		return zlib.NewReader(r)
	} else if strings.HasSuffix(p, ".gz") {
		return gzip.NewReader(r)
	}
	return r, nil
}

func (f *FileHelper) NewPbReader(p string) (pbio.ReadCloser, error) {
	r, err := f.NewReader(p)
	if err != nil {
		return nil, err
	}
	return pbio.NewDelimitedReader(r, DefaultProtoMaxSize), err
}

func (f *FileHelper) NewWriter(p string) (io.WriteCloser, error) {
	var err error
	var w io.WriteCloser
	if strings.HasPrefix(p, "gs://") {
		gs, err := ParseGcsPath(p)
		if err != nil {
			return nil, err
		}
		c, err := f.getClient()
		if err != nil {
			return nil, err
		}
		// TODO use returned CancelFunc.
		ctx, _ := f.createContext()
		w = c.Bucket(gs.Bucket).Object(gs.Path).NewWriter(ctx)
	} else {
		if f.createNewDirs {
			os.MkdirAll(path.Dir(p), DefaultDirPerm)
		}
		w, err = os.Create(p)
		if err != nil {
			return nil, err
		}
	}
	return MaybeAddCompression(p, w)
}

func (f *FileHelper) NewPbWriter(p string) (pbio.WriteCloser, error) {
	w, err := f.NewWriter(p)
	if err != nil {
		return nil, err
	}
	return pbio.NewDelimitedWriter(w), nil
}

func (f *FileHelper) FindLast(gs *GCSPath, pattern *regexp.Regexp) (string, error) {
	c, err := f.getClient()
	if err != nil {
		return "", err
	}
	return FindLast(c, gs, pattern)
}

func (f *FileHelper) FindLastForDate(gs *GCSPath, pattern *regexp.Regexp, t time.Time) (string, error) {
	c, err := f.getClient()
	if err != nil {
		return "", err
	}
	return FindLastForDate(c, gs, pattern, t)
}

func (f *FileHelper) FindAnyForDateSharded(gs *GCSPath, pattern *regexp.Regexp, t time.Time) ([]string, error) {
	c, err := f.getClient()
	if err != nil {
		return nil, err
	}
	return FindAnyForDateSharded(c, gs, pattern, t)
}

func (f *FileHelper) FindAnySharded(gs *GCSPath, pattern *regexp.Regexp, t time.Time) ([]string, error) {
	c, err := f.getClient()
	if err != nil {
		return nil, err
	}
	var ret []string
	for i := 0; i < MaxDaysInPast; i++ {
		ret, err = FindAnyForDateSharded(c, gs, pattern, t)
		if !os.IsNotExist(err) {
			break
		}
		t = t.AddDate(0, 0, -1)
	}
	return ret, err
}

func (f *FileHelper) Close() error {
	f.m.Lock()
	defer f.m.Unlock()
	c := f.client
	if c != nil {
		return c.Close()
	}
	return nil
}

func (f *FileHelper) ListAll(gs *GCSPath, pattern *regexp.Regexp) ([]string, error) {
	c, err := f.getClient()
	if err != nil {
		return nil, err
	}
	ctx, canc := f.createContext()
	defer canc()
	objIter := c.Bucket(gs.Bucket).Objects(ctx, &storage.Query{
		Prefix: gs.Path,
	})
	var p []string
	for {
		attr, err := objIter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		if pattern.MatchString(attr.Name) {
			p = append(p, attr.Name)
		}
	}
	return p, nil
}

func ObjectsExist(client *storage.Client, files ...*GCSPath) bool {
	for _, v := range files {
		_, err := client.Bucket(v.Bucket).Object(v.Path).Attrs(context.TODO())
		if err == storage.ErrObjectNotExist {
			logrus.Debugf("not exist: %s", v.String())
			return false
		}
	}
	return true
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
