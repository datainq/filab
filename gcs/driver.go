package gcs

import (
	"context"
	"io"
	"path/filepath"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/datainq/filab"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

var googleCloudStorage = "Google Cloud Storage Driver"

type Option interface {
	apply(*GcsDriver)
}

type withBlock struct{}

func (withBlock) apply(g *GcsDriver) {
	g.connectOnNew = true
}

func WithBlock() Option {
	return withBlock{}
}

func WithKeyFile(f string) Option {
	return &withKeyFile{f}
}

type withKeyFile struct {
	f string
}

func (w *withKeyFile) apply(f *GcsDriver) {
	f.keyFile = w.f
}

//type newDir bool
//
//func (newDir) apply(f *GcsDriver) {
//	f.createNewDirs = true
//}
//
//func WithNewDir() Option {
//	return newDir(true)
//}

func WithClient(c *storage.Client) Option {
	return withClient{c}
}

type withClient struct {
	c *storage.Client
}

func (w withClient) apply(f *GcsDriver) {
	f.client = w.c
}

type withTimeout struct {
	timeout time.Duration
}

func (w withTimeout) apply(f *GcsDriver) {
	f.timeout = w.timeout
}

func WithTimeout(t time.Duration) Option {
	return withTimeout{t}
}

type GcsDriver struct {
	connectOnNew bool
	timeout      time.Duration
	keyFile      string
	client       *storage.Client
	m            sync.RWMutex
}

func (GcsDriver) Name() string {
	return googleCloudStorage
}

func (GcsDriver) Scheme() string {
	return "gs"
}

func Type() filab.DriverType {
	return filab.DriverType(&googleCloudStorage)
}

func (GcsDriver) Type() filab.DriverType {
	return Type()
}

func (*GcsDriver) Parse(s string) (filab.Path, error) {
	return ParseGcsPath(s)
}

func (f *GcsDriver) getClient() (*storage.Client, error) {
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
	} else {
		opts = append(opts, option.WithGRPCDialOption(grpc.WithInsecure()))
	}
	var err error
	ctx, canc := context.WithTimeout(context.Background(), f.timeout)
	defer canc()
	f.client, err = storage.NewClient(ctx, opts...)
	f.m.Unlock()
	return f.client, err
}

func (g *GcsDriver) Exist(ctx context.Context, p filab.Path) (bool, error) {
	c, err := g.getClient()
	if err != nil {
		return false, err
	}
	gp := p.(GCSPath)
	_, err = c.Bucket(gp.Bucket).Object(gp.Path).Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (g *GcsDriver) NewReader(ctx context.Context, p filab.Path) (io.ReadCloser, error) {
	c, err := g.getClient()
	if err != nil {
		return nil, err
	}
	gp := p.(GCSPath)
	return c.Bucket(gp.Bucket).Object(gp.Path).NewReader(ctx)
}

func (g *GcsDriver) NewWriter(ctx context.Context, p filab.Path) (io.WriteCloser, error) {
	c, err := g.getClient()
	if err != nil {
		return nil, err
	}
	gp := p.(GCSPath)
	return c.Bucket(gp.Bucket).Object(gp.Path).NewWriter(ctx), nil
}

func (g *GcsDriver) List(ctx context.Context, p filab.Path) ([]filab.Path, error) {
	gs := p.(GCSPath)
	c, err := g.getClient()
	if err != nil {
		return nil, err
	}
	objIter := c.Bucket(gs.Bucket).Objects(ctx, &storage.Query{
		Delimiter: "/",
		Prefix:    gs.Path,
	})
	var ret []filab.Path
	for {
		attr, err := objIter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		ret = append(ret, p.Join(attr.Name))
	}
	return ret, nil
}

func (*GcsDriver) Walk(context.Context, filab.Path, filepath.WalkFunc) {
	panic("implement me")
}

//type FileHelper struct {
//	timeout time.Duration
//	baseCtx context.Context
//
//	keyFile       string
//	client        *storage.Client
//	m             *sync.RWMutex
//	createNewDirs bool
//}
//
//func WithKeyFile(f string) Option {
//	return &withKeyFile{f}
//}
//
//type withKeyFile struct {
//	f string
//}
//
//func (w *withKeyFile) Apply(f *FileHelper) {
//	f.keyFile = w.f
//}
//
//type newDir bool
//
//func (newDir) Apply(f *FileHelper) {
//	f.createNewDirs = true
//}
//
//func WithNewDir() Option {
//	return newDir(true)
//}
//
//func WithClient(c *storage.Client) Option {
//	return withClient{c}
//}
//
//type withClient struct {
//	c *storage.Client
//}
//
//func (w withClient) Apply(f *FileHelper) {
//	f.client = w.c
//}
//
//type withTimeout struct {
//	timeout time.Duration
//}
//
//func (w withTimeout) Apply(f *FileHelper) {
//	f.timeout = w.timeout
//}
//
//func WithTimeout(t time.Duration) Option {
//	return withTimeout{t}
//}
//
//type Option interface {
//	Apply(*FileHelper)
//}
//
//func NewFileHelper(opts ...Option) *FileHelper {
//	r := &FileHelper{
//		m:       &sync.RWMutex{},
//		baseCtx: context.Background(),
//	}
//	for _, o := range opts {
//		o.Apply(r)
//	}
//	return r
//}
//
//func (f *FileHelper) getClient() (*storage.Client, error) {
//	f.m.RLock()
//	if f.client != nil {
//		f.m.RUnlock()
//		return f.client, nil
//	}
//	f.m.RUnlock()
//	f.m.Lock()
//	if f.client != nil {
//		f.m.Unlock()
//		return f.client, nil
//	}
//	opts := []option.ClientOption{option.WithGRPCDialOption(grpc.WithBlock())}
//	if len(f.keyFile) > 0 {
//		opts = append(opts, option.WithCredentialsFile(f.keyFile))
//	}
//	var err error
//	ctx, canc := f.createContext()
//	defer canc()
//	f.client, err = storage.NewClient(ctx, opts...)
//	f.m.Unlock()
//	return f.client, err
//}
//
//func (f *FileHelper) createContext() (context.Context, context.CancelFunc) {
//	if f.timeout > 0 {
//		return context.WithTimeout(f.baseCtx, f.timeout)
//	}
//	return f.baseCtx, func() {}
//}
//
//func (f *FileHelper) NewReader(p string) (io.ReadCloser, error) {
//	var err error
//	var r io.ReadCloser
//	if strings.HasPrefix(p, "gs://") {
//		gs, err2 := ParseGcsPath(p)
//		if err2 != nil {
//			return nil, err2
//		}
//		c, err2 := f.getClient()
//		if err2 != nil {
//			return nil, err2
//		}
//		ctx, _ := f.createContext()
//		r, err = c.Bucket(gs.Bucket).Object(gs.Path).NewReader(ctx)
//	} else {
//		r, err = os.Open(p)
//	}
//	if err != nil {
//		return nil, err
//	}
//
//	if strings.HasSuffix(p, ".7z") {
//		return zlib.NewReader(r)
//	} else if strings.HasSuffix(p, ".gz") {
//		return gzip.NewReader(r)
//	}
//	return r, nil
//}
//
//func (f *FileHelper) NewPbReader(p string) (pbio.ReadCloser, error) {
//	r, err := f.NewReader(p)
//	if err != nil {
//		return nil, err
//	}
//	return pbio.NewDelimitedReader(r, pbio.DefaultProtoMaxSize), err
//}
//
//func (f *FileHelper) NewWriter(p string) (io.WriteCloser, error) {
//	var err error
//	var w io.WriteCloser
//	if strings.HasPrefix(p, "gs://") {
//		gs, err := ParseGcsPath(p)
//		if err != nil {
//			return nil, err
//		}
//		c, err := f.getClient()
//		if err != nil {
//			return nil, err
//		}
//		// TODO use returned CancelFunc.
//		ctx, _ := f.createContext()
//		w = c.Bucket(gs.Bucket).Object(gs.Path).NewWriter(ctx)
//	} else {
//		if f.createNewDirs {
//			os.MkdirAll(path.Dir(p), DefaultDirPerm)
//		}
//		w, err = os.Create(p)
//		if err != nil {
//			return nil, err
//		}
//	}
//	return MaybeAddCompression(p, w)
//}
//
//func (f *FileHelper) NewPbWriter(p string) (pbio.WriteCloser, error) {
//	w, err := f.NewWriter(p)
//	if err != nil {
//		return nil, err
//	}
//	return pbio.NewDelimitedWriter(w), nil
//}
//
//func (f *FileHelper) FindLast(gs *GCSPath, pattern *regexp.Regexp) (string, error) {
//	c, err := f.getClient()
//	if err != nil {
//		return "", err
//	}
//	return FindLast(c, gs, pattern)
//}
//
//func (f *FileHelper) FindLastForDate(gs *GCSPath, pattern *regexp.Regexp, t time.Time) (string, error) {
//	c, err := f.getClient()
//	if err != nil {
//		return "", err
//	}
//	return FindLastForDate(c, gs, pattern, t)
//}
//
//func (f *FileHelper) FindAnyForDateSharded(gs GCSPath, pattern *regexp.Regexp, t time.Time) ([]string, error) {
//	c, err := f.getClient()
//	if err != nil {
//		return nil, err
//	}
//	return FindAnyForDateSharded(c, gs, pattern, t)
//}
//
//func (f *FileHelper) FindAnySharded(gs GCSPath, pattern *regexp.Regexp, t time.Time) ([]string, error) {
//	c, err := f.getClient()
//	if err != nil {
//		return nil, err
//	}
//	var ret []string
//	for i := 0; i < MaxDaysInPast; i++ {
//		ret, err = FindAnyForDateSharded(c, gs, pattern, t)
//		if !os.IsNotExist(err) {
//			break
//		}
//		t = t.AddDate(0, 0, -1)
//	}
//	return ret, err
//}
//
//func (f *FileHelper) Close() error {
//	f.m.Lock()
//	defer f.m.Unlock()
//	c := f.client
//	if c != nil {
//		return c.Close()
//	}
//	return nil
//}
//
//func (f *FileHelper) ListAll(gs *GCSPath, pattern *regexp.Regexp) ([]string, error) {
//	c, err := f.getClient()
//	if err != nil {
//		return nil, err
//	}
//	ctx, canc := f.createContext()
//	defer canc()
//	objIter := c.Bucket(gs.Bucket).Objects(ctx, &storage.Query{
//		Prefix: gs.Path,
//	})
//	var p []string
//	for {
//		attr, err := objIter.Next()
//		if err != nil {
//			if err == iterator.Done {
//				break
//			}
//			return nil, err
//		}
//		if pattern.MatchString(attr.Name) {
//			p = append(p, attr.Name)
//		}
//	}
//	return p, nil
//}
