package filab

import (
	"compress/gzip"
	"compress/zlib"
	"context"
	"io"
	"strings"

	"github.com/datainq/rwmc"
	"github.com/orian/pbio"
)

const DefaultProtoMaxSize = 10000000

type DriverType *string

type WalkFunc func(p Path, err error) error

type FileStoreBase interface {
	Parse(s string) (Path, error)
	Exist(context.Context, Path) (bool, error)
	Delete(context.Context, Path) error
	NewReader(context.Context, Path) (io.ReadCloser, error)
	NewWriter(context.Context, Path) (io.WriteCloser, error)
	List(context.Context, Path) ([]Path, error)
	Walk(context.Context, Path, WalkFunc) error
}

type StorageDriver interface {
	Name() string
	Scheme() string
	Type() DriverType

	FileStoreBase
}

type FileStorage interface {
	RegisterDriver(driver StorageDriver) error

	FileStoreBase

	MustParse(p string) Path

	NewReaderS(p Path) (io.ReadCloser, error)
	NewPbReaderS(p Path) (pbio.ReadCloser, error)

	NewWriterS(p Path) (io.WriteCloser, error)
	NewPbWriterS(p Path) (pbio.WriteCloser, error)
}

var defaultStore = New()

func DefaultFileStore() FileStorage {
	return defaultStore
}

func New() FileStorage {
	return &fileStore{
		bySchemaPrefix:  make(map[string]StorageDriver),
		byType:          make(map[DriverType]StorageDriver),
		ProtoMaxSize:    DefaultProtoMaxSize,
		AutoCompression: true,
	}
}

type fileStore struct {
	bySchemaPrefix map[string]StorageDriver
	byType         map[DriverType]StorageDriver
	local          StorageDriver

	ProtoMaxSize    int
	AutoCompression bool
}

func (f *fileStore) NewReaderS(p Path) (io.ReadCloser, error) {
	r, err := f.NewReader(context.Background(), p)
	if err != nil || !f.AutoCompression {
		return r, err
	}
	if strings.HasSuffix(p.String(), ".7z") {
		r, err = zlib.NewReader(r)
	} else if strings.HasSuffix(p.String(), ".gz") {
		r, err = gzip.NewReader(r)
	}
	return r, err
}

func (f *fileStore) NewPbReaderS(p Path) (pbio.ReadCloser, error) {
	r, err := f.NewReaderS(p)
	if err != nil {
		return nil, err
	}
	return pbio.NewDelimitedReader(r, f.ProtoMaxSize), nil
}

func (f *fileStore) NewWriterS(p Path) (io.WriteCloser, error) {
	w, err := f.NewWriter(context.Background(), p)
	if err != nil || !f.AutoCompression {
		return w, err
	}
	return MaybeAddCompression(p.String(), w)
}

func (f *fileStore) NewPbWriterS(p Path) (pbio.WriteCloser, error) {
	w, err := f.NewWriterS(p)
	if err != nil {
		return nil, err
	}
	return pbio.NewDelimitedWriter(w), nil
}

func (f *fileStore) Parse(s string) (Path, error) {
	for k, v := range f.bySchemaPrefix {
		if strings.HasPrefix(s, k) {
			g, err := v.Parse(s)
			if err != nil {
				return nil, err
			}
			return g, nil
		}
	}
	return f.local.Parse(s)
}

func (f *fileStore) MustParse(s string) Path {
	p, err := f.Parse(s)
	if err != nil {
		panic("cannot parse")
	}
	return p
}

func (f *fileStore) Exist(ctx context.Context, p Path) (bool, error) {
	return f.byType[p.Type()].Exist(ctx, p)
}

func (f *fileStore) Delete(ctx context.Context, p Path) error {
	return f.byType[p.Type()].Delete(ctx, p)
}

func (f *fileStore) NewReader(ctx context.Context, p Path) (io.ReadCloser, error) {
	return f.byType[p.Type()].NewReader(ctx, p)
}

func (f *fileStore) NewWriter(ctx context.Context, p Path) (io.WriteCloser, error) {
	return f.byType[p.Type()].NewWriter(ctx, p)
}

func (f *fileStore) List(ctx context.Context, p Path) ([]Path, error) {
	return f.byType[p.Type()].List(ctx, p)
}

func (f *fileStore) Walk(ctx context.Context, p Path, w WalkFunc) error {
	return f.byType[p.Type()].Walk(ctx, p, w)
}

func (f *fileStore) RegisterDriver(driver StorageDriver) error {
	scheme := driver.Scheme()
	if scheme != "" {
		f.bySchemaPrefix[scheme+"://"] = driver
	} else {
		f.local = driver
	}
	f.byType[driver.Type()] = driver
	return nil
}

func MaybeAddCompression(file string, w io.WriteCloser) (io.WriteCloser, error) {
	if strings.HasSuffix(file, ".7z") {
		w1, err := zlib.NewWriterLevel(w, zlib.BestCompression)
		if err != nil {
			return w1, err
		}
		return rwmc.NewWriteMultiCloser(w1, w), nil
	} else if strings.HasSuffix(file, ".gz") {
		w1, err := gzip.NewWriterLevel(w, gzip.BestCompression)
		if err != nil {
			return w1, err
		}
		return rwmc.NewWriteMultiCloser(w1, w), nil
	}
	return w, nil
}

func MaybeAddDecompression(file string, r io.ReadCloser) (io.ReadCloser, error) {
	if r == nil {
		return nil,nil
	}
	if strings.HasSuffix(file, ".7z") {
		return zlib.NewReader(r)
	} else if strings.HasSuffix(file, ".gz") {
		return gzip.NewReader(r)
	}
	return r, nil
}
