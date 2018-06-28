package fileutils

import (
	"compress/gzip"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/datainq/filab"
	"github.com/datainq/filab/gcs"
	"github.com/datainq/rwmc"
	"github.com/hashicorp/errwrap"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

type CloudStorageClient struct {
	bucketName string
	client     *storage.Client
	baseCtx    context.Context
	bucket     *storage.BucketHandle
}

func ConnectToCloud(keyFile string) (*storage.Client, error) {
	opts := []option.ClientOption{
		option.WithGRPCDialOption(grpc.WithBlock()), // enforces a wait until connected
	}
	if len(keyFile) > 0 {
		opts = append(opts, option.WithCredentialsFile(keyFile))
	}
	ctx, canc := context.WithTimeout(context.Background(), 10*time.Second)
	defer canc()
	return storage.NewClient(ctx, opts...)
}

func NewCloudStorageClient(bucketName string, keyFile string) (*CloudStorageClient, error) {
	client, err := ConnectToCloud(keyFile)
	if err != nil {
		return nil, err
	}
	c := &CloudStorageClient{
		bucketName,
		client,
		context.Background(),
		nil,
	}
	if len(bucketName) > 0 {
		c = c.ForBucket(bucketName)
	}
	return c, nil
}

func (g *CloudStorageClient) ForBucket(bucket string) *CloudStorageClient {
	var c CloudStorageClient = *g
	c.bucket = c.client.Bucket(bucket)
	return &c
}

var ErrObjectExist = errors.New("storage: object does exist")

func GcsCreateWriter(client *storage.Client, ctx context.Context, gsPath string, overwrite bool) (*storage.Writer, error) {
	p, err := gcs.ParseGcsPath(gsPath)
	if err != nil {
		return nil, err
	}
	object := client.Bucket(p.Bucket).Object(p.Path)
	if !overwrite {
		_, err := object.Attrs(ctx)
		if err == nil {
			// don't overwrite
			return nil, ErrObjectExist
		} else if err != storage.ErrObjectNotExist {
			return nil, err
		}
		object = object.If(storage.Conditions{DoesNotExist: !overwrite})
	}
	w := object.NewWriter(ctx)
	//w.ContentType = "text/plain"
	//w.ContentEncoding = "gzip"
	w.Metadata = map[string]string{
		"x-created-at": time.Now().UTC().String(),
		//"x-for-date":   "",
	}
	return w, nil
}

func (g *CloudStorageClient) CreateWriter(ctx context.Context, fileName string,
	overwrite bool) (*storage.Writer, error) {
	p := (&gcs.GCSPath{g.bucketName, fileName}).String()
	return GcsCreateWriter(g.client, ctx, p, overwrite)
}

func (g *CloudStorageClient) Reader(filepath string, ctx context.Context) (*storage.Reader, error) {
	return g.bucket.Object(filepath).NewReader(ctx)
}

func (g *CloudStorageClient) GzipReader(filepath string, ctx context.Context) (reader io.ReadCloser, err error) {
	reader, err = g.bucket.Object(filepath).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(filepath, ".gz") {
		r, err := gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		reader = rwmc.NewReadMultiCloser(r, reader)
	}
	return
}

func (g *CloudStorageClient) CopyAndRm(dst, src string) error {
	srcObj := g.bucket.Object(src)
	copier := g.bucket.Object(dst).CopierFrom(srcObj)
	_, err := copier.Run(g.baseCtx)
	if err != nil {
		return err
	}
	return srcObj.Delete(g.baseCtx)
}

func (g *CloudStorageClient) Copy(dst, src string) error {
	bucket := g.client.Bucket(g.bucketName)
	copier := bucket.Object(dst).CopierFrom(bucket.Object(src))
	_, err := copier.Run(g.baseCtx)
	return err
}

func (g *CloudStorageClient) Close() error {
	return g.client.Close()
}

func (g *CloudStorageClient) Find(s string) *storage.ObjectIterator {
	if len(s) > 0 && s[len(s)-1] != '/' {
		s = s + "/"
		logrus.Infof("added trailing /: %q", s)
	}
	return g.bucket.Objects(
		context.Background(),
		&storage.Query{Prefix: s, Delimiter: "/"},
	)
}

func CopyToCloudF(gclient *CloudStorageClient, filePath, objectPath string) {
	ctx, canc := context.WithTimeout(context.Background(), time.Minute)
	defer canc()
	if err := OldCopyToCloud(gclient.client, ctx, filePath, objectPath); err != nil {
		logrus.Fatal(err)
	}
}

func CopyToCloud(baseCtx context.Context, storage filab.FileStorage,
	src, dest filab.Path) error {

	ctx, canc := context.WithCancel(baseCtx)
	defer canc()
	r, err := storage.NewReader(ctx, src)
	if err != nil {
		return err
	}
	defer r.Close()

	w, err := storage.NewWriter(ctx, dest)
	if err != nil {
		return err
	}

	if _, err := io.Copy(w, r); err != nil {
		return err
	}
	return w.Close()
}

func OldCopyToCloud(gclient *storage.Client, baseCtx context.Context,
	filePath, objectPath string) error {

	ctx, canc := context.WithCancel(baseCtx)
	defer canc()
	logrus.Debugf("creating cloud writer: %s", objectPath)
	writer, err := GcsCreateWriter(gclient, ctx, objectPath, true)
	if err != nil {
		return errwrap.Wrapf("cannot create writer: {{err}}", err)
	}
	logrus.Debugf("creating local reader")
	src, err := os.Open(filePath)
	if err != nil {
		return errwrap.Wrapf("cannot open file to read: {{err}}", err)
	}
	defer src.Close()
	logrus.Debugf("copying")
	if _, err = io.Copy(writer, src); err != nil {
		return errwrap.Wrapf("problem with copying content: {{err}}", err)
	}
	logrus.Debugf("closing cloud writer")
	if err = writer.Close(); err != nil {
		return errwrap.Wrapf("problem closing cloud object: {{err}}", err)
	}
	return nil
}
