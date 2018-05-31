package fileutils

import (
	"compress/gzip"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
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

func NewCloudStorageClient(bucketName string, keyFile string) (*CloudStorageClient, error) {
	opts := []option.ClientOption{
		option.WithGRPCDialOption(grpc.WithBlock()), // enforces a wait until connected
	}
	if len(keyFile) > 0 {
		opts = append(opts, option.WithCredentialsFile(keyFile))
	}
	ctx, canc := context.WithTimeout(context.Background(), 10*time.Second)
	defer canc()
	client, err := storage.NewClient(ctx, opts...)
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

func (g *CloudStorageClient) CreateWriter(ctx context.Context, fileName string,
	overwrite bool) (*storage.Writer, error) {
	object := g.bucket.Object(fileName)

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
	if err := CopyToCloud(gclient, ctx, filePath, objectPath); err != nil {
		logrus.Fatal(err)
	}
}

func CopyToCloud(gclient *CloudStorageClient, ctx context.Context,
	filePath, objectPath string) error {

	logrus.Debugf("creating cloud writer: %s", objectPath)
	writer, err := gclient.CreateWriter(ctx, objectPath, true)
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
