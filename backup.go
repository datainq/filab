package filab

import (
	"context"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/sirupsen/logrus"
)

// AggregateToGcs copies a messages from all files into one dest path.
func AggregateToGcs(gceClient *CloudStorageClient, ctx context.Context, files []string,
	destGsPath string) error {
	var w io.WriteCloser
	// TODO should not overwrite without checking the size / checksum
	gceWriter, err := gceClient.CreateWriter(ctx, destGsPath, true)
	if err != nil {
		logrus.Errorf("cannot create dest cloud object %s: %s", destGsPath, err)
		return err
	}
	w, err = MaybeAddCompression(destGsPath, gceWriter)
	if err != nil {
		logrus.Fatalf("cannot add compression: %s", err)
		gceWriter.CloseWithError(err)
		return err
	}

	if err := AggregateProtoFiles(files, w); err != nil {
		logrus.Errorf("aggregation err: %s", err)
		gceWriter.CloseWithError(err)
		return err
	}

	logrus.Infof("successfully backuped up: %s", destGsPath)

	if err = w.Close(); err != nil {
		logrus.Errorf("error closing bucket object: %s", err)
		return err
	}
	return nil
}

type Backuper struct {
	Interval   time.Duration
	GcsPath    GCSPath
	GceKeyFile string
	// If set true, the proto files will be first aggregated.
	Aggregate         bool
	DeleteAfterBackup bool
	StripSrcPrefix    string

	queue        []ft
	inProgress   []ft
	guardModify  *sync.Mutex
	guardRun     *sync.Mutex
	pleaseFinish chan struct{}
	done         chan struct{}
	gceClient    *CloudStorageClient
}

func NewBackuper(interval time.Duration, gcsPath GCSPath, key string, delete bool) *Backuper {
	return &Backuper{
		Interval:          interval,
		GcsPath:           gcsPath,
		GceKeyFile:        key,
		DeleteAfterBackup: delete,
		guardModify:       &sync.Mutex{},
		guardRun:          &sync.Mutex{},
		pleaseFinish:      make(chan struct{}),
		done:              make(chan struct{}),
	}
}

func (b *Backuper) Start() {
	tckr := time.NewTicker(b.Interval)
	for {
		ctx, canc := context.WithTimeout(context.Background(), 15*time.Second)
		if err := b.BackupNow(ctx); err != nil {
			logrus.Errorf("backup problem: %s", err)
		}
		canc()
		select {
		case <-tckr.C:
		case <-b.pleaseFinish:
			tckr.Stop()
			close(b.done)
			return
		}
	}
}

func (b *Backuper) Stop() {
	close(b.pleaseFinish)
	<-b.done
	if b.gceClient != nil {
		b.gceClient.Close()
	}
}

func (b *Backuper) backupAggregated(gceClient *CloudStorageClient, baseCtx context.Context) error {
	for len(b.inProgress) > 0 {
		mt := b.inProgress[0]
		files := []string{}
		y, m, d := mt.t.Date()
		for i, v := range b.inProgress {
			if y1, m1, d1 := v.t.Date(); y != y1 || m != m1 || d != d1 || i > 5 {
				break
			}
			files = append(files, v.f)
		}

		dest := path.Join(b.GcsPath.Path, mt.t.Format("2006/01/02/150405")+".pb.gz")
		ctx, canc := context.WithTimeout(baseCtx, time.Minute)
		if err := AggregateToGcs(gceClient, ctx, files, dest); err != nil {
			return err
		}
		canc()
		b.inProgress = b.inProgress[len(files):]

		if b.DeleteAfterBackup {
			for _, f := range files {
				if err := os.Remove(f); err != nil {
					logrus.Errorf("cannot delete backuped: %s", err)
				}
			}
		}
	}
	return nil
}

func (b *Backuper) backup(gceClient *CloudStorageClient, baseCtx context.Context) error {
	var retErr error
	var left []ft
	for _, mt := range b.inProgress {
		destPath := mt.f
		if b.StripSrcPrefix != "" {
			destPath = strings.TrimPrefix(mt.f, b.StripSrcPrefix)
		}
		destPath = b.GcsPath.Join(destPath).Path
		ctx, canc := context.WithTimeout(baseCtx, time.Minute)
		err := CopyToCloud(gceClient, ctx, mt.f, destPath)
		canc()
		if err != nil {
			multierror.Append(retErr, err)
			left = append(left, mt)
			continue
		}

		if b.DeleteAfterBackup {
			if err := os.Remove(mt.f); err != nil {
				logrus.Errorf("cannot delete backuped: %s", err)
			}
		}
	}
	b.inProgress = left
	return nil
}

func (b *Backuper) getClient() *CloudStorageClient {
	gceClient := b.gceClient
	if gceClient == nil {
		logrus.Debug("connect to Cloud Storage")
		var err error
		gceClient, err = NewCloudStorageClient(b.GcsPath.Bucket, b.GceKeyFile)
		if err != nil {
			// TODO should not fail miserably
			logrus.Fatalf("cannot create GCE client: %s", err)
		}
		if b.Interval < 15*time.Minute {
			b.gceClient = gceClient
		}
	}
	return gceClient
}

func (b *Backuper) BackupNow(ctx context.Context) error {
	b.guardRun.Lock()
	defer b.guardRun.Unlock()
	b.guardModify.Lock()
	b.inProgress = append(b.inProgress, b.queue...)
	b.queue = nil
	b.guardModify.Unlock()

	if len(b.inProgress) == 0 {
		logrus.Debug("nothing to backup")
		return nil
	}

	// Increasing time
	sort.Slice(b.inProgress, func(i, j int) bool {
		return b.inProgress[i].t.Before(b.inProgress[j].t)
	})

	gceClient := b.getClient()
	if b.gceClient == nil {
		defer gceClient.Close()
	}

	if b.Aggregate {
		b.backupAggregated(gceClient, ctx)
	} else {
		b.backup(gceClient, ctx)
	}

	return nil
}

type ft struct {
	f string
	t time.Time
}

func (b *Backuper) Add(f string, t time.Time) {
	b.guardModify.Lock()
	b.queue = append(b.queue, ft{f, t})
	b.guardModify.Unlock()
}
