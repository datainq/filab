package fileutils

import (
	"context"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/datainq/filab"
	"github.com/hashicorp/go-multierror"
	"github.com/sirupsen/logrus"
)

// AggregateToGcs copies a messages from all files into one dest path.
func AggregateToGcs(storage filab.FileStorage, ctx context.Context,
	files []filab.Path, destGsPath filab.Path) error {
	var w io.WriteCloser
	// TODO should not overwrite without checking the size / checksum
	gceWriter, err := storage.NewWriter(ctx, destGsPath)
	if err != nil {
		logrus.Errorf("cannot create dest cloud object %s: %s", destGsPath, err)
		return err
	}
	w, err = filab.MaybeAddCompression(destGsPath.String(), gceWriter)
	if err != nil {
		logrus.Fatalf("cannot add compression: %s", err)
		return err
	}

	if err := AggregateProtoFiles(storage, files, w); err != nil {
		logrus.Errorf("aggregation err: %s", err)
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
	GcsPath    filab.Path
	GceKeyFile string
	// If set true, the proto files will be first aggregated.
	Aggregate         bool
	DeleteAfterBackup bool
	StripSrcPrefix    string
	storage           filab.FileStorage

	queue        []ft
	inProgress   []ft
	guardModify  *sync.Mutex
	guardRun     *sync.Mutex
	pleaseFinish chan struct{}
	done         chan struct{}
}

func NewBackuper(storage filab.FileStorage, destPath filab.Path,
	interval time.Duration, delete bool) *Backuper {

	return &Backuper{
		Interval:          interval,
		GcsPath:           destPath,
		DeleteAfterBackup: delete,
		storage:           storage,
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
}

func (b *Backuper) backupAggregated(baseCtx context.Context) error {
	for len(b.inProgress) > 0 {
		mt := b.inProgress[0]
		var files []filab.Path
		y, m, d := mt.t.Date()
		for i, v := range b.inProgress {
			if y1, m1, d1 := v.t.Date(); y != y1 || m != m1 || d != d1 || i > 5 {
				break
			}
			files = append(files, v.f)
		}

		dest := b.GcsPath.Join(mt.t.Format("2006/01/02/150405") + ".pb.gz")
		ctx, canc := context.WithTimeout(baseCtx, time.Minute)
		if err := AggregateToGcs(b.storage, ctx, files, dest); err != nil {
			return err
		}
		canc()
		b.inProgress = b.inProgress[len(files):]

		if b.DeleteAfterBackup {
			for _, f := range files {
				if err := b.storage.Delete(context.Background(), f); err != nil {
					logrus.Errorf("cannot delete backuped: %s", err)
				}
			}
		}
	}
	return nil
}

func (b *Backuper) backup(baseCtx context.Context) error {
	var retErr error
	var left []ft
	for _, mt := range b.inProgress {
		srcPath := mt.f
		destPath := b.GcsPath.Join(srcPath.String())
		if b.StripSrcPrefix != "" {
			destPath = b.GcsPath.Join(strings.TrimPrefix(srcPath.String(), b.StripSrcPrefix))
		}
		ctx, canc := context.WithTimeout(baseCtx, time.Minute)
		err := CopyToCloud(ctx, b.storage, srcPath, destPath)
		canc()
		if err != nil {
			retErr = multierror.Append(retErr, err)
			left = append(left, mt)
			continue
		}

		if b.DeleteAfterBackup {
			if err := b.storage.Delete(context.Background(), mt.f); err != nil {
				logrus.Errorf("cannot delete backuped: %s", err)
			}
		}
	}
	b.inProgress = left
	return nil
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

	if b.Aggregate {
		return b.backupAggregated(ctx)
	} else {
		return b.backup(ctx)
	}

	return nil
}

type ft struct {
	f filab.Path
	t time.Time
}

func (b *Backuper) Add(f string, t time.Time) {
	p, err := b.storage.Parse(f)
	if err != nil {
		return
	}
	b.guardModify.Lock()
	b.queue = append(b.queue, ft{p, t})
	b.guardModify.Unlock()
}
