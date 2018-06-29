package fileutils

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/datainq/filab"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	DefaultQueueSize            = 10000
	DefaultAutoReloaderInterval = 11 * time.Minute

	MaxDaysInPast = 30
)

//// FindLastForDate searches in a give path matching regexp.
//// The path format is: <base>/YYYY/MM/dd/<pattern> with a given time t formatted
//// into it.
//// Returns a path or error.
//FindLastForDate(basePath string, pattern *regexp.Regexp, t time.Time) (string, error)
//
//// FindAnyForDateSharded is analogous to FindLastForDate except that it searches for
//// a sharded pattern.
//FindAnyForDateSharded(basePath string, pattern *regexp.Regexp, t time.Time) ([]string, error)
//
//// ReaderForLast searches for a last file matching given pattern.
//ReaderForLast(dirPath string, pattern *regexp.Regexp) io.ReadCloser
//

type StringMatcher interface {
	MatchString(s string) bool
}

func PrefixedWithPattern(storage filab.FileStorage, ctx context.Context,
	prefix filab.Path, pattern StringMatcher) ([]filab.Path, error) {
	names, err := storage.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	var ret []filab.Path
	for _, n := range names {
		if pattern.MatchString(n.String()) {
			ret = append(ret, n)
		}
	}
	return ret, nil
}

func GenSharded(dirPath filab.Path, prefix string, numShards int, suffix string) []filab.Path {
	var paths []filab.Path
	for i := 0; i < numShards; i++ {
		paths = append(paths,
			dirPath.Join(fmt.Sprintf("%s-%05d-of-%05d%s", prefix, i, numShards, suffix)))
	}
	return paths
}

//func FindLastForDate(client *storage.Client, gs *GCSPath, pattern *regexp.Regexp, t time.Time) (string, error) {
//	bucket := client.Bucket(gs.Bucket)
//	for i := 0; i < MaxDaysInPast; i++ {
//		prefix := path.Join(gs.Path, t.Format("2006/01/02")) + "/"
//		logrus.Debugf("search with basePath: %s", prefix)
//		objc := BucketFind(bucket, prefix)
//		var names []string
//		for {
//			atr, err := objc.Next()
//			if err == iterator.Done {
//				break
//			} else if err != nil {
//				return "", err
//			}
//			if len(atr.Prefix) > 0 {
//				logrus.Debugf("prefix: %s", atr.Prefix)
//				continue
//			}
//			if !pattern.MatchString(atr.Name) {
//				continue
//			}
//			names = append(names, atr.Name)
//		}
//		if len(names) > 0 {
//			sort.Strings(names)
//			for _, v := range names {
//				logrus.Debugf("matching: %s", v)
//			}
//			return gs.WithPath(names[len(names)-1]).String(), nil
//		}
//		t = t.AddDate(0, 0, -1)
//	}
//	return "", errors.New("not found")
//
//}
//
//func FindLast(client *storage.Client, gs *GCSPath, pattern *regexp.Regexp) (string, error) {
//	return FindLastForDate(client, gs, pattern, time.Now().UTC())
//}
//

func ObjectsExist(storage filab.FileStorage, files ...filab.Path) bool {
	for _, v := range files {
		if ok, err := storage.Exist(context.Background(), v); !ok {
			logrus.Debugf("not exist: %s", v.String())
			return false
		} else if err != nil {
			logrus.Errorf("error checking object existance: %s", err)
			return false
		}
	}
	return true
}

// TODO URGENT this is complex and needs tests
// FindSharded looks up a set of files matching sharding pattern.
func FindSharded(storage filab.FileStorage, gs filab.Path,
	pattern *regexp.Regexp) ([]filab.Path, error) {

	if pattern.NumSubexp() != 3 {
		logrus.Errorf("expecting a pattern with 3 subexp, got: %d", pattern.NumSubexp())
	}

	logrus.Debugf("search with basePath: %s", gs)
	var names []filab.Path
	processed := make(map[string]bool)
	done := errors.New("done")
	err := storage.Walk(context.Background(), gs, func(p filab.Path, err error) error {
		if !pattern.MatchString(p.String()) {
			logrus.Debugf("does not match pattern: %s", p)
			return nil
		}
		submatches := pattern.FindStringSubmatch(p.String())
		if len(submatches) != 4 {
			logrus.Debugf("not enough submatches: %s", p)
			return nil
		}
		numShards, err := strconv.ParseInt(submatches[2], 10, 64)
		if err != nil {
			logrus.Debugf("cannot parse shard num: %s", p)
			return nil
		}
		shards := GenSharded(p.Dir(), submatches[1], int(numShards), submatches[3])
		for _, shard := range shards {
			processed[shard.String()] = true
		}
		if !ObjectsExist(storage, shards...) {
			logrus.Debugf("not all shards exist: %s", p.String())
			return nil
		}
		names = shards

		return done
	})

	if err == done {
		return names, nil
	}

	return nil, os.ErrNotExist
}

//
