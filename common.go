package filab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

const (
	DefaultDirPerm              = 0740
	DefaultQueueSize            = 10000
	DefaultProtoMaxSize         = 10000000
	DefaultAutoReloaderInterval = 11 * time.Minute

	MaxDaysInPast = 30
)

type StringMatcher interface {
	MatchString(s string) bool
}

func PrefixedWithPattern(client *storage.Client, ctx context.Context, gs *GCSPath, pattern StringMatcher) ([]string, error) {
	bucket := client.Bucket(gs.Bucket)
	logrus.Debugf("search with basePath: %s", gs)
	iter := bucket.Objects(ctx, &storage.Query{Prefix: gs.Path})
	var names []string
	for {
		atr, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		if len(atr.Prefix) > 0 {
			logrus.Debugf("prefix: %s", atr.Prefix)
			continue
		}
		if !pattern.MatchString(atr.Name) {
			continue
		}
		names = append(names, atr.Name)
	}
	return names, nil
}

func BucketFind(bucket *storage.BucketHandle, s string) *storage.ObjectIterator {
	if len(s) > 0 && s[len(s)-1] != '/' {
		s = s + "/"
		logrus.Infof("added trailing /: %q", s)
	}
	return bucket.Objects(
		context.Background(),
		&storage.Query{Prefix: s, Delimiter: "/"},
	)
}

func FindLastForDate(client *storage.Client, gs *GCSPath, pattern *regexp.Regexp, t time.Time) (string, error) {
	bucket := client.Bucket(gs.Bucket)
	for i := 0; i < MaxDaysInPast; i++ {
		prefix := path.Join(gs.Path, t.Format("2006/01/02")) + "/"
		logrus.Debugf("search with basePath: %s", prefix)
		objc := BucketFind(bucket, prefix)
		var names []string
		for {
			atr, err := objc.Next()
			if err == iterator.Done {
				break
			} else if err != nil {
				return "", err
			}
			if len(atr.Prefix) > 0 {
				logrus.Debugf("prefix: %s", atr.Prefix)
				continue
			}
			if !pattern.MatchString(atr.Name) {
				continue
			}
			names = append(names, atr.Name)
		}
		if len(names) > 0 {
			sort.Strings(names)
			for _, v := range names {
				logrus.Debugf("matching: %s", v)
			}
			return gs.WithPath(names[len(names)-1]).String(), nil
		}
		t = t.AddDate(0, 0, -1)
	}
	return "", errors.New("not found")

}

func FindLast(client *storage.Client, gs *GCSPath, pattern *regexp.Regexp) (string, error) {
	return FindLastForDate(client, gs, pattern, time.Now().UTC())
}

func GenSharded(dirPath *GCSPath, prefix string, numShards int, suffix string) []*GCSPath {
	var paths []*GCSPath
	for i := 0; i < numShards; i++ {
		paths = append(paths,
			dirPath.Join(fmt.Sprintf("%s-%05d-of-%05d%s", prefix, i, numShards, suffix)))
	}
	return paths
}

// TODO URGENT this is complex and needs tests
func FindAnyForDateSharded(client *storage.Client, gs *GCSPath, pattern *regexp.Regexp, date time.Time) ([]string, error) {
	if pattern.NumSubexp() != 3 {
		logrus.Errorf("expecting a pattern with 3 subexp, got: %d", pattern.NumSubexp())
	}
	bucket := client.Bucket(gs.Bucket)

	pathPrefix := gs.Join(date.Format("2006/01/02"))
	logrus.Debugf("search with basePath: %s", pathPrefix)
	objc := BucketFind(bucket, pathPrefix.Path)
	var names []string
	processed := make(map[string]bool)
	for objc != nil {
		atr, err := objc.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		if len(atr.Prefix) > 0 {
			logrus.Debugf("prefix: %s", atr.Prefix)
			continue
		}
		if processed[atr.Name] {
			logrus.Debugf("seems processed: %s", atr.Name)
			continue
		}
		if !pattern.MatchString(atr.Name) {
			logrus.Debugf("does not match pattern: %s", atr.Name)
			continue
		}
		submatches := pattern.FindStringSubmatch(atr.Name)
		if len(submatches) != 4 {
			logrus.Debugf("not enough submatches: %s", atr.Prefix)
			continue
		}
		numShards, err := strconv.ParseInt(submatches[2], 10, 64)
		if err != nil {
			logrus.Debugf("cannot parse shard num: %s", atr.Name)
			continue
		}
		shards := GenSharded(pathPrefix, submatches[1], int(numShards), submatches[3])
		for _, shard := range shards {
			processed[shard.String()] = true
		}
		if !ObjectsExist(client, shards...) {
			logrus.Debugf("not all shards exist: %s", atr.Name)
			continue
		}
		for _, shard := range shards {
			names = append(names, shard.String())
		}
		return names, nil
	}
	return nil, os.ErrNotExist
}

