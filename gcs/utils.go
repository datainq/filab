package gcs

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/datainq/filab"
	"github.com/sirupsen/logrus"
)

func ObjectsExist(client *storage.Client, files ...filab.Path) bool {
	for _, v := range files {
		p := v.(GCSPath)
		_, err := client.Bucket(p.Bucket).Object(p.Path).Attrs(context.TODO())
		if err == storage.ErrObjectNotExist {
			logrus.Debugf("not exist: %s", v.String())
			return false
		}
	}
	return true
}
