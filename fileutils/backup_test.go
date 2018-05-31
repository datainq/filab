package fileutils

import (
	"bytes"
	"io"
	"testing"

	"github.com/datainq/filab"
	"github.com/datainq/filab/local"
	"github.com/datainq/filab/testdata"
	"github.com/orian/pbio"
	"github.com/sirupsen/logrus"
)

type CloseBuffer struct {
	bytes.Buffer
}

func (c *CloseBuffer) Close() error {
	return nil
}

func (c *CloseBuffer) Write(b []byte) (n int, err error) {
	return c.Buffer.Write(b)
}

func TestAggregate(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	files := []filab.Path{
		local.LocalPath("testdata/20170126/143235"),
		local.LocalPath("testdata/20170126/143436"),
		local.LocalPath("testdata/20170126/1528.pb"),
		local.LocalPath("testdata/20170126/1650.pb"),
		local.LocalPath("testdata/20170126/1650.pb.gz"),
	}
	buf := &CloseBuffer{}
	var err error
	if err = AggregateProtoFiles(filab.DefaultFileStore(), files, buf); err != nil {
		t.Errorf("problem: %s", err)
		t.FailNow()
	}

	rc := pbio.NewDelimitedReader(buf, 1000000)
	t.Logf("wrote size %d", buf.Len())
	cnt := 0
	for err == nil {
		req := testdata.Entry{}
		err = rc.ReadMsg(&req)
		if err == nil {
			cnt++
			logrus.Debug(req.String())
		}
	}
	if err != nil && err != io.EOF {
		t.Errorf("err: %s", err)
	}
	if cnt != 15 {
		t.Errorf("want: 15, got: %d", cnt)
	}
}

//func TestGenFiles(t *testing.T) {
//	files := []string{
//		"testdata/20170126/143235",
//		"testdata/20170126/143436",
//		"testdata/20170126/1528.pb",
//		"testdata/20170126/1650.pb",
//		"testdata/20170126/1650.pb.gz",
//	}
//	g := 0
//	for i,v := range files {
//		w, err := NewFileWriter(v)
//		if err != nil {
//			t.Fatalf("canot create writer: %s", err)
//		}
//		pbW := pbio.NewDelimitedWriter(w)
//		for j := 0; j < i+1; j++ {
//			pbW.WriteMsg(&testdata.Entry{Text: fmt.Sprintf("entry=%d", g)})
//			g++
//		}
//		pbW.Close()
//	}
//}
