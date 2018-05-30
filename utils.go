package filab

import (
	"io"

	"github.com/orian/pbio"
	"github.com/sirupsen/logrus"
)

func AggregateProtoFiles(files []string, dest io.WriteCloser) error {
	for _, f := range files {
		r, err := NewFileReader(f)
		if err != nil {
			if err != io.ErrUnexpectedEOF && err != io.EOF {
				return err
			}
			continue
		}

		r1 := pbio.NewDelimitedCopier(r, DefaultProtoMaxSize) // 1MB
		n := 0
		for err = nil; err == nil; err = r1.CopyMsg(dest) {
			n++
		}
		switch err {
		case io.ErrUnexpectedEOF:
			logrus.Errorf("corrupted file: %s", f)
			fallthrough
		case io.EOF:
			err = nil
			fallthrough
		case nil:
			r = r1
		default:
			// writing may be corrupted
			r1.Close()
			return err
		}
		logrus.Debugf("file: %s DONE, read msg: %d", f, n)
		if err = r.Close(); err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			logrus.Errorf("fail on a file: %s", f)
			return err
		}
	}
	return nil
}
