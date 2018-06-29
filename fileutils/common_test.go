package fileutils

import (
	"regexp"
	"testing"

	"github.com/datainq/filab"
	"github.com/datainq/filab/local"
	"github.com/stretchr/testify/assert"
)

var pattern = regexp.MustCompile(`/(sharded)-\d{5}-of-(\d{5})(.txt)$`)

func TestFindSharded(t *testing.T) {
	storage := filab.New()
	storage.RegisterDriver(local.New())

	fls, err := FindSharded(storage, storage.MustParse("./testdata/sharded/"), pattern)
	assert.NoError(t, err)
	expected := []filab.Path{
		storage.MustParse("testdata/sharded/sharded-00000-of-00003.txt"),
		storage.MustParse("testdata/sharded/sharded-00001-of-00003.txt"),
		storage.MustParse("testdata/sharded/sharded-00002-of-00003.txt"),
	}
	assert.Equal(t, expected, fls)
}
