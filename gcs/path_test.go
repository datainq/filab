package gcs

import (
	"testing"

	"github.com/datainq/filab"
	"github.com/stretchr/testify/assert"
)

var (
	_ filab.Path = &GCSPath{}
)

func TestParseGcsPath(t *testing.T) {
	p, err := ParseGcsPath("gs://bucket/file")
	assert.NoError(t, err)
	assert.Equal(t, "bucket", p.Bucket)
	assert.Equal(t, "file", p.Path)
	assert.Equal(t, "gs://bucket/file", p.String())
}

func TestGCSPath_BaseStr(t *testing.T) {
	p, err := ParseGcsPath("gs://bucket/file")
	assert.NoError(t, err)
	assert.Equal(t, "gs://bucket", p.DirStr())
	assert.Equal(t, "file", p.BaseStr())
}
