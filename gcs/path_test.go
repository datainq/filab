package gcs

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/datainq/filab"
)

var (
	_ filab.Path = &GCSPath{}
)

func TestParseGcsPath(t *testing.T) {
	p,err := ParseGcsPath("gs://bucket/file")
	assert.NoError(t, err)
	assert.Equal(t, "bucket", p.Bucket)
	assert.Equal(t, "file", p.Path)
	assert.Equal(t, "gs://bucket/file", p.String())
}