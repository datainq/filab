package local

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/datainq/filab"
)

var (
	_ filab.Path = LocalPath("")
)

func TestParseLocalPath(t *testing.T) {
	p,err := ParseLocalPath("/dir/file")
	assert.NoError(t, err)
	assert.Equal(t, "/dir/file", p.String())
}