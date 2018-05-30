package local

import (
	"testing"

	"github.com/datainq/filab"
	"github.com/stretchr/testify/assert"
)

var (
	_ filab.Path = LocalPath("")
)

func TestParseLocalPath(t *testing.T) {
	p, err := ParseLocalPath("/dir/file")
	assert.NoError(t, err)
	assert.Equal(t, "/dir/file", p.String())
}
