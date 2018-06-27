package local

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/datainq/filab"
	"github.com/stretchr/testify/assert"
)

var _ filab.StorageDriver = driver{}

func TestParse(t *testing.T) {
	d := New()
	p, err := d.Parse("/dir/file")
	assert.NoError(t, err)
	assert.Equal(t, "/dir/file", p.String())
}

func TestDriver_NewWriter(t *testing.T) {
	d := New()
	p, _ := d.Parse("/tmp/file")
	w, err := d.NewWriter(context.Background(), p)
	assert.NoError(t, err)
	defer w.Close()
	n, err := w.Write([]byte("test string"))
	assert.Equal(t, 11, n)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
}

func TestDriver_NewReader(t *testing.T) {
	d := New()
	p, _ := d.Parse("testdata/file")
	r, err := d.NewReader(context.Background(), p)
	assert.NoError(t, err)
	defer r.Close()
	b, err := ioutil.ReadAll(r)
	assert.Equal(t, 11, len(b))
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
}

func TestDriver_Exist(t *testing.T) {
	d := New()
	p, _ := d.Parse("testdata/file")
	ok, err := d.Exist(context.Background(), p)
	assert.NoError(t, err)
	assert.True(t, ok)

	p, _ = d.Parse("testdata/nofile")
	ok, err = d.Exist(context.Background(), p)
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestDriver_List(t *testing.T) {
	d := New()
	p, _ := d.Parse("testdata/")
	ps, err := d.List(context.Background(), p)
	assert.NoError(t, err)
	assert.Len(t, ps, 1)
	assert.Equal(t, ps[0].String(), "testdata/file")
}
