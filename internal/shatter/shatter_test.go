package shatter

import (
	"testing"

	"gotest.tools/assert"
)

func TestShatter_Chunk(t *testing.T) {
	data := []byte("header\nmaria\njose\nmadalena\njoao\nmatheus")

	shatter := &Shatter{}
	outputs, e := shatter.Chunk(&ChunkInput{
		Lines:     3,
		FinalName: "test.csv",
		Data:      data,
	})

	assert.Assert(t, e == nil, "Error: %s", e)
	for o := range outputs {
		t.Logf("Output Key: %s \n", o.Key)
		if o.Data == nil {
			t.Errorf("Data not found")
		}
	}
}
