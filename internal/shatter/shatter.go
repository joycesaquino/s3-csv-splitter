package shatter

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type ChunkOutput struct {
	Key  string
	Data []byte
}

type ChunkInput struct {
	Data       []byte
	Lines      int
	FinalName  string
	ParentPath string
}

type Shatter struct{}

func (s *Shatter) Chunk(input *ChunkInput) (<-chan *ChunkOutput, error) {
	if !input.validate() {
		return nil, errors.New("Input is invalid. Data and Name is required! ")
	}

	output := make(chan *ChunkOutput)

	dataBreakLine := input.dataBreakLine()
	dataSize := len(dataBreakLine) - 1 // Remove header
	pos := input.Lines
	name := input.normalizeFileName()

	var wg sync.WaitGroup
	// starting one ignoring header
	for i := 1; i <= dataSize; i += input.Lines {
		if pos > dataSize {
			pos = dataSize
		}

		key := input.generateKey(name, i)
		wg.Add(1)
		// Chunk
		go func(index int, pos int, key string) {
			defer wg.Done()
			chunk := bytes.Join(dataBreakLine[index:pos], []byte{breakLine})
			output <- &ChunkOutput{Data: chunk, Key: key}
		}(i, pos, key)
		pos += input.Lines
	}

	go func() {
		wg.Wait()
		close(output)
	}()

	return output, nil
}

const slash = "/"
const csv = ".csv"

func (ci *ChunkInput) normalizeFileName() string {
	fileName := ci.FinalName
	if strings.Contains(fileName, slash) {
		split := strings.Split(fileName, slash)
		fileName = split[len(split)-1]
	}
	if strings.Contains(fileName, csv) {
		fileName = strings.Split(fileName, csv)[0]
	}
	return fileName
}

const splitterPattern = "%s-%d.csv"
const splitterPatternWithParent = "%s/%s/%s/%s/" + splitterPattern

func (ci *ChunkInput) generateKey(filename string, index int) string {
	year, month, day := time.Now().Date()
	return fmt.Sprintf(splitterPatternWithParent, ci.ParentPath, fmt.Sprint(year), fmt.Sprint(month), fmt.Sprint(day), filename, index)
}

const breakLine = '\n'

func (ci *ChunkInput) dataBreakLine() [][]byte {
	return bytes.Split(ci.Data, []byte{breakLine})
}

func (ci *ChunkInput) validate() bool {
	if ci.Data == nil || ci.FinalName == "" {
		return false
	}
	if ci.Lines <= 0 {
		ci.Lines = 10
	}
	return true
}

func New() *Shatter {
	return &Shatter{}
}
