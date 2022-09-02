package splitter

import (
	"context"
	"csv-splitter/internal/reader"
	"csv-splitter/internal/shatter"
	"csv-splitter/internal/writer"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/caarlos0/env"
	"log"
)

type Status string

const (
	ReadingError Status = "Reading error"
	ChunkError   Status = "Chunk error"
	Failed       Status = "Failed all files"
	Partial      Status = "Some files cannot be split"
	Completed    Status = "Split Completed"
)

type Config struct {
	BucketWriter string `env:"BUCKET_WRITER,required"`
	Path         string `env:"SPLITTER_PATH_RESULT,required"`
	NumberFiles  int    `env:"SPLITTER_NUMBER_LINES" envDefault:"50000"`
}

type SplitOutput struct {
	Status Status
	Errors []error
}

type Splitter struct {
	config  *Config
	reader  reader.Reader
	writer  *writer.Writer
	shatter *shatter.Shatter
}

func (s *Splitter) Split(bucketName string, key string) *SplitOutput {
	result := make(chan *SplitOutput, 1)
	result <- s.split(bucketName, key)
	return <-result
}

func (s *Splitter) split(bucketName string, key string) *SplitOutput {

	log.Printf("Reading file: %s/%s", bucketName, key)

	// Reading file on s3
	fileBytes, err := s.reader.Read(bucketName, key)
	if err != nil {
		return &SplitOutput{Status: ReadingError, Errors: []error{err}}
	}

	log.Printf("Successful read: %s/%s", bucketName, key)

	// Chunk in N files
	chunkInput := &shatter.ChunkInput{
		FinalName:  key,
		Data:       fileBytes,
		Lines:      s.config.NumberFiles,
		ParentPath: s.config.Path,
	}

	log.Printf("Starting chunk - File: %s - NumbersOfFile: %d - ParentPath: %s", chunkInput.FinalName, chunkInput.Lines, chunkInput.ParentPath)

	outputs, err := s.shatter.Chunk(chunkInput)
	log.Printf("Finish chunk")

	if err != nil {
		return &SplitOutput{Status: ChunkError, Errors: []error{err}}
	}

	log.Printf("Starting write")

	// Write
	results := s.writer.Write(s.config.BucketWriter, outputs)

	// Verify Results
	return s.verifyResults(results)
}

func (s *Splitter) verifyResults(results <-chan *writer.Result) *SplitOutput {
	// Parse Chan to Array
	var es []error
	for r := range results {
		es = append(es, r)
	}

	// Verify Results
	l := len(es)
	if l > 0 {
		if l < s.config.NumberFiles {
			return &SplitOutput{Status: Partial, Errors: es}
		} else {
			return &SplitOutput{Status: Failed, Errors: es}
		}
	}

	return &SplitOutput{Status: Completed}
}

func New(ctx context.Context, session *session.Session) *Splitter {
	var sc Config

	err := env.Parse(&sc)
	if err != nil {
		log.Fatalf("Error when create splitter: %s", err)
	}

	return &Splitter{
		config:  &sc,
		shatter: shatter.New(),
		writer:  writer.New(ctx, session),
		reader:  reader.NewS3Reader(ctx, session),
	}
}
