package reader

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/caarlos0/env"
)

type Reader interface {
	Read(bucket string, key string) ([]byte, error)
}

type Config struct {
	BufferSize int `env:"BUFFER_SIZE" envDefault:"1024"`
}

type S3Reader struct {
	cfg *Config
	ctx context.Context
	dwl *s3manager.Downloader
}

func (sr *S3Reader) Read(bucket string, key string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Key:    aws.String(key),
		Bucket: aws.String(bucket),
	}

	// Create a new temporary file
	tempFile, err := os.Create(filepath.Join(os.TempDir(), "temp.csv"))
	if err != nil {
		return nil, err
	}

	_, err = sr.dwl.DownloadWithContext(sr.ctx, tempFile, input)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(tempFile)
}

func NewS3Reader(ctx context.Context, session *session.Session) *S3Reader {
	var cfg Config
	err := env.Parse(&cfg)
	if err != nil {
		log.Printf("Error to create reader config: %s", err)
	}

	s := s3.New(session)
	dwl := s3manager.NewDownloaderWithClient(s, func(d *s3manager.Downloader) {
		d.PartSize = 10 * 1024 * 1024 // 10MB per part
		d.Concurrency = 4
	})

	return &S3Reader{
		ctx: ctx,
		cfg: &cfg,
		dwl: dwl,
	}
}
