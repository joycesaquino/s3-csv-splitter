package writer

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"splitter/internal/shatter"
)

const BucketWriter = `env:"BUCKET_WRITER,required"`

type Result struct {
	Key   string
	Cause error
}

func (r Result) Error() string {
	return fmt.Sprintf("Error to write %s on s3. %s", r.Key, r.Cause)
}

type Writer struct {
	s3 *S3Writer
}

func (w *Writer) Write(bucket string, outputs <-chan *shatter.ChunkOutput) <-chan *Result {
	results := make(chan *Result, len(outputs))

	var wg sync.WaitGroup

	for o := range outputs {

		wg.Add(1)

		go func(o shatter.ChunkOutput) {
			defer wg.Done()
			e := w.s3.Write(bucket, o.Key, o.Data)
			if e != nil {
				results <- &Result{Key: o.Key, Cause: e}
			}
		}(*o)
	}

	// Wait and Close
	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}

type S3Writer struct {
	ctx      context.Context
	uploader *s3manager.Uploader
}

func (sw *S3Writer) Write(bucket string, key string, body []byte) error {

	input := &s3manager.UploadInput{
		Key:    aws.String(key),
		Bucket: aws.String(bucket),
		Body:   bytes.NewReader(body),
	}

	_, err := sw.uploader.UploadWithContext(sw.ctx, input)
	return err
}

func NewS3Writer(ctx context.Context, session *session.Session) *S3Writer {
	s := s3.New(session)
	return &S3Writer{
		ctx:      ctx,
		uploader: s3manager.NewUploaderWithClient(s),
	}
}

func New(ctx context.Context, session *session.Session) *Writer {
	return &Writer{s3: NewS3Writer(ctx, session)}
}
