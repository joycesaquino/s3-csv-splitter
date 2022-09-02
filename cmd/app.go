package main

import (
	"context"
	"csv-splitter/internal/splitter"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
)

func main() {
	lambda.Start(Handler)
}

func Handler(ctx context.Context, events events.S3Event) {

	// Create the session that the service will use.
	sess := session.Must(session.NewSession())

	records := events.Records
	notCompleted := make(map[string]*splitter.SplitOutput, len(records))
	spltr := splitter.New(ctx, sess)
	for _, r := range records {
		key := r.S3.Object.Key
		bucket := r.S3.Bucket.Name
		output := spltr.Split(bucket, key)
		if output.Status != splitter.Completed {
			notCompleted[key] = output
		}
	}

	if len(notCompleted) > 0 {
		for key, output := range notCompleted {
			log.Printf("Error spliting - Key: %s - Status: %s - Errors: %s", key, output.Status, output.Errors)
		}
	} else {
		log.Print("All keys has been split")
	}

}
