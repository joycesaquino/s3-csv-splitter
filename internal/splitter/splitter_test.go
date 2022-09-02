package splitter

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lightsail"
)

func setEnv() {
	_ = os.Setenv("SPLITTER_BUCKET", "the-source-bucket-output")
	_ = os.Setenv("SPLITTER_NUMBER_FILES", "50000")
	_ = os.Setenv("SPLITTER_PATH_RESULT", "results")
	_ = os.Setenv("BUCKET_WRITER", "the-target-bucket-splits")
}

func getSession() *session.Session {
	awsConfig := &aws.Config{}
	awsConfig.WithRegion(lightsail.RegionNameUsEast1)

	// Create the session that the service will use.
	return session.Must(session.NewSession(awsConfig))
}

func TestSplitter_Split(t *testing.T) {
	setEnv()

	sess := getSession()
	ctx := context.Background()
	splitter := New(ctx, sess)

	const bucket = "the-target-bucket-output"
	const key = "results-test/2020/April/8/11fa391c-64d7-4029-b557-4758d9e8674e.csv"

	output := splitter.Split(bucket, key)
	for _, e := range output.Errors {
		t.Errorf("Error in split - Status: %s - %s", output.Status, e)
	}

}
