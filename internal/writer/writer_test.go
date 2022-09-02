package writer

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lightsail"
	"gotest.tools/assert"
)

func getSession() *session.Session {
	awsConfig := &aws.Config{}
	awsConfig.WithRegion(lightsail.RegionNameUsEast1)

	// Create the session that the service will use.
	return session.Must(session.NewSession(awsConfig))
}

func TestS3Writer_Write(t *testing.T) {
	sess := getSession()
	ctx := context.Background()

	writer := NewS3Writer(ctx, sess)
	err := writer.Write("the-source-bucket-results", "key-test/test.txt", []byte("test"))
	assert.Assert(t, err == nil, "Error write file: %s", err)
}
