package reader

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

func TestS3Reader_Read(t *testing.T) {
	sess := getSession()
	ctx := context.Background()

	reader := NewS3Reader(ctx, sess)
	_, e := reader.Read("your-bucket-name", "bucket-key/test.txt")
	assert.Assert(t, e == nil, "Error to reading file: %s", e)

}
