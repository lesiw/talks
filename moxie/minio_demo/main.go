package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	stderr io.Writer = os.Stderr

	open        = os.Open
	exit        = os.Exit
	uploadFunc  = upload
	newS3Client = func(cfg aws.Config, optFn ...func(*s3.Options)) *S3Client {
		return &S3Client{s3.NewFromConfig(cfg, optFn...)}
	}

	s3opts = s3.Options{
		BaseEndpoint: aws.String("http://127.0.0.1:9000"),
		Credentials: credentials.NewStaticCredentialsProvider(
			"minioadmin", "minioadmin", ""),
	}
)

//go:generate moxie S3Client
type S3Client struct {
	*s3.Client
}

func main() {
	if err := run(os.Args...); err != nil {
		fmt.Fprintln(stderr, err)
		exit(1)
	}
}

func run(args ...string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: %s path file", args[0])
	}
	f, err := open(args[2])
	if err != nil {
		return err
	}
	bucket, path, ok := strings.Cut(args[1], "/")
	if !ok {
		return fmt.Errorf("bad path: %q", args[1])
	}
	if err := uploadFunc(f, bucket, path); err != nil {
		return err
	}
	return nil
}

func upload(r io.ReadSeeker, bucket, key string) error {
	ctx := context.Background()
	client := newS3Client(aws.Config{Region: "us-east-1"}, s3OptsFunc)

upload:
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Body:   r,
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil && strings.Contains(err.Error(), "NoSuchBucket") {
		_, err := client.CreateBucket(
			ctx,
			&s3.CreateBucketInput{Bucket: aws.String(bucket)},
		)
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		if _, err := r.Seek(0, 0); err != nil {
			return fmt.Errorf("failed to rewind reader: %w", err)
		}
		goto upload
	} else if err != nil {
		return fmt.Errorf("failed to upload object: %w", err)
	}

	return nil
}

func s3OptsFunc(o *s3.Options) {
	o.BaseEndpoint = s3opts.BaseEndpoint
	o.Credentials = s3opts.Credentials
}
