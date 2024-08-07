package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gotest.tools/v3/assert"
)

func TestMainFunction(t *testing.T) {
	f := &os.File{}
	swap(t, &open, func(path string) (*os.File, error) {
		assert.Equal(t, path, "file.dat")
		return f, nil
	})
	swap(t, &uploadFunc, func(rdr io.Reader, bucket, key string) error {
		assert.Equal(t, rdr, f)
		assert.Equal(t, bucket, "somebucket")
		assert.Equal(t, key, "some/key")
		return nil
	})
	swap(t, &os.Args, []string{"prog", "somebucket/some/key", "file.dat"})

	main()
}

func TestMainBadArgs(t *testing.T) {
	errbuf := new(bytes.Buffer)
	swap[io.Writer](t, &stderr, errbuf)
	swap(t, &exit, func(code int) { assert.Equal(t, code, 1) })
	swap(t, &open, func(path string) (*os.File, error) {
		t.Errorf("open called")
		return nil, nil
	})
	swap(t, &uploadFunc, func(rdr io.Reader, bucket, key string) error {
		t.Errorf("uploadFunc called")
		return nil
	})
	swap(t, &os.Args, []string{"prog", "somebucket/some/key"})

	main()

	if !strings.Contains(errbuf.String(), "usage:") {
		t.Errorf("expected usage string, got %q", errbuf.String())
	}
}

func TestMainBadFile(t *testing.T) {
	errbuf := new(bytes.Buffer)
	ferr := errors.New("no such file")
	swap[io.Writer](t, &stderr, errbuf)
	swap(t, &exit, func(code int) { assert.Equal(t, code, 1) })
	swap(t, &open, func(path string) (*os.File, error) {
		assert.Equal(t, path, "badfile")
		return nil, ferr
	})
	swap(t, &uploadFunc, func(rdr io.Reader, bucket, key string) error {
		t.Errorf("uploadFunc called")
		return nil
	})
	swap(t, &os.Args, []string{"prog", "somebucket/some/key", "badfile"})

	main()

	if !strings.Contains(errbuf.String(), ferr.Error()) {
		t.Errorf("want %q, got %q", ferr.Error(), errbuf.String())
	}
}

func TestMainBadPath(t *testing.T) {
	errbuf := new(bytes.Buffer)
	swap[io.Writer](t, &stderr, errbuf)
	swap(t, &exit, func(code int) { assert.Equal(t, code, 1) })
	swap(t, &open, func(path string) (*os.File, error) {
		assert.Equal(t, path, "goodfile")
		return new(os.File), nil
	})
	swap(t, &uploadFunc, func(rdr io.Reader, bucket, key string) error {
		t.Errorf("uploadFunc called")
		return nil
	})
	swap(t, &os.Args, []string{"prog", "badpath", "goodfile"})

	main()

	assert.Equal(t, errbuf.String(), "bad path: \"badpath\"\n")
}

func TestMainUploadError(t *testing.T) {
	errbuf := new(bytes.Buffer)
	f := new(os.File)
	uerr := errors.New("failed to upload file")
	swap[io.Writer](t, &stderr, errbuf)
	swap(t, &exit, func(code int) { assert.Equal(t, code, 1) })
	swap(t, &open, func(path string) (*os.File, error) {
		assert.Equal(t, path, "goodfile")
		return f, nil
	})
	swap(t, &uploadFunc, func(rdr io.Reader, bucket, key string) error {
		assert.Equal(t, rdr, f)
		assert.Equal(t, bucket, "somebucket")
		assert.Equal(t, key, "some/key")
		return uerr
	})
	swap(t, &os.Args, []string{"prog", "somebucket/some/key", "goodfile"})

	main()

	assert.Assert(t, strings.Contains(errbuf.String(), uerr.Error()))
}

func TestBucketExists(t *testing.T) {
	c := new(S3Client)
	swap(t, &newS3Client, func(aws.Config, ...func(*s3.Options)) *S3Client {
		return c
	})
	r := strings.NewReader("foo")
	c._PutObject_Stub()

	err := upload(r, "bucket", "file.txt")

	assert.NilError(t, err)
	assert.Equal(t, len(c._PutObject_Calls()), 1)
	call := c._PutObject_Calls()[0]
	assert.Equal(t, *call.params.Bucket, "bucket")
	assert.Equal(t, *call.params.Key, "file.txt")
	assert.Equal(t, call.params.Body, r)
	assert.Equal(t, len(c._CreateBucket_Calls()), 0)
}

func TestBucketDoesNotExist(t *testing.T) {
	c := new(S3Client)
	swap(t, &newS3Client, func(aws.Config, ...func(*s3.Options)) *S3Client {
		return c
	})
	r := strings.NewReader("foo")
	c._PutObject_Return(nil, errors.New("NoSuchBucket"))
	c._CreateBucket_Stub()
	c._PutObject_Return(nil, nil)

	err := upload(r, "bucket", "file.txt")

	assert.NilError(t, err)
	assert.Equal(t, len(c._PutObject_Calls()), 2)
	for _, call := range c._PutObject_Calls() {
		assert.Equal(t, *call.params.Bucket, "bucket")
		assert.Equal(t, *call.params.Key, "file.txt")
		assert.Equal(t, call.params.Body, r)
	}
	assert.Equal(t, len(c._CreateBucket_Calls()), 1)
	call := c._CreateBucket_Calls()[0]
	assert.Equal(t, *call.params.Bucket, "bucket")
}

func TestBucketExistsPutFailure(t *testing.T) {
	c := new(S3Client)
	swap(t, &newS3Client, func(aws.Config, ...func(*s3.Options)) *S3Client {
		return c
	})
	r := strings.NewReader("foo")
	perr := errors.New("failed to PutObject")
	c._PutObject_Return(nil, perr)

	err := upload(r, "bucket", "file.txt")

	assert.ErrorContains(t, err, perr.Error())
	assert.Equal(t, len(c._PutObject_Calls()), 1)
	call := c._PutObject_Calls()[0]
	assert.Equal(t, *call.params.Bucket, "bucket")
	assert.Equal(t, *call.params.Key, "file.txt")
	assert.Equal(t, call.params.Body, r)
	assert.Equal(t, len(c._CreateBucket_Calls()), 0)
}

func TestBucketDoesNotExistCreateFailure(t *testing.T) {
	c := new(S3Client)
	swap(t, &newS3Client, func(aws.Config, ...func(*s3.Options)) *S3Client {
		return c
	})
	r := strings.NewReader("foo")
	cerr := errors.New("failed to CreateBucket")
	c._PutObject_Return(nil, errors.New("NoSuchBucket"))
	c._CreateBucket_Return(nil, cerr)

	err := upload(r, "bucket", "file.txt")

	assert.ErrorContains(t, err, cerr.Error())
	assert.Equal(t, len(c._PutObject_Calls()), 1)
	pocall := c._PutObject_Calls()[0]
	assert.Equal(t, *pocall.params.Bucket, "bucket")
	assert.Equal(t, *pocall.params.Key, "file.txt")
	assert.Equal(t, pocall.params.Body, r)
	assert.Equal(t, len(c._CreateBucket_Calls()), 1)
	cbcall := c._CreateBucket_Calls()[0]
	assert.Equal(t, *cbcall.params.Bucket, "bucket")
}

func TestS3OptsFunc(t *testing.T) {
	opts := new(s3.Options)
	s3OptsFunc(opts)
	assert.Equal(t, opts.BaseEndpoint, s3opts.BaseEndpoint)
	assert.Equal(t, opts.Credentials, s3opts.Credentials)
}

func TestNewS3Client(t *testing.T) {
	c := newS3Client(aws.Config{})
	assert.Assert(t, c.Client != nil)
}

func swap[T any](t *testing.T, orig *T, with T) {
	o := *orig
	t.Cleanup(func() { *orig = o })
	*orig = with
}
