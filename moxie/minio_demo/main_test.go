package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestMainFunction(t *testing.T) {
	f := &os.File{}
	swap(t, &open, func(path string) (*os.File, error) {
		if want := "file.dat"; path != want {
			t.Errorf("open(%q), want %q", path, want)
		}
		return f, nil
	})
	swap(t, &uploadFunc, func(r io.ReadSeeker, bucket, key string) error {
		if r != f || bucket != "somebucket" || key != "some/key" {
			t.Errorf("upload(%p, %q, %q), want %p, %q, %q",
				r, bucket, key,
				f, "somebucket", "some/key")
		}
		return nil
	})
	swap(t, &os.Args, []string{"prog", "somebucket/some/key", "file.dat"})

	main()
}

func TestMainBadArgs(t *testing.T) {
	errbuf := new(bytes.Buffer)
	swap[io.Writer](t, &stderr, errbuf)
	swap(t, &exit, func(code int) {
		if want := 1; code != want {
			t.Errorf("exit(%d), want %d", code, want)
		}
	})
	swap(t, &open, func(path string) (*os.File, error) {
		t.Errorf("open(%q), want no calls", path)
		return nil, nil
	})
	swap(t, &uploadFunc, func(r io.ReadSeeker, bucket, key string) error {
		t.Errorf("upload(%p, %q, %q), want no calls", r, bucket, key)
		return nil
	})
	swap(t, &os.Args, []string{"prog", "somebucket/some/key"})

	main()

	if got, s := errbuf.String(), "usage:"; !strings.Contains(got, s) {
		t.Errorf("errbuf = %q, want substr %q", got, s)
	}
}

func TestMainBadFile(t *testing.T) {
	errbuf := new(bytes.Buffer)
	ferr := errors.New("no such file")
	swap[io.Writer](t, &stderr, errbuf)
	swap(t, &exit, func(code int) {
		if want := 1; code != want {
			t.Errorf("exit(%d), want %d", code, want)
		}
	})
	swap(t, &open, func(path string) (*os.File, error) {
		if want := "badfile"; path != want {
			t.Errorf("open(%q), want %q", path, want)
		}
		return nil, ferr
	})
	swap(t, &uploadFunc, func(r io.ReadSeeker, bucket, key string) error {
		t.Errorf("upload(%p, %q, %q), want no calls", r, bucket, key)
		return nil
	})
	swap(t, &os.Args, []string{"prog", "somebucket/some/key", "badfile"})

	main()

	if got, s := errbuf.String(), ferr.Error(); !strings.Contains(got, s) {
		t.Errorf("errbuf = %q, want substr %q", got, s)
	}
}

func TestMainBadPath(t *testing.T) {
	errbuf := new(bytes.Buffer)
	swap[io.Writer](t, &stderr, errbuf)
	swap(t, &exit, func(code int) {
		if want := 1; code != want {
			t.Errorf("exit(%d), want %d", code, want)
		}
	})
	swap(t, &open, func(path string) (*os.File, error) {
		if want := "goodfile"; path != want {
			t.Errorf("open(%q), want %q", path, want)
		}
		return new(os.File), nil
	})
	swap(t, &uploadFunc, func(r io.ReadSeeker, bucket, key string) error {
		t.Errorf("upload(%p, %q, %q), want no calls", r, bucket, key)
		return nil
	})
	swap(t, &os.Args, []string{"prog", "badpath", "goodfile"})

	main()

	if got, want := errbuf.String(), "bad path: \"badpath\"\n"; got != want {
		t.Errorf("errbuf -want +got\n%s", cmp.Diff(got, want))
	}
}

func TestMainUploadError(t *testing.T) {
	errbuf := new(bytes.Buffer)
	f := new(os.File)
	uerr := errors.New("failed to upload file")
	swap[io.Writer](t, &stderr, errbuf)
	swap(t, &exit, func(code int) {
		if want := 1; code != want {
			t.Errorf("exit(%d), want %d", code, want)
		}
	})
	swap(t, &open, func(path string) (*os.File, error) {
		if want := "goodfile"; path != want {
			t.Errorf("open(%q), want %q", path, want)
		}
		return f, nil
	})
	swap(t, &uploadFunc, func(r io.ReadSeeker, bucket, key string) error {
		if r != f || bucket != "somebucket" || key != "some/key" {
			t.Errorf("upload(%p, %q, %q), want %p, %q, %q",
				r, bucket, key,
				f, "somebucket", "some/key")
		}
		return uerr
	})
	swap(t, &os.Args, []string{"prog", "somebucket/some/key", "goodfile"})

	main()

	if got, s := errbuf.String(), uerr.Error(); !strings.Contains(got, s) {
		t.Errorf("errbuf = %q, want substr %q", got, s)
	}
}

func TestBucketExists(t *testing.T) {
	c := new(S3Client)
	swap(t, &newS3Client, func(aws.Config, ...func(*s3.Options)) *S3Client {
		return c
	})
	body := "Hello, world!"
	r, bucket, key := strings.NewReader(body), "bucket", "file.txt"
	c._PutObject_Stub()

	uerr := upload(r, bucket, key)

	if uerr != nil {
		t.Errorf("upload(%p, %q, %q) = %q, want nil", r, bucket, key, uerr)
	}
	if gotc, wantc := len(c._PutObject_Calls()), 1; gotc == wantc {
		params := c._PutObject_Calls()[0].params
		if got, want := ptrstr(params.Bucket), ptrstr(&bucket); got != want {
			t.Errorf("PutObjectInput.Bucket = %s, want %s", got, want)
		}
		if got, want := ptrstr(params.Key), ptrstr(&key); got != want {
			t.Errorf("PutObjectInput.Key = %s, want %s", got, want)
		}
		if buf, err := io.ReadAll(params.Body); err != nil {
			t.Errorf("failed to read PutObjectInput.Body: %s", err)
		} else if got, want := string(buf), body; got != want {
			t.Errorf("PutObjectInput.Body = %q, want %q", got, want)
		}
	} else {
		t.Errorf("PutObject call count = %d, want %d", gotc, wantc)
	}
	if gotc, wantc := len(c._CreateBucket_Calls()), 0; gotc != wantc {
		t.Errorf("CreateBucket call count = %d, want %d", gotc, wantc)
	}
}

func TestBucketDoesNotExist(t *testing.T) {
	c := new(S3Client)
	swap(t, &newS3Client, func(aws.Config, ...func(*s3.Options)) *S3Client {
		return c
	})
	body := "Hello, world!"
	r, bucket, key := strings.NewReader(body), "bucket", "file.txt"
	validateParams := func(params *s3.PutObjectInput) {
		if got, want := ptrstr(params.Bucket), ptrstr(&bucket); got != want {
			t.Errorf("PutObjectInput.Bucket = %s, want %s", got, want)
		}
		if got, want := ptrstr(params.Key), ptrstr(&key); got != want {
			t.Errorf("PutObjectInput.Key = %s, want %s", got, want)
		}
		if buf, err := io.ReadAll(params.Body); err != nil {
			t.Errorf("failed to read PutObjectInput.Body: %s", err)
		} else if got, want := string(buf), body; got != want {
			t.Errorf("PutObjectInput.Body = %q, want %q", got, want)
		}
	}
	c._PutObject_Do(func(
		_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options),
	) (*s3.PutObjectOutput, error) {
		validateParams(params)
		return nil, errors.New("NoSuchBucket")
	})
	c._CreateBucket_Stub()
	c._PutObject_Do(func(
		_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options),
	) (*s3.PutObjectOutput, error) {
		validateParams(params)
		return nil, nil
	})

	uerr := upload(r, bucket, key)

	if uerr != nil {
		t.Errorf("upload(%p, %q, %q) = %q, want nil", r, bucket, key, uerr)
	}
	if gotc, wantc := len(c._PutObject_Calls()), 2; gotc != wantc {
		t.Errorf("PutObject call count = %d, want %d", gotc, wantc)
	}
	if gotc, wantc := len(c._CreateBucket_Calls()), 1; gotc == wantc {
		got, want := *c._CreateBucket_Calls()[0].params.Bucket, bucket
		if got != want {
			t.Errorf("CreateBucketInput.Bucket = %q, want %q", got, want)
		}
	} else {
		t.Errorf("CreateBucket call count = %d, want %d", gotc, wantc)
	}
}

func TestBucketExistsPutFailure(t *testing.T) {
	c := new(S3Client)
	swap(t, &newS3Client, func(aws.Config, ...func(*s3.Options)) *S3Client {
		return c
	})
	body := "Hello, world!"
	r, bucket, key := strings.NewReader(body), "bucket", "file.txt"
	perr := errors.New("failed to PutObject")
	c._PutObject_Return(nil, perr)

	uerr := upload(r, bucket, key)

	if s := perr.Error(); uerr == nil {
		t.Errorf("upload(%p, %q, %q) = <nil>, want substr %q",
			r, bucket, key, s)
	} else if got := uerr.Error(); !strings.Contains(got, s) {
		t.Errorf("upload(%p, %q, %q) = %q, want substr %q",
			r, bucket, key, got, s)
	}
	if gotc, wantc := len(c._PutObject_Calls()), 1; gotc == wantc {
		params := c._PutObject_Calls()[0].params
		if got, want := ptrstr(params.Bucket), ptrstr(&bucket); got != want {
			t.Errorf("PutObjectInput.Bucket = %s, want %s", got, want)
		}
		if got, want := ptrstr(params.Key), ptrstr(&key); got != want {
			t.Errorf("PutObjectInput.Key = %s, want %s", got, want)
		}
		if buf, err := io.ReadAll(params.Body); err != nil {
			t.Errorf("failed to read PutObjectInput.Body: %s", err)
		} else if got, want := string(buf), body; got != want {
			t.Errorf("PutObjectInput.Body = %q, want %q", got, want)
		}
	} else {
		t.Errorf("PutObject call count = %d, want %d", gotc, wantc)
	}
	if gotc, wantc := len(c._CreateBucket_Calls()), 0; gotc != wantc {
		t.Errorf("CreateBucket call count = %d, want %d", gotc, wantc)
	}
}

func TestBucketDoesNotExistCreateFailure(t *testing.T) {
	c := new(S3Client)
	swap(t, &newS3Client, func(aws.Config, ...func(*s3.Options)) *S3Client {
		return c
	})
	body := "Hello, world!"
	r, bucket, key := strings.NewReader(body), "bucket", "file.txt"
	cerr := errors.New("failed to CreateBucket")
	c._PutObject_Return(nil, errors.New("NoSuchBucket"))
	c._CreateBucket_Return(nil, cerr)

	uerr := upload(r, "bucket", "file.txt")

	if s := cerr.Error(); uerr == nil {
		t.Errorf("upload(%p, %q, %q) = <nil>, want substr %q",
			r, bucket, key, s)
	} else if got := uerr.Error(); !strings.Contains(got, s) {
		t.Errorf("upload(%p, %q, %q) = %q, want substr %q",
			r, bucket, key, got, s)
	}
	if gotc, wantc := len(c._PutObject_Calls()), 1; gotc == wantc {
		params := c._PutObject_Calls()[0].params
		if got, want := ptrstr(params.Bucket), ptrstr(&bucket); got != want {
			t.Errorf("PutObjectInput.Bucket = %s, want %s", got, want)
		}
		if got, want := ptrstr(params.Key), ptrstr(&key); got != want {
			t.Errorf("PutObjectInput.Key = %s, want %s", got, want)
		}
		if buf, err := io.ReadAll(params.Body); err != nil {
			t.Errorf("failed to read PutObjectInput.Body: %s", err)
		} else if got, want := string(buf), body; got != want {
			t.Errorf("PutObjectInput.Body = %q, want %q", got, want)
		}
	} else {
		t.Errorf("PutObject call count = %d, want %d", gotc, wantc)
	}
	if gotc, wantc := len(c._CreateBucket_Calls()), 1; gotc == wantc {
		got, want := *c._CreateBucket_Calls()[0].params.Bucket, bucket
		if got != want {
			t.Errorf("CreateBucketInput.Bucket = %q, want %q", got, want)
		}
	} else {
		t.Errorf("CreateBucket call count = %d, want %d", gotc, wantc)
	}
}

func TestS3OptsFunc(t *testing.T) {
	opts := new(s3.Options)
	s3OptsFunc(opts)
	copts := cmpopts.IgnoreUnexported(s3.Options{})
	if !cmp.Equal(s3opts, *opts, copts) {
		t.Errorf("s3.Options -want +got:\n%s", cmp.Diff(s3opts, *opts, copts))
	}
}

func TestNewS3Client(t *testing.T) {
	if cfg := (aws.Config{}); newS3Client(cfg).Client == nil {
		t.Errorf("newS3Client(%+v) = <nil>", cfg)
	}
}

func swap[T any](t *testing.T, orig *T, with T) {
	t.Helper()
	o := *orig
	t.Cleanup(func() { *orig = o })
	*orig = with
}

func ptrstr(s *string) string {
	if s == nil {
		return "<nil>"
	} else {
		return fmt.Sprintf("*%q", *s)
	}
}
