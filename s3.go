package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog/log"
)

type Service interface {
	Delete(string) error
	Get(string) ([]byte, error)
	Put(string, any) error
	Keys(string, string, int32) ([]string, error)
	URL(string, int64) (string, error)
	Find(string, any) error
}

type client struct {
	Bucket *string
	*s3.Client
	*s3.PresignClient
	context.Context
}

// New returns a new S3 client with a Background context.
// An optional variadic set of Config values can be provided as
// input that will be prepended to the configs slice.
func New(optFns ...func(*config.LoadOptions) error) Service {
	return NewWithContext(context.Background(), optFns...)
}

// NewWithContext returns a new S3 client with the provided context.
// An optional variadic set of Config values can be provided as
// input that will be prepended to the configs slice.
func NewWithContext(ctx context.Context, optFns ...func(*config.LoadOptions) error) Service {
	cfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		panic(err)
	}
	b := os.Getenv("S3_BUCKET")
	if b == "" {
		panic("S3_BUCKET environment variable must be set")
	}
	c := s3.NewFromConfig(cfg)
	return &client{
		&b,
		c,
		s3.NewPresignClient(c),
		ctx,
	}
}

func (c *client) Delete(k string) error {
	_, err := c.DeleteObject(c.Context, &s3.DeleteObjectInput{
		Bucket: c.Bucket,
		Key:    &k,
	})

	log.Trace().
		Err(err).
		Str("key", k).
		Msg("Delete")

	return err
}

func (c *client) Get(k string) ([]byte, error) {
	out, err := c.GetObject(c.Context, &s3.GetObjectInput{
		Bucket: c.Bucket,
		Key:    &k,
	})

	var body []byte
	if err == nil {
		defer out.Body.Close()
		body, err = io.ReadAll(out.Body)
	}

	log.Trace().
		Err(err).
		Str("key", k).
		Bytes("body", body).
		Msg("Get")

	return body, err
}

func (c *client) Put(k string, a any) (err error) {

	var body []byte
	switch b := a.(type) {
	case []byte:
		body = b
	case string:
		body = []byte(b)
	default:
		if body, err = json.Marshal(a); err != nil {
			return
		}
	}

	_, err = c.PutObject(c.Context, &s3.PutObjectInput{
		Bucket: c.Bucket,
		Key:    &k,
		Body:   bytes.NewReader(body),
	})

	log.Trace().
		Err(err).
		Str("key", k).
		Bytes("body", body).
		Msg("Put")

	return
}

func (c *client) Keys(p, a string, s int32) ([]string, error) {

	out, err := c.ListObjectsV2(c.Context, &s3.ListObjectsV2Input{
		Bucket:     c.Bucket,
		Prefix:     &p,
		MaxKeys:    &s,
		StartAfter: &a,
	})

	var keys []string
	if err == nil {
		for _, obj := range out.Contents {
			keys = append(keys, *obj.Key)
		}
	}

	log.Trace().
		Err(err).
		Str("prefix", p).
		Str("after", a).
		Int32("size", s).
		Strs("keys", keys).
		Msg("Keys")

	return keys, err
}

func (c *client) URL(k string, i int64) (string, error) {

	out, err := c.PresignGetObject(c.Context, &s3.GetObjectInput{
		Bucket: c.Bucket,
		Key:    &k,
	}, s3.WithPresignExpires(time.Duration(i)*time.Minute))

	var url string
	if out != nil {
		url = out.URL
	}

	log.Trace().
		Err(err).
		Str("key", k).
		Int64("exp", i).
		Str("url", url).
		Msg("URL")

	return url, err
}

func (c *client) Find(k string, a any) error {

	b, err := c.Get(k)
	if err == nil {
		err = json.Unmarshal(b, a)
	}

	log.Trace().
		Err(err).
		Str("key", k).
		Any("body", a).
		Msg("FindOne")

	return err
}
