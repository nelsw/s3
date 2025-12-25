package s3

import (
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

var service Service

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:         os.Stdout,
		FieldsOrder: []string{"key", "prefix", "after", "size", "body", "keys"},
	})
}

func testKey(id ...ulid.ULID) string {
	var ID = ulid.MustParse("01K48PC0BK13BWV2CGWFP8QQH0")
	if len(id) > 0 {
		ID = id[0]
	}
	return "users/" + ID.String() + "/_.json"
}

func testBody(id ...ulid.ULID) string {
	var ID = ulid.MustParse("01K48PC0BK13BWV2CGWFP8QQH0")
	if len(id) > 0 {
		ID = id[0]
	}
	return `{"id":"` + ID.String() + `"}`
}

func InitTest(t *testing.T) {
	t.Setenv("S3_BUCKET", "bytelyon-db")
	service = New()
}

func TestClient_Put(t *testing.T) {
	InitTest(t)
	assert.NoError(t, service.Put(testKey(), testBody()))
}

func TestClient_Get(t *testing.T) {
	InitTest(t)
	out, err := service.Get(testKey())
	assert.NoError(t, err)
	assert.Equal(t, testBody(), string(out))
}

func TestClient_Delete(t *testing.T) {
	InitTest(t)
	assert.NoError(t, service.Delete(testKey()))
}

func TestClient_Keys(t *testing.T) {

	InitTest(t)

	var ids []ulid.ULID
	for i := 0; i < 10; i++ {
		ids = append(ids, ulid.Make())
		assert.NoError(t, service.Put(testKey(ids[i]), testBody(ids[i])))
	}

	keys, err := service.Keys("users/", testKey(ids[5]), 2)

	assert.NoError(t, err)
	assert.Len(t, keys, 2)
	assert.Equal(t, testKey(ids[6]), keys[0])
	assert.Equal(t, testKey(ids[7]), keys[1])

	for _, id := range ids {
		_ = service.Delete(testKey(id))
	}
}

func TestClient_URL(t *testing.T) {

	InitTest(t)

	assert.NoError(t, service.Put(testKey(), testBody()))

	url, err := service.URL(testKey(), 5)
	assert.NoError(t, err)

	var out *http.Response
	out, err = http.Get(url)
	assert.NoError(t, err)

	defer out.Body.Close()
	var b []byte
	b, err = io.ReadAll(out.Body)

	assert.NoError(t, err)
	assert.Equal(t, testBody(), string(b))

	_ = service.Delete(testKey())
}

func TestClient_FindOne(t *testing.T) {
	InitTest(t)

	id := ulid.MustParse("01K48PC0BK13BWV2CGWFP8QQH0")

	assert.NoError(t, service.Put(testKey(id), testBody(id)))

	type User struct {
		ID ulid.ULID `json:"id"`
	}
	var user = new(User)

	assert.NoError(t, service.Find(testKey(id), user))
	assert.Equal(t, id, user.ID)
}
