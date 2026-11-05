package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cache"
	"google.golang.org/grpc"
)

func serveCLI(t *testing.T, options ...grpc.ServerOption) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer(options...)
	pb.RegisterCacheGatewayServer(s, cache.New())
	go s.Serve(l)
	t.Cleanup(s.Stop)
	t.Setenv("ORACACHE_ENDPOINT", l.Addr().String())
}

func TestCLI(t *testing.T) {
	serveCLI(t)
	call := func(args ...string) map[string]any {
		t.Helper()
		var out, stderr bytes.Buffer
		if code := Run(args, forbiddenReader{}, &out, &stderr); code != 0 {
			t.Fatalf("%v: code=%d %s", args, code, stderr.String())
		}
		var v map[string]any
		if err := json.Unmarshal(out.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	if v := call("get", "k"); v["status"] != "GET_STATUS_MISS" {
		t.Fatal(v)
	}
	if v := call("put", "--expected-rev=0", "k", "hello"); v["rev"] != "1" || v["status"] != "PUT_STATUS_OK" {
		t.Fatal(v)
	}
	if v := call("get", "--rev=1", "k"); v["data"] != base64.StdEncoding.EncodeToString([]byte("hello")) {
		t.Fatal(v)
	}
	if v := call("put", "--expected-rev=0", "k", "conflict"); v["status"] != "PUT_STATUS_CONFLICT" {
		t.Fatal(v)
	}
	if v := call("delete", "--rev=1", "k"); v["status"] != "DELETE_STATUS_DELETED" {
		t.Fatal(v)
	}
	if v := call("get", "k"); v["status"] != "GET_STATUS_MISS" {
		t.Fatal(v)
	}
}
func TestUsageErrors(t *testing.T) {
	t.Setenv("ORACACHE_ENDPOINT", "")
	var out, stderr bytes.Buffer
	if Run([]string{"get", "k"}, strings.NewReader(""), &out, &stderr) == 0 || !strings.Contains(stderr.String(), "ORACACHE_ENDPOINT") {
		t.Fatal(stderr.String())
	}
	t.Setenv("ORACACHE_ENDPOINT", "127.0.0.1:1")
	for _, args := range [][]string{{}, {"unknown"}, {"get"}, {"put", "--expected-rev=-1", "k"}, {"get", "--rev=-1", "k"}, {"get", "--timeout=0s", "k"}, {"get", "a", "b"}} {
		if Run(args, strings.NewReader(""), &out, &stderr) == 0 {
			t.Fatal(args)
		}
	}
	if Run([]string{"get", "--timeout=10ms", "k"}, strings.NewReader(""), &out, &stderr) == 0 {
		t.Fatal("unavailable endpoint succeeded")
	}
}

func TestPositionalValues(t *testing.T) {
	serveCLI(t)
	binary := []byte{0, 255, '\n', 'x'}
	file := filepath.Join(t.TempDir(), "binary with spaces")
	if err := os.WriteFile(file, binary, 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, value string
		want        []byte
		stdin       string
	}{
		{"literal spaces", "hello there", []byte("hello there"), ""},
		{"empty", "", nil, ""},
		{"literal at", "@@text", []byte("@text"), ""},
		{"file", "@" + file, binary, ""},
		{"stdin", "@-", binary, string(binary)},
		{"maximum", strings.Repeat("x", 10240), bytes.Repeat([]byte{'x'}, 10240), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, stderr bytes.Buffer
			if code := Run([]string{"put", tc.name, tc.value, "--expected-rev=0", "--timeout=1s"}, strings.NewReader(tc.stdin), &out, &stderr); code != 0 {
				t.Fatal(stderr.String())
			}
			out.Reset()
			if code := Run([]string{"get", tc.name, "--rev=1"}, forbiddenReader{}, &out, &stderr); code != 0 {
				t.Fatal(stderr.String())
			}
			var v map[string]any
			if err := json.Unmarshal(out.Bytes(), &v); err != nil {
				t.Fatal(err)
			}
			got, _ := v["data"].(string)
			if got != base64.StdEncoding.EncodeToString(tc.want) {
				t.Fatalf("%v", v)
			}
		})
	}
}

func TestExpectedRevisionPresence(t *testing.T) {
	requests := make(chan *pb.PutRequest, 3)
	serveCLI(t, grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if r, ok := request.(*pb.PutRequest); ok {
			requests <- r
		}
		return handler(ctx, request)
	}))
	for _, tc := range []struct {
		args     []string
		present  bool
		expected uint64
	}{
		{[]string{"--timeout=1s", "put", "key", "value"}, false, 0},
		{[]string{"put", "--expected-rev=0", "key", "value"}, true, 0},
		{[]string{"put", "key", "value", "--expected-rev=7"}, true, 7},
	} {
		var out, stderr bytes.Buffer
		if Run(tc.args, forbiddenReader{}, &out, &stderr) != 0 {
			t.Fatal(stderr.String())
		}
		request := <-requests
		if (request.ExpectedRev != nil) != tc.present || request.GetExpectedRev() != tc.expected {
			t.Fatalf("wrong optional presence: %v", request)
		}
	}
}
