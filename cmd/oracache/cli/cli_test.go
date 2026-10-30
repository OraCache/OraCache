package cli

import (
	"bytes"
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

func TestCLI(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	pb.RegisterCacheGatewayServer(s, cache.New())
	go s.Serve(l)
	t.Cleanup(s.Stop)
	t.Setenv("ORACACHE_ENDPOINT", l.Addr().String())
	call := func(input string, args ...string) map[string]any {
		t.Helper()
		var out, stderr bytes.Buffer
		if code := Run(args, strings.NewReader(input), &out, &stderr); code != 0 {
			t.Fatalf("%v: code=%d %s", args, code, stderr.String())
		}
		var v map[string]any
		if err := json.Unmarshal(out.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	if v := call("", "get", "k"); v["status"] != "GET_STATUS_MISS" {
		t.Fatal(v)
	}
	if v := call("hello\x00", "put", "--expected-rev=0", "k"); v["rev"] != "1" {
		t.Fatal(v)
	}
	if v := call("", "get", "--rev=1", "k"); v["data"] != base64.StdEncoding.EncodeToString([]byte("hello\x00")) {
		t.Fatal(v)
	}
	if v := call("no", "put", "--expected-rev=0", "k"); v["status"] != "PUT_STATUS_CONFLICT" {
		t.Fatal(v)
	}
	file := filepath.Join(t.TempDir(), "payload")
	if err := os.WriteFile(file, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	if v := call("", "put", "--file", file, "k"); v["rev"] != "2" {
		t.Fatal(v)
	}
	if v := call("", "delete", "--rev=2", "k"); v["status"] != "DELETE_STATUS_DELETED" {
		t.Fatal(v)
	}
	if v := call("", "get", "k"); v["rev"] != "1" {
		t.Fatal(v)
	}
	call("", "delete", "k")
	if v := call("", "get", "k"); v["status"] != "GET_STATUS_MISS" {
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
