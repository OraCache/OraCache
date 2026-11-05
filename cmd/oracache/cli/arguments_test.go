package cli

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type forbiddenReader struct{}

func (forbiddenReader) Read([]byte) (int, error) { panic("unexpected stdin read") }

type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) { return 0, errors.New("input broken") }

func TestHelpWithoutEndpointOrInput(t *testing.T) {
	t.Setenv("ORACACHE_ENDPOINT", "")
	for _, args := range [][]string{{"--help"}, {"get", "--help"}, {"put", "--help"}, {"delete", "--help"}} {
		var out, err bytes.Buffer
		if code := Run(args, forbiddenReader{}, &out, &err); code != 0 {
			t.Fatalf("%v: %d %s", args, code, err.String())
		}
		if !strings.Contains(out.String(), "Usage:") || !strings.Contains(out.String(), "--help") {
			t.Fatalf("not human help: %q", out.String())
		}
	}
}

func TestInvalidArgumentsNeverReadInput(t *testing.T) {
	t.Setenv("ORACACHE_ENDPOINT", "127.0.0.1:1")
	for _, args := range [][]string{
		{"put", "key"}, {"put", "", "@-"}, {"put", "key", "@-", "extra"},
		{"put", "--file=x", "key"}, {"put", "key", "@-", "--timeout=0s"},
		{"put", "key", "@-", "--timeout=-1s"}, {"put", "key", "@-", "--expected-rev=-1"},
	} {
		var out, err bytes.Buffer
		if Run(args, forbiddenReader{}, &out, &err) == 0 {
			t.Fatalf("accepted %v", args)
		}
	}
	t.Setenv("ORACACHE_ENDPOINT", "")
	var out, err bytes.Buffer
	if Run([]string{"put", "key", "@-"}, forbiddenReader{}, &out, &err) == 0 || !strings.Contains(err.String(), "ORACACHE_ENDPOINT") {
		t.Fatal(err.String())
	}
}

func TestPayloadErrorsBeforeRPC(t *testing.T) {
	t.Setenv("ORACACHE_ENDPOINT", "127.0.0.1:1")
	largeFile := filepath.Join(t.TempDir(), "large")
	if err := os.WriteFile(largeFile, bytes.Repeat([]byte{'x'}, 10241), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, value, want string
		input             io.Reader
	}{
		{"literal limit", strings.Repeat("x", 10241), "payload exceeds", forbiddenReader{}},
		{"stdin limit", "@-", "payload exceeds", strings.NewReader(strings.Repeat("x", 10241))},
		{"file limit", "@" + largeFile, "payload exceeds", forbiddenReader{}},
		{"missing file", "@" + filepath.Join(t.TempDir(), "missing"), "no such file", forbiddenReader{}},
		{"stdin error", "@-", "input broken", brokenReader{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, err bytes.Buffer
			if Run([]string{"put", "key", tc.value}, tc.input, &out, &err) == 0 || !strings.Contains(err.String(), tc.want) {
				t.Fatalf("%s", err.String())
			}
		})
	}
}
