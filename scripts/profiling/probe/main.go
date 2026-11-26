// Command probe copies a container-loopback diagnostics response to stdout.
// This lets a scratch image export binary profiles without publishing pprof.
package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 2 || (args[0] != "GET" && args[0] != "POST") {
		return fmt.Errorf("usage: probe GET|POST http://127.0.0.1:port/path")
	}
	u, err := url.Parse(args[1])
	if err != nil || u.Scheme != "http" || !net.ParseIP(u.Hostname()).IsLoopback() {
		return fmt.Errorf("expected a loopback HTTP URL")
	}
	req, err := http.NewRequest(args[0], u.String(), nil)
	if err != nil {
		return err
	}
	client := http.Client{Timeout: 3 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("HTTP %s", resp.Status)
	}
	_, err = io.Copy(os.Stdout, resp.Body)
	return err
}
