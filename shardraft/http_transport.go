package shardraft

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HTTPTransport sends Raft messages to fixed URLs. The host can use HTTPS and
// configure authentication on its supplied http.Client/server. The PoC provides
// no peer authentication; group IDs are routing checks, not credentials.
type HTTPTransport struct {
	addresses map[uint64]string
	client    *http.Client
}

func NewHTTPTransport(addresses map[uint64]string, client *http.Client) (*HTTPTransport, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("Raft endpoints are required")
	}
	copyOfAddresses := make(map[uint64]string, len(addresses))
	for id, address := range addresses {
		u, err := url.Parse(address)
		if err != nil || id == 0 || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("invalid Raft endpoint for peer %d", id)
		}
		copyOfAddresses[id] = address
	}
	if client == nil {
		client = &http.Client{}
	}
	return &HTTPTransport{addresses: copyOfAddresses, client: client}, nil
}

func (t *HTTPTransport) Send(ctx context.Context, group string, to uint64, data []byte) error {
	address, exists := t.addresses[to]
	if !exists {
		return fmt.Errorf("unknown peer %d", to)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, address, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Oracache-Raft-Group", group)
	response, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("peer %d returned HTTP %d", to, response.StatusCode)
	}
	return nil
}

// RaftHandler is mounted by the host at the exact URLs passed to HTTPTransport.
// It does not create or own a listener. Incoming payloads are size-limited.
func (n *Instance) RaftHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxWireBytes)
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid message body", http.StatusRequestEntityTooLarge)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), n.cfg.SendTimeout)
		defer cancel()
		if err := n.Step(ctx, r.Header.Get("X-Oracache-Raft-Group"), data); err != nil {
			code := http.StatusServiceUnavailable
			if status.Code(err) == codes.InvalidArgument {
				code = http.StatusBadRequest
			}
			http.Error(w, err.Error(), code)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}
