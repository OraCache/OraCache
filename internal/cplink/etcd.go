package cplink

import (
	"errors"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func DialEtcd(addresses string) (*clientv3.Client, error) {
	config, err := TLSFromEnv("ORACACHE_ETCD", false)
	if err != nil {
		return nil, err
	}
	endpoints := strings.Split(addresses, ",")
	for i, endpoint := range endpoints {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			return nil, errors.New("etcd endpoint must not be empty")
		}
		if config != nil && !strings.HasPrefix(endpoint, "https://") {
			return nil, errors.New("etcd mTLS requires https endpoints")
		}
		endpoints[i] = endpoint
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, TLS: config, DialTimeout: 3 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		return nil, fmt.Errorf("open etcd client: %w", err)
	}
	return client, nil
}
