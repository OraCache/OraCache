// Package cplink configures authenticated connections on the Control Plane.
package cplink

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
)

const ClusterHeader = "oracache-cluster-id"

func Cluster() string { return os.Getenv("ORACACHE_CP_CLUSTER_ID") }

// TLSFromEnv configures a dedicated trust root and an mTLS identity.
// An entirely unset profile selects the legacy plaintext connection.
func TLSFromEnv(prefix string, server bool) (*tls.Config, error) {
	ca, cert, key := os.Getenv(prefix+"_TLS_CA"), os.Getenv(prefix+"_TLS_CERT"), os.Getenv(prefix+"_TLS_KEY")
	if ca == "" && cert == "" && key == "" {
		return nil, nil
	}
	if ca == "" || cert == "" || key == "" {
		return nil, fmt.Errorf("%s TLS requires CA, CERT and KEY", prefix)
	}
	pem, err := os.ReadFile(ca)
	if err != nil {
		return nil, fmt.Errorf("read %s CA: %w", prefix, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s CA contains no certificates", prefix)
	}
	identity, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, fmt.Errorf("load %s identity: %w", prefix, err)
	}
	config := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{identity}}
	if server {
		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = roots
	} else {
		config.RootCAs = roots
	}
	return config, nil
}

// DiskFromEnv returns the optional checkpoint for this process.
func DiskFromEnv(region, client string) (*cpconfig.Disk, error) {
	path := os.Getenv("ORACACHE_CP_SNAPSHOT_FILE")
	if path == "" {
		return nil, nil
	}
	cluster := Cluster()
	if cluster == "" {
		return nil, errors.New("snapshot requires ORACACHE_CP_CLUSTER_ID")
	}
	age := time.Hour
	if raw := os.Getenv("ORACACHE_CP_SNAPSHOT_MAX_AGE"); raw != "" {
		value, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("parse snapshot max age: %w", err)
		}
		if value <= 0 {
			return nil, errors.New("snapshot max age must be positive")
		}
		age = value
	}
	return &cpconfig.Disk{Path: path, Cluster: cluster, Region: region, Client: client, MaxAge: age}, nil
}
