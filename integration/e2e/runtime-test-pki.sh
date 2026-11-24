#!/bin/sh
# Disposable certificates only. The fixture shares the directory with test containers.
set -eu
cd "${1:?output directory required}"
chmod 755 .
for profile in cp etcd other; do
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
    -subj "/CN=OraCache test $profile" -keyout "$profile-ca.key" -out "$profile-ca.pem"
done
issue() {
  name=$1 profile=$2 usage=$3 san=$4
  openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -subj "/CN=$name" -keyout "$name.key" -out "$name.csr"
  printf 'extendedKeyUsage=%s\nsubjectAltName=%s\n' "$usage" "$san" > leaf.ext
  openssl x509 -req -in "$name.csr" -days 1 -CA "$profile-ca.pem" -CAkey "$profile-ca.key" \
    -CAcreateserial -extfile leaf.ext -out "$name.pem"
  chmod 644 "$name.key"
}
issue cp-server cp serverAuth DNS:cp-region-a,DNS:cp-region-b,IP:127.0.0.1
issue etcd-server etcd serverAuth,clientAuth DNS:etcd-1,DNS:etcd-2,DNS:etcd-3,IP:127.0.0.1
issue cp-etcd etcd clientAuth DNS:cp-etcd
issue gw-local cp clientAuth URI:spiffe://runtime-test/gateway/region-a/gw-local
issue untrusted other clientAuth URI:spiffe://runtime-test/node/region-a/node-1
for number in 1 2 3; do
  issue "node-$number" cp clientAuth "URI:spiffe://runtime-test/node/region-a/node-$number"
done
rm -f -- *-ca.key ./*.csr ./*.srl leaf.ext
