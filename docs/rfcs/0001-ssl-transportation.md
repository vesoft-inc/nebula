# Summary

Introduce the SSL/TLS protocol for secure data transport.

# Motivation

Crypto the data transportation.

# Usage explanation

Define some global flags:

```c++
DEFINE_string(cert_path, "", "Path to cert pem.");
DEFINE_string(key_path, "", "Path to cert key.");
DEFINE_string(ca_path, "", "Path to trusted CA file.");
DEFINE_string(password_path, "", "Path to password file.");
DEFINE_bool(enable_ssl, false, "Wether enable ssl.");

DEFINE_bool(enable_graph_ssl, false, "Wether enable ssl in graph server only.");
```

And provide two mode:

1. Crypto the whole data transportation in the cluster when the flag `enable_ssl` is set to `true`. In this mode, we enable SSL transportation for follow components of daemon:

```
graphd: meta client + storage client + graph server

metad: admin storage client + metad server + raft client + raft server

storaged: meta client + internal storage client + internal storage server + graph storage server + admin storage server + raft client + raft server
```

2. Crypto the graph server only when the flag `enable_ssl` is set to `false` and `enable_graph_ssl` is set to `true`. In this mode, we enable SSL transportation for follow components of daemon:

```
graphd: graph server
```

This mode is optimized for the case that the cluster setup in one room and communicate over the internal network, and only graph server expose the interface to outside.

# Design explanation

Enable the thrift provided SSL/TLS function.

In server side, setup certificate and private key. In client side, provide the SSL/TLS transport socket.

For clients, these will be done by corresponding maintainer self.

# Rationale and alternatives

# Drawbacks

Extra performance cost.

# Prior art

# Unresolved questions

The thrift support the dual plaintext/SSL mode by default. The enable plaintext client communicate with SSL/TLS server in plaintext protocol. And it's unable to configure.

# Future possibilities
