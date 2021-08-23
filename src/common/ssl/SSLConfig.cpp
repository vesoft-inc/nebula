/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/ssl/SSLConfig.h"

DEFINE_string(cert_path, "", "Path to cert pem.");
DEFINE_string(key_path, "", "Path to cert key.");
DEFINE_string(ca_path, "", "Path to trusted CA file.");
DEFINE_string(password_path, "", "Path to password file.");
DEFINE_bool(enable_ssl, false, "Wether enable ssl.");
DEFINE_bool(enable_graph_ssl, false, "Wether enable ssl.");

namespace nebula {

std::shared_ptr<wangle::SSLContextConfig> sslContextConfig() {
  auto sslCfg = std::make_shared<wangle::SSLContextConfig>();
  sslCfg->addCertificate(FLAGS_cert_path, FLAGS_key_path, FLAGS_password_path);
  sslCfg->clientCAFile = FLAGS_ca_path;
  sslCfg->isDefault = true;
  return sslCfg;
}

std::shared_ptr<folly::SSLContext> createSSLContext() {
  auto context = std::make_shared<folly::SSLContext>();
  folly::ssl::setSignatureAlgorithms<folly::ssl::SSLCommonOptions>(*context);
  return context;
}

}  // namespace nebula
