/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/ssl/SSLConfig.h"

DEFINE_string(cert_path, "", "Path to cert pem.");
DEFINE_string(key_path, "", "Path to cert key.");
DEFINE_string(password_path, "", "Path to password.");
DEFINE_string(ca_path, "", "Path to trusted CA file.");
DEFINE_bool(enable_ssl, false, "Whether to enable ssl.");
DEFINE_bool(enable_graph_ssl, false, "Whether to enable ssl of graph server.");
DEFINE_bool(enable_meta_ssl, false, "Whether to enable ssl of meta server.");

namespace nebula {

std::shared_ptr<wangle::SSLContextConfig> sslContextConfig() {
  auto sslCfg = std::make_shared<wangle::SSLContextConfig>();
  sslCfg->addCertificate(FLAGS_cert_path, FLAGS_key_path, FLAGS_password_path);
  sslCfg->clientVerification = folly::SSLContext::VerifyClientCertificate::DO_NOT_REQUEST;
  sslCfg->isDefault = true;
  return sslCfg;
}

std::shared_ptr<folly::SSLContext> createSSLContext() {
  auto context = std::make_shared<folly::SSLContext>();
  if (!FLAGS_ca_path.empty()) {
    context->loadTrustedCertificates(FLAGS_ca_path.c_str());
    // don't do peer name validation
    context->authenticate(true, false);
    // verify the server cert
    context->setVerificationOption(folly::SSLContext::SSLVerifyPeerEnum::VERIFY);
  }
  folly::ssl::setSignatureAlgorithms<folly::ssl::SSLCommonOptions>(*context);
  return context;
}

}  // namespace nebula
