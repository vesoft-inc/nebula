
/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_SSL_SSLCONFIG_H
#define COMMON_SSL_SSLCONFIG_H

#include <folly/io/async/SSLContext.h>
#include <gflags/gflags.h>  // for DECLARE_bool
#include <wangle/ssl/SSLContextConfig.h>

#include <memory>  // for shared_ptr

namespace wangle {
struct SSLContextConfig;
}  // namespace wangle

namespace folly {
class SSLContext;

class SSLContext;
}  // namespace folly
namespace wangle {
struct SSLContextConfig;
}  // namespace wangle

DECLARE_bool(enable_ssl);
DECLARE_bool(enable_graph_ssl);
DECLARE_bool(enable_meta_ssl);

namespace nebula {

extern std::shared_ptr<wangle::SSLContextConfig> sslContextConfig();

extern std::shared_ptr<folly::SSLContext> createSSLContext();

}  // namespace nebula
#endif
