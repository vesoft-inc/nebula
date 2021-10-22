
/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <folly/io/async/SSLContext.h>
#include <gflags/gflags.h>
#include <wangle/ssl/SSLContextConfig.h>

#include <memory>

DECLARE_bool(enable_ssl);
DECLARE_bool(enable_graph_ssl);
DECLARE_bool(enable_meta_ssl);

namespace nebula {

extern std::shared_ptr<wangle::SSLContextConfig> sslContextConfig();

extern std::shared_ptr<folly::SSLContext> createSSLContext();

}  // namespace nebula
