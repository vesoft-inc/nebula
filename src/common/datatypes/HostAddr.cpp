/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/datatypes/HostAddr.h"

#include <folly/hash/Hash.h>

#include "common/base/Base.h"

namespace nebula {

bool HostAddr::operator==(const HostAddr& rhs) const {
  return host == rhs.host && port == rhs.port;
}

bool HostAddr::operator!=(const HostAddr& rhs) const {
  return !(*this == rhs);
}

HostAddr& HostAddr::operator=(const HostAddr& rhs) {
  host = rhs.host;
  port = rhs.port;
  return *this;
}

bool HostAddr::operator<(const HostAddr& rhs) const {
  if (host == rhs.host) {
    return port < rhs.port;
  }
  return host < rhs.host;
}

}  // namespace nebula

namespace std {

// Inject a customized hash function
std::size_t hash<nebula::HostAddr>::operator()(const nebula::HostAddr& h) const noexcept {
  int64_t code = folly::hash::fnv32_buf(h.host.data(), h.host.size());
  code <<= 32;
  code |= folly::hash::fnv32_buf(&(h.port), sizeof(nebula::Port));
  return code;
}

}  // namespace std
