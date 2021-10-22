/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/datatypes/HostAndPath.h"

namespace nebula {

bool HostAndPath::operator==(const HostAndPath& rhs) const {
  return host == rhs.host && path == rhs.path;
}

bool HostAndPath::operator!=(const HostAndPath& rhs) const {
  return !(*this == rhs);
}

bool HostAndPath::operator<(const HostAndPath& rhs) const {
  if (host == rhs.host) {
    return path < rhs.path;
  }
  return host < rhs.host;
}

HostAndPath& HostAndPath::operator=(const HostAndPath& hostAndPath) {
  if (&hostAndPath != this) {
    host = hostAndPath.host;
    path = hostAndPath.path;
  }
  return *this;
}

}  // namespace nebula
