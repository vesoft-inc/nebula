/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_HOSTANDPATH_H_
#define COMMON_DATATYPES_HOSTANDPATH_H_

#include <sstream>

#include "common/datatypes/HostAddr.h"

namespace nebula {

struct HostAndPath {
  HostAddr host;
  std::string path;

  HostAndPath() : host(), path("") {}

  HostAndPath(const HostAndPath& hp) : host(hp.host), path(hp.path) {}

  HostAndPath(HostAddr h, std::string p) : host(std::move(h)), path(std::move(p)) {}

  HostAndPath& operator=(const HostAndPath& hostAndPath);

  void clear() {
    host.clear();
    path.clear();
  }

  void __clear() { clear(); }

  std::string toString() const {
    std::stringstream os;
    os << host.toString() << ":" << path;
    return os.str();
  }

  bool operator==(const HostAndPath& rhs) const;

  bool operator!=(const HostAndPath& rhs) const;

  bool operator<(const HostAndPath& rhs) const;
};

inline std::ostream& operator<<(std::ostream& os, const HostAndPath& hostAndPath) {
  return os << hostAndPath.toString();
}

}  // namespace nebula

#endif  // COMMON_DATATYPES_HOSTANDPATH_H_
