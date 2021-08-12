/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VERSION_VERSION_H_
#define VERSION_VERSION_H_

#include <string>

namespace nebula {

std::string gitInfoSha();
std::string versionString(bool verbose = true);

}  // namespace nebula

#endif  // VERSION_VERSION_H_
