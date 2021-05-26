/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_MACOR_H_
#define COMMON_BASE_MACOR_H_

namespace nebula {

// for fixed_string data type, maximum of 256 bytes are allowed in the index.
// Full text index is similar.
#ifndef MAX_INDEX_TYPE_LENGTH
#define MAX_INDEX_TYPE_LENGTH 256
#endif

}  // namespace nebula

#endif  // COMMON_BASE_MACOR_H_
