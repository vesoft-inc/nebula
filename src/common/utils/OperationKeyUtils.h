/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_OPERATIONKEYUTILS_H_
#define COMMON_BASE_OPERATIONKEYUTILS_H_

#include "utils/Types.h"

namespace nebula {

/**
 * The OperationKeyUtils is use to generate the operation key when rebuilding index. 
 * */
class OperationKeyUtils final {
public:
    ~OperationKeyUtils() = default;

    static std::string modifyOperationKey(PartitionID part, const std::string& key);

    static std::string deleteOperationKey(PartitionID part);

    static bool isModifyOperation(const folly::StringPiece& rawKey);

    static bool isDeleteOperation(const folly::StringPiece& rawKey);

    static std::string getOperationKey(const folly::StringPiece& rawValue);

    static std::string operationPrefix(PartitionID part);

private:
    OperationKeyUtils() = delete;
};

}  // namespace nebula

#endif  // COMMON_BASE_OPERATIONKEYUTILS_H_
