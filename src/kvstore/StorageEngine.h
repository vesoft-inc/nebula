/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef KVSTORE_STORAGEENGINE_H_
#define KVSTORE_STORAGEENGINE_H_

#include "base/Base.h"
#include "kvstore/include/Iterator.h"
#include "kvstore/include/ResultCode.h"

namespace nebula {
namespace kvstore {

class StorageEngine {
public:
    explicit StorageEngine(GraphSpaceID spaceId)
                : spaceId_(spaceId) {}

    virtual ~StorageEngine() = default;

    virtual ResultCode get(const std::string& key,
                           std::string* value) = 0;

    virtual ResultCode put(std::string key,
                           std::string value) = 0;

    virtual ResultCode multiPut(std::vector<KV> keyValues) = 0;
    /**
     * Get all results in range [start, end)
     * */
    virtual ResultCode range(const std::string& start,
                             const std::string& end,
                             std::unique_ptr<StorageIter>* iter) = 0;

    /**
     * Get all results with 'prefix' str as prefix.
     * */
    virtual ResultCode prefix(const std::string& prefix,
                              std::unique_ptr<StorageIter>* iter) = 0;

    virtual ResultCode remove(const std::string& key) = 0;

    /**
     * Remove range [start, end)
     * */
    virtual ResultCode removeRange(const std::string& start,
                                   const std::string& end) = 0;

protected:
    GraphSpaceID spaceId_;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_STORAGEENGINE_H_

