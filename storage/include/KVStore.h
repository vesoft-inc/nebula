/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_INCLUDE_KVSTORE_H_
#define STORAGE_INCLUDE_KVSTORE_H_

#include "base/Base.h"
#include "storage/include/ResultCode.h"
#include "storage/include/Iterator.h"

namespace vesoft {
namespace vgraph {
namespace storage {


using KVCallback = std::function<void(ResultCode code, HostAddr hostAddr)>;

class KVStore {
public:
	static KVStore* instance(HostAddr local, std::vector<std::string> paths);

    virtual ~KVStore() = default;
 
    virtual ResultCode get(GraphSpaceID spaceId,
				   		   PartitionID  partId,
                           const std::string& key,
                           std::string& value) = 0;
    /**
     * Get all results in range [start, end)
     * */
    virtual ResultCode range(GraphSpaceID spaceId,
				   			 PartitionID  partId,
                             const std::string& start,
                             const std::string& end,
                             std::unique_ptr<StorageIter>& iter) = 0;

    /**
     * Get all results with prefix.
     * */
    virtual ResultCode prefix(GraphSpaceID spaceId,
				   			  PartitionID  partId,
                              const std::string& prefix,
                              std::unique_ptr<StorageIter>& iter) = 0;


    virtual ResultCode asyncMultiPut(GraphSpaceID spaceId,
				   				     PartitionID  partId,
									 std::vector<KV> keyValues,
									 KVCallback cb) = 0;

protected:
	KVStore() = default;

private:
	static KVStore* instance_;
};

}  // namespace storage
}  // namespace vgraph
}  // namespace vesoft
#endif  // STORAGE_INCLUDE_KVSTORE_H_

