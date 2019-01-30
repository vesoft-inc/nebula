/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_QUERYVERTEXPROPSPROCESSOR_H_
#define STORAGE_QUERYVERTEXPROPSPROCESSOR_H_

#include "base/Base.h"
#include "storage/QueryBoundProcessor.h"

namespace nebula {
namespace storage {

class QueryVertexPropsProcessor : public QueryBoundProcessor {
public:
    static QueryVertexPropsProcessor* instance(kvstore::KVStore* kvstore) {
        return new QueryVertexPropsProcessor(kvstore);
    }

    void process(const cpp2::VertexPropRequest& req);

private:
    explicit QueryVertexPropsProcessor(kvstore::KVStore* kvstore)
        : QueryBoundProcessor(kvstore) {}
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERYVERTEXPROPSPROCESSOR_H_
