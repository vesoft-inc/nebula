/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_MOCK_STORAGESERVICE_H_
#define GRAPH_MOCK_STORAGESERVICE_H_

#include "base/Base.h"
#include "base/Status.h"
#include "base/StatusOr.h"
#include "gen-cpp2/StorageService.h"
#include "thread/GenericThreadPool.h"
#include "graph/mock/SchemaManager.h"

namespace nebula {
namespace graph {

class StorageService {
public:
    explicit StorageService(SchemaManager *sm);

    folly::SemiFuture<Status> addTag(const std::string *tag,
                                     int64_t id,
                                     const std::vector<std::string*> &names,
                                     const std::vector<VariantType> &values);

    folly::SemiFuture<Status> addEdge(const std::string *edge,
                                      int64_t srcid, int64_t dstid,
                                      const std::vector<std::string*> &names,
                                      const std::vector<VariantType> &values);
    using OutBoundResult = StatusOr<storage::cpp2::QueryResponse>;
    folly::SemiFuture<OutBoundResult> getOutBound(const std::vector<int64_t> &ids,
                                                  const std::string *edge,
                                                  const std::vector<std::string> &eprops,
                                                  const std::vector<std::string> &vprops);

private:
    struct TagKey {
        int64_t     id_{0};
        int64_t     type_{0};
    };

    struct EdgeKey {
        int64_t     srcid_{0};
        int64_t     dstid_{0};
        int32_t     type_{0};
    };

    struct TagLess {
        bool operator()(const TagKey &lhs, const TagKey &rhs) const {
            if (lhs.id_ != rhs.id_) {
                return lhs.id_ < rhs.id_;
            }
            return lhs.type_ < rhs.type_;
        }
    };

    struct EdgeLess {
        bool operator()(const EdgeKey &lhs, const EdgeKey &rhs) const {
            if (lhs.srcid_ != rhs.srcid_) {
                return lhs.srcid_ < rhs.dstid_;
            }
            if (lhs.type_ != rhs.type_) {
                return lhs.type_ < rhs.type_;
            }
            return lhs.dstid_ < rhs.dstid_;
        }
    };

    using PropertiesType = std::unordered_map<std::string, VariantType>;
    using TagsType = std::map<TagKey, PropertiesType, TagLess>;
    using EdgesType = std::map<EdgeKey, PropertiesType, EdgeLess>;

private:
    TagsType                                    tags_;
    EdgesType                                   edges_;
    std::unique_ptr<thread::GenericThreadPool>  pool_;
    SchemaManager                              *sm_;
};

}   // namespace graph
}   // namespace nebula

#endif
