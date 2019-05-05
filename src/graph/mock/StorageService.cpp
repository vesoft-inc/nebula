/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "graph/mock/StorageService.h"

namespace nebula {
namespace graph {

StorageService::StorageService(SchemaManager *sm) {
    pool_ = std::make_unique<thread::GenericThreadPool>();
    pool_->start(4, "storage-svc");
    sm_ = sm;
}


folly::SemiFuture<Status>
StorageService::addTag(const std::string *tag,
                       int64_t id,
                       const std::vector<std::string*> &names,
                       const std::vector<VariantType> &values) {
    auto task = [this, tag, id, names, values] () -> Status {
        auto *schema = sm_->getTagSchema(*tag);
        if (schema == nullptr) {
            return Status::Error("Tag `%s' not defined", tag->c_str());
        }
        TagKey key;
        key.id_ = id;
        key.type_ = schema->id();
        auto &props = tags_[key];
        for (auto i = 0u; i < names.size(); i++) {
            props[*names[i]] = values[i];
        }
        return Status::OK();
    };
    return pool_->addTask(std::move(task));
}


folly::SemiFuture<Status>
StorageService::addEdge(const std::string *edge,
                        int64_t srcid, int64_t dstid,
                        const std::vector<std::string*> &names,
                        const std::vector<VariantType> &values) {
    auto task = [this, edge, srcid, dstid, names, values] () -> Status {
        auto *schema = sm_->getEdgeSchema(*edge);
        if (schema == nullptr) {
            return Status::Error("Edge `%s' not defined", edge->c_str());
        }
        EdgeKey key;
        key.srcid_ = srcid;
        key.dstid_ = dstid;
        key.type_ = schema->id();
        auto  &props = edges_[key];
        for (auto i = 0u; i < names.size(); i++) {
            props[*names[i]] = values[i];
        }
        return Status::OK();
    };

    return pool_->addTask(std::move(task));
}

folly::SemiFuture<StorageService::OutBoundResult>
StorageService::getOutBound(const std::vector<int64_t> &ids,
                            const std::string *edge,
                            const std::vector<std::string> &eprops,
                            const std::vector<std::string> &vprops) {
    auto task = [this, ids, edge, eprops, vprops] () -> OutBoundResult {
        OutBoundResult result;
        auto *schema = sm_->getEdgeSchema(*edge);
        if (schema == nullptr) {
            return Status::Error("Edge `%s' not defined", edge->c_str());
        }
        // storage::cpp2::QueryResponse resp;
        for (auto id : ids) {
            EdgeKey start, end;
            start.srcid_ = id;
            start.type_ = schema->id();
            start.dstid_ = -1;
            end.srcid_ = id;
            end.type_ = start.type_ + 1;
            end.dstid_ = -1;
            auto left = edges_.lower_bound(start);
            auto right = edges_.lower_bound(end);
            for (auto iter = left; iter != right; ++iter) {
                if (iter->first.srcid_ != id) {
                    break;
                }
                if (iter->first.type_ != start.type_) {
                    break;
                }
                // auto &properties = iter->second;
                FLOG_INFO("%ld -%s-> %ld", id, edge->c_str(), iter->first.dstid_);
            }
        }
        return result;
    };

    return pool_->addTask(std::move(task));
}

}   // namespace graph
}   // namespace nebula
