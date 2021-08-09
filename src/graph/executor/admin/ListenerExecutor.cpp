/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "executor/admin/ListenerExecutor.h"
#include "planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> AddListenerExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *alNode = asNode<AddListener>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->addListener(spaceId, alNode->type(), alNode->hosts())
        .via(runner())
        .thenValue([this](StatusOr<bool> resp) {
            SCOPED_TIMER(&execTime_);
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            return Status::OK();
        });
}

folly::Future<Status> RemoveListenerExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *rlNode = asNode<RemoveListener>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->removeListener(spaceId, rlNode->type())
        .via(runner())
        .thenValue([this](StatusOr<bool> resp) {
            SCOPED_TIMER(&execTime_);
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            return Status::OK();
        });
}

folly::Future<Status> ShowListenerExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->listListener(spaceId)
        .via(runner())
        .thenValue([this](StatusOr<std::vector<meta::cpp2::ListenerInfo>> resp) {
            SCOPED_TIMER(&execTime_);
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }

            auto listenerInfos = std::move(resp).value();
            std::sort(listenerInfos.begin(), listenerInfos.end(), [](const auto& a, const auto& b) {
                if (a.part_id_ref() != b.part_id_ref()) {
                    return a.part_id_ref() < b.part_id_ref();
                }
                if (a.type_ref() != b.type_ref()) {
                    return a.type_ref() < b.type_ref();
                }
                return a.host_ref() < b.host_ref();
            });

            DataSet result({"PartId", "Type", "Host", "Status"});
            for (const auto& info : listenerInfos) {
                Row row;
                row.values.emplace_back(info.get_part_id());
                row.values.emplace_back(
                    apache::thrift::util::enumNameSafe(info.get_type()));
                row.values.emplace_back((*info.host_ref()).toString());
                row.values.emplace_back(
                    apache::thrift::util::enumNameSafe(info.get_status()));
                result.emplace_back(std::move(row));
            }

            return finish(std::move(result));
        });
}

}   // namespace graph
}   // namespace nebula
