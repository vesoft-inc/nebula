/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "executor/admin/SpaceExecutor.h"
#include "context/QueryContext.h"
#include "service/PermissionManager.h"
#include "planner/plan/Admin.h"
#include "util/SchemaUtil.h"
#include "util/ScopedTimer.h"
#include "util/FTIndexUtils.h"

namespace nebula {
namespace graph {
folly::Future<Status> CreateSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *csNode = asNode<CreateSpace>(node());
    return qctx()->getMetaClient()->createSpace(csNode->getSpaceDesc(), csNode->getIfNotExists())
            .via(runner())
            .thenValue([](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}


folly::Future<Status> DescSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *dsNode = asNode<DescSpace>(node());
    return qctx()->getMetaClient()->getSpace(dsNode->getSpaceName())
            .via(runner())
            .thenValue([this](StatusOr<meta::cpp2::SpaceItem> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto &spaceItem = resp.value();
                auto &properties = spaceItem.get_properties();
                auto spaceId = spaceItem.get_space_id();

                // check permission
                auto *session = qctx_->rctx()->session();
                NG_RETURN_IF_ERROR(PermissionManager::canReadSpace(session, spaceId));

                DataSet dataSet;
                dataSet.colNames = {"ID",
                                    "Name",
                                    "Partition Number",
                                    "Replica Factor",
                                    "Charset",
                                    "Collate",
                                    "Vid Type",
                                    "Atomic Edge",
                                    "Group",
                                    "Comment"};
                Row row;
                row.values.emplace_back(spaceId);
                row.values.emplace_back(properties.get_space_name());
                row.values.emplace_back(properties.get_partition_num());
                row.values.emplace_back(properties.get_replica_factor());
                row.values.emplace_back(properties.get_charset_name());
                row.values.emplace_back(properties.get_collate_name());
                row.values.emplace_back(SchemaUtil::typeToString(properties.get_vid_type()));
                bool sAtomicEdge{false};
                if (properties.isolation_level_ref().has_value()  &&
                    (*properties.isolation_level_ref() == meta::cpp2::IsolationLevel::TOSS)) {
                    sAtomicEdge = true;
                }
                row.values.emplace_back(sAtomicEdge);
                if (properties.group_name_ref().has_value()) {
                    row.values.emplace_back(*properties.group_name_ref());
                } else {
                    row.values.emplace_back("default");
                }
                if (properties.comment_ref().has_value()) {
                    row.values.emplace_back(*properties.comment_ref());
                } else {
                    row.values.emplace_back();
                }
                dataSet.rows.emplace_back(std::move(row));
                return finish(ResultBuilder()
                                  .value(Value(std::move(dataSet)))
                                  .iter(Iterator::Kind::kDefault)
                                  .finish());
            });
}

folly::Future<Status> DropSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *dsNode = asNode<DropSpace>(node());

    // prepare text search index before drop meta data.
    std::vector<std::string> ftIndexes;
    auto spaceIdRet = qctx()->getMetaClient()->getSpaceIdByNameFromCache(dsNode->getSpaceName());
    if (spaceIdRet.ok()) {
        auto ftIndexesRet = qctx()->getMetaClient()->getFTIndexBySpaceFromCache(spaceIdRet.value());
        NG_RETURN_IF_ERROR(ftIndexesRet);
        auto map = std::move(ftIndexesRet).value();
        auto get = [] (const auto &ptr) { return ptr.first; };
        std::transform(map.begin(), map.end(), std::back_inserter(ftIndexes), get);
    } else {
        LOG(WARNING) << "Get space ID failed when prepare text index: " << dsNode->getSpaceName();
    }

    return qctx()->getMetaClient()->dropSpace(dsNode->getSpaceName(), dsNode->getIfExists())
            .via(runner())
            .thenValue([this, dsNode, spaceIdRet, ftIndexes](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Drop space `" << dsNode->getSpaceName()
                               << "' failed: " << resp.status();
                    return resp.status();
                }
                if (dsNode->getSpaceName() == qctx()->rctx()->session()->space().name) {
                    SpaceInfo spaceInfo;
                    spaceInfo.name = "";
                    spaceInfo.id = -1;
                    qctx()->rctx()->session()->setSpace(std::move(spaceInfo));
                }
                if (!ftIndexes.empty()) {
                    auto tsRet = FTIndexUtils::getTSClients(qctx()->getMetaClient());
                    if (!tsRet.ok()) {
                        LOG(WARNING) << "Get text search clients failed";
                        return Status::OK();
                    }
                    for (const auto& ftindex : ftIndexes) {
                        auto ftRet = FTIndexUtils::dropTSIndex(std::move(tsRet).value(), ftindex);
                        if (!ftRet.ok()) {
                            LOG(WARNING) << "Drop fulltext index `"
                                         << ftindex << "' failed: " << ftRet.status();
                        }
                    }
                }
                return Status::OK();
            });
}


folly::Future<Status> ShowSpacesExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    return qctx()->getMetaClient()->listSpaces().via(runner()).thenValue(
        [this](StatusOr<std::vector<meta::SpaceIdName>> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "Show spaces failed: " << resp.status();
                return resp.status();
            }
            auto spaceItems = std::move(resp).value();

            DataSet dataSet({"Name"});
            std::set<std::string> orderSpaceNames;
            for (auto &space : spaceItems) {
                if (!PermissionManager::canReadSpace(qctx_->rctx()->session(),
                                                     space.first).ok()) {
                    continue;
                }
                orderSpaceNames.emplace(space.second);
            }
            for (auto &name : orderSpaceNames) {
                Row row;
                row.values.emplace_back(name);
                dataSet.rows.emplace_back(std::move(row));
            }
            return finish(ResultBuilder()
                              .value(Value(std::move(dataSet)))
                              .iter(Iterator::Kind::kDefault)
                              .finish());
        });
}

folly::Future<Status> ShowCreateSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *scsNode = asNode<ShowCreateSpace>(node());
    return qctx()->getMetaClient()->getSpace(scsNode->getSpaceName())
            .via(runner())
            .thenValue([this, scsNode](StatusOr<meta::cpp2::SpaceItem> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "Show create space `" << scsNode->getSpaceName()
                               << "' failed: " << resp.status();
                    return resp.status();
                }
                auto properties = resp.value().get_properties();
                DataSet dataSet({"Space", "Create Space"});
                Row row;
                row.values.emplace_back(properties.get_space_name());
                std::string sAtomicEdge{"false"};
                if (properties.isolation_level_ref().has_value() &&
                    (*properties.isolation_level_ref() == meta::cpp2::IsolationLevel::TOSS)) {
                    sAtomicEdge = "true";
                }
                auto fmt = properties.comment_ref().has_value() ?
                           "CREATE SPACE `%s` (partition_num = %d, replica_factor = %d, "
                           "charset = %s, collate = %s, vid_type = %s, atomic_edge = %s"
                           ") ON %s"
                           " comment = '%s'" :
                           "CREATE SPACE `%s` (partition_num = %d, replica_factor = %d, "
                           "charset = %s, collate = %s, vid_type = %s, atomic_edge = %s"
                           ") ON %s";
                if (properties.comment_ref().has_value()) {
                    row.values.emplace_back(folly::stringPrintf(
                        fmt,
                        properties.get_space_name().c_str(),
                        properties.get_partition_num(),
                        properties.get_replica_factor(),
                        properties.get_charset_name().c_str(),
                        properties.get_collate_name().c_str(),
                        SchemaUtil::typeToString(properties.get_vid_type()).c_str(),
                        sAtomicEdge.c_str(),
                        properties.group_name_ref().has_value() ?
                            properties.get_group_name()->c_str() :
                            "default",
                        properties.comment_ref()->c_str()));
                } else {
                    row.values.emplace_back(folly::stringPrintf(
                        fmt,
                        properties.get_space_name().c_str(),
                        properties.get_partition_num(),
                        properties.get_replica_factor(),
                        properties.get_charset_name().c_str(),
                        properties.get_collate_name().c_str(),
                        SchemaUtil::typeToString(properties.get_vid_type()).c_str(),
                        sAtomicEdge.c_str(),
                        properties.group_name_ref().has_value() ?
                            properties.group_name_ref()->c_str() :
                            "default"));
                }
                dataSet.rows.emplace_back(std::move(row));
                return finish(ResultBuilder()
                                  .value(Value(std::move(dataSet)))
                                  .iter(Iterator::Kind::kDefault)
                                  .finish());
            });
}
}   // namespace graph
}   // namespace nebula
