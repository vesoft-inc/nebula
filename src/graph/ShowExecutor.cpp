/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/ShowExecutor.h"
#include "network/NetworkUtils.h"

namespace nebula {
namespace graph {

using nebula::network::NetworkUtils;

ShowExecutor::ShowExecutor(Sentence *sentence,
                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<ShowSentence*>(sentence);
}


Status ShowExecutor::prepare() {
    return Status::OK();
}


void ShowExecutor::execute() {
    if (sentence_->showType() == ShowSentence::ShowType::kShowParts ||
        sentence_->showType() == ShowSentence::ShowType::kShowTags ||
        sentence_->showType() == ShowSentence::ShowType::kShowEdges ||
        sentence_->showType() == ShowSentence::ShowType::kShowCreateTag ||
        sentence_->showType() == ShowSentence::ShowType::kShowCreateEdge) {
        auto status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            DCHECK(onError_);
            onError_(std::move(status));
            return;
        }
    }
    auto showType = sentence_->showType();
    switch (showType) {
        case ShowSentence::ShowType::kShowHosts:
            showHosts();
            break;
        case ShowSentence::ShowType::kShowSpaces:
            showSpaces();
            break;
        case ShowSentence::ShowType::kShowParts:
            showParts();
            break;
        case ShowSentence::ShowType::kShowTags:
            showTags();
            break;
        case ShowSentence::ShowType::kShowEdges:
            showEdges();
            break;
        case ShowSentence::ShowType::kShowUsers:
        case ShowSentence::ShowType::kShowUser:
        case ShowSentence::ShowType::kShowRoles:
            // TODO(boshengchen)
            break;
        case ShowSentence::ShowType::kShowCreateSpace:
            showCreateSpace();
            break;
        case ShowSentence::ShowType::kShowCreateTag:
            showCreateTag();
            break;
        case ShowSentence::ShowType::kShowCreateEdge:
            showCreateEdge();
            break;
        case ShowSentence::ShowType::kShowSnapshots:
            showSnapshots();
            break;
        case ShowSentence::ShowType::kUnknown:
            onError_(Status::Error("Type unknown"));
            break;
        // intentionally no `default'
    }
}


void ShowExecutor::showHosts() {
    auto future = ectx()->getMetaClient()->listHosts();
    auto *runner = ectx()->rctx()->runner();
    constexpr static char kNoValidPart[] = "No valid partition";

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        auto hostItems = std::move(resp).value();
        std::vector<cpp2::RowValue> rows;
        std::vector<std::string> header{"Ip", "Port", "Status", "Leader count",
                                        "Leader distribution", "Partition distribution"};
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(header));
        std::sort(hostItems.begin(), hostItems.end(), [](const auto& a, const auto& b) {
            // sort with online/offline and ip
            if (a.get_status() == b.get_status()) {
                return a.hostAddr.ip < b.hostAddr.ip;
            }
            return a.get_status() < b.get_status();
        });

        for (auto& item : hostItems) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(6);
            auto hostAddr = HostAddr(item.hostAddr.ip, item.hostAddr.port);
            row[0].set_str(NetworkUtils::ipFromHostAddr(hostAddr));
            row[1].set_str(folly::to<std::string>(NetworkUtils::portFromHostAddr(hostAddr)));
            switch (item.get_status()) {
                case meta::cpp2::HostStatus::ONLINE:
                    row[2].set_str("online");
                    break;
                case meta::cpp2::HostStatus::OFFLINE:
                case meta::cpp2::HostStatus::UNKNOWN:
                    row[2].set_str("offline");
                    break;
            }

            int32_t leaderCount = 0;
            std::string leaders;
            for (auto& spaceEntry : item.get_leader_parts()) {
                leaderCount += spaceEntry.second.size();
                leaders += spaceEntry.first + ": " +
                           folly::to<std::string>(spaceEntry.second.size()) + ", ";
            }
            if (!leaders.empty()) {
                leaders.resize(leaders.size() - 2);
            }

            row[3].set_integer(leaderCount);

            std::string parts;
            for (auto& spaceEntry : item.get_all_parts()) {
                parts += spaceEntry.first + ": " +
                         folly::to<std::string>(spaceEntry.second.size()) + ", ";
            }
            if (!parts.empty()) {
                parts.resize(parts.size() - 2);
            } else {
                // if there is no valid parition on a host at all
                leaders = kNoValidPart;
                parts = kNoValidPart;
            }
            row[4].set_str(leaders);
            row[5].set_str(parts);

            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }
        resp_->set_rows(std::move(rows));

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error(folly::stringPrintf("Internal error : %s",
                                                   e.what().c_str())));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void ShowExecutor::showSpaces() {
    auto future = ectx()->getMetaClient()->listSpaces();
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        auto retShowSpaces = std::move(resp).value();
        std::vector<cpp2::RowValue> rows;
        std::vector<std::string> header{"Name"};
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(header));

        for (auto &space : retShowSpaces) {
            std::vector<cpp2::ColumnValue> row;
            row.emplace_back();
            row.back().set_str(std::move(space.second));
            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }
        resp_->set_rows(std::move(rows));

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error(folly::stringPrintf("Internal error : %s",
                                                   e.what().c_str())));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void ShowExecutor::showParts() {
    auto spaceId = ectx()->rctx()->session()->space();
    auto future = ectx()->getMetaClient()->listParts(spaceId);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        auto partItems = std::move(resp).value();
        std::vector<cpp2::RowValue> rows;
        std::vector<std::string> header{"Partition ID", "Leader", "Peers", "Losts"};
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(header));

        std::sort(partItems.begin(), partItems.end(),
            [] (const auto& a, const auto& b) {
                return a.get_part_id() < b.get_part_id();
            });

        for (auto& item : partItems) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(4);
            row[0].set_integer(item.get_part_id());

            if (item.__isset.leader) {
                auto leader = item.get_leader();
                std::vector<HostAddr> leaders = {{leader->ip, leader->port}};
                std::string leaderStr = NetworkUtils::toHosts(leaders);
                row[1].set_str(leaderStr);
            } else {
                row[1].set_str("");
            }

            std::vector<HostAddr> peers;
            for (auto& peer : item.get_peers()) {
                peers.emplace_back(peer.ip, peer.port);
            }
            std::string peersStr = NetworkUtils::toHosts(peers);
            row[2].set_str(peersStr);

            std::vector<HostAddr> losts;
            for (auto& lost : item.get_losts()) {
                losts.emplace_back(lost.ip, lost.port);
            }
            std::string lostsStr = NetworkUtils::toHosts(losts);
            row[3].set_str(lostsStr);

            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }
        resp_->set_rows(std::move(rows));

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error(folly::stringPrintf("Internal error : %s",
                                                   e.what().c_str())));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void ShowExecutor::showTags() {
    auto spaceId = ectx()->rctx()->session()->space();
    auto future = ectx()->getMetaClient()->listTagSchemas(spaceId);
    auto *runner = ectx()->rctx()->runner();
    resp_ = std::make_unique<cpp2::ExecutionResponse>();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        auto value = std::move(resp).value();
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        std::vector<cpp2::RowValue> rows;
        std::vector<std::string> header{"ID", "Name"};
        resp_->set_column_names(std::move(header));

        std::map<nebula::cpp2::TagID, std::string> tagItems;
        for (auto &tag : value) {
            tagItems.emplace(tag.get_tag_id(), tag.get_tag_name());
        }

        for (auto &item : tagItems) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(2);
            row[0].set_integer(item.first);
            row[1].set_str(item.second);
            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }

        resp_->set_rows(std::move(rows));
        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error(folly::stringPrintf("Internal error : %s",
                                                   e.what().c_str())));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void ShowExecutor::showEdges() {
    auto spaceId = ectx()->rctx()->session()->space();
    auto future = ectx()->getMetaClient()->listEdgeSchemas(spaceId);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        auto value = std::move(resp).value();
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        std::vector<cpp2::RowValue> rows;
        std::vector<std::string> header{"ID", "Name"};
        resp_->set_column_names(std::move(header));

        std::map<nebula::cpp2::EdgeType, std::string> edgeItems;
        for (auto &edge : value) {
            edgeItems.emplace(edge.get_edge_type(), edge.get_edge_name());
        }

        for (auto &item : edgeItems) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(2);
            row[0].set_integer(item.first);
            row[1].set_str(item.second);
            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }
        resp_->set_rows(std::move(rows));
        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error(folly::stringPrintf("Internal error : %s",
                                                   e.what().c_str())));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void ShowExecutor::showCreateSpace() {
    auto *name = sentence_->getName();
    auto future = ectx()->getMetaClient()->getSpace(*name);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        std::vector<std::string> header{"Space", "Create Space"};
        resp_->set_column_names(std::move(header));

        std::vector<cpp2::RowValue> rows;
        std::vector<cpp2::ColumnValue> row;
        row.resize(2);
        auto properties = resp.value().get_properties();
        row[0].set_str(properties.get_space_name());

        std::string buf;
        buf.reserve(256);
        buf += folly::stringPrintf("CREATE SPACE %s (", properties.get_space_name().c_str());
        buf += "partition_num = ";
        buf += folly::to<std::string>(properties.get_partition_num());
        buf += ", ";
        buf += "replica_factor = ";
        buf += folly::to<std::string>(properties.get_replica_factor());
        buf += ")";

        row[1].set_str(buf);;
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
        resp_->set_rows(std::move(rows));

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error(folly::stringPrintf("Internal error : %s",
                                                   e.what().c_str())));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void ShowExecutor::showCreateTag() {
    auto *name = sentence_->getName();
    auto spaceId = ectx()->rctx()->session()->space();

    // Get the lastest ver
    auto future = ectx()->getMetaClient()->getTagSchema(spaceId, *name);
    auto *runner = ectx()->rctx()->runner();


    auto cb = [this] (auto &&resp) {
        auto *tagName =  sentence_->getName();
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        std::vector<std::string> header{"Tag", "Create Tag"};
        resp_->set_column_names(std::move(header));

        std::vector<cpp2::RowValue> rows;
        std::vector<cpp2::ColumnValue> row;
        row.resize(2);
        row[0].set_str(*tagName);

        std::string buf;
        buf.reserve(256);
        buf += folly::stringPrintf("CREATE TAG %s (\n", tagName->c_str());

        auto schema = resp.value();
        for (auto& item : schema.columns) {
            buf += "  ";
            buf += item.name;
            buf += " ";
            buf += valueTypeToString(item.type);
            buf += ",\n";
        }

        if (!schema.columns.empty()) {
            buf.resize(buf.size() -2);
            buf += "\n";
        }
        buf += ") ";
        nebula::cpp2::SchemaProp prop = schema.schema_prop;
        buf += "ttl_duration = ";
        if (prop.get_ttl_duration()) {
            buf += folly::to<std::string>(*prop.get_ttl_duration());
        } else {
            buf += "0";
        }
        buf += ", ttl_col = ";
        if (prop.get_ttl_col() && !(prop.get_ttl_col()->empty())) {
            buf += *prop.get_ttl_col();
        } else {
            buf += "\"\"";
        }

        row[1].set_str(buf);
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
        resp_->set_rows(std::move(rows));

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error(folly::stringPrintf("Internal error : %s",
                                                   e.what().c_str())));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void ShowExecutor::showCreateEdge() {
    auto *name = sentence_->getName();
    auto spaceId = ectx()->rctx()->session()->space();

    // Get the lastest ver
    auto future = ectx()->getMetaClient()->getEdgeSchema(spaceId, *name);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        auto *edgeName =  sentence_->getName();
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        std::vector<std::string> header{"Edge", "Create Edge"};
        resp_->set_column_names(std::move(header));

        std::vector<cpp2::RowValue> rows;
        std::vector<cpp2::ColumnValue> row;
        row.resize(2);
        row[0].set_str(*edgeName);

        std::string buf;
        buf.reserve(256);
        buf += folly::stringPrintf("CREATE EDGE %s (\n",  edgeName->c_str());

        auto schema = resp.value();
        for (auto& item : schema.columns) {
            buf += "  ";
            buf += item.name;
            buf += " ";
            buf += valueTypeToString(item.type);
            buf += ",\n";
        }

        if (!schema.columns.empty()) {
            buf.resize(buf.size() -2);
            buf += "\n";
        }
        buf += ") ";
        nebula::cpp2::SchemaProp prop = schema.schema_prop;
        buf += "ttl_duration = ";
        if (prop.get_ttl_duration()) {
            buf += folly::to<std::string>(*prop.get_ttl_duration());
        } else {
            buf += "0";
        }
        buf += ", ttl_col = ";
        if (prop.get_ttl_col() && !(prop.get_ttl_col()->empty())) {
            buf += *prop.get_ttl_col();
        } else {
            buf += "\"\"";
        }

        row[1].set_str(buf);
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
        resp_->set_rows(std::move(rows));

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error(folly::stringPrintf("Internal error : %s",
                                                   e.what().c_str())));
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void ShowExecutor::showSnapshots() {
    auto future = ectx()->getMetaClient()->listSnapshots();
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        auto getStatus = [](meta::cpp2::SnapshotStatus status) -> std::string {
            std::string str;
            switch (status) {
                case meta::cpp2::SnapshotStatus::INVALID :
                    str = "INVALID";
                    break;
                case meta::cpp2::SnapshotStatus::VALID :
                    str = "VALID";
                    break;
            }
            return str;
        };

        auto retShowSnapshots = std::move(resp).value();
        std::vector<cpp2::RowValue> rows;
        std::vector<std::string> header{"Name", "Status", "Hosts"};
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(header));
        for (auto &snapshot : retShowSnapshots) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(3);
            row[0].set_str(snapshot.name);
            row[1].set_str(getStatus(snapshot.status));
            row[2].set_str(snapshot.hosts);
            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }
        resp_->set_rows(std::move(rows));

        DCHECK(onFinish_);
        onFinish_(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error(folly::stringPrintf("Internal error : %s",
                                                   e.what().c_str())));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}


void ShowExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
