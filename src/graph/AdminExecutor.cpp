/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "graph/AdminExecutor.h"
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
    auto show_kind = sentence_->showKind();
    resp_ = std::make_unique<cpp2::ExecutionResponse>();
    std::vector<cpp2::RowValue> rows;
    std::vector<cpp2::ColumnValue> row;
    std::vector<std::string> header;

    StatusOr<std::vector<HostAddr>> retShowHosts;

    switch (show_kind) {
        case ShowKind::kShowHosts:
            InitMetaClient();
            retShowHosts = metaClient_->listHosts();
            CHECK(retShowHosts.ok());

            header.clear();
            header.push_back("Ip");
            header.push_back("Port");
            resp_->set_column_names(std::move(header));

            for (auto &host : retShowHosts.value()) {
                 row.clear();
                 row.resize(2);
                 row[0].set_str(NetworkUtils::ipFromHostAddr(host));
                 row[1].set_str(folly::to<std::string>(NetworkUtils::portFromHostAddr(host)));
                 rows.emplace_back();
                 rows.back().set_columns(std::move(row));
            }
            resp_->set_rows(std::move(rows));
            break;
        case ShowKind::kUnknown:
            LOG(FATAL) << "Show Sentence kind unknown: " <<show_kind;
        // intentionally no `default'
    }
    DCHECK(onFinish_);
    onFinish_();
}


void ShowExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}


AddHostsExecutor::AddHostsExecutor(Sentence *sentence,
                                   ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<AddHostsSentence*>(sentence);
}


Status AddHostsExecutor::prepare() {
    host_ = sentence_->hosts();
    if (host_.size() == 0) {
        LOG(FATAL) << "Add hosts Sentence host address illegal";
    }
    return Status::OK();
}


void AddHostsExecutor::execute() {
    InitMetaClient();
    auto ret = metaClient_->addHosts(host_);
    CHECK_EQ(ret, Status::OK());

    DCHECK(onFinish_);
    onFinish_();
}


CreateSpaceExecutor::CreateSpaceExecutor(Sentence *sentence,
                                         ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<CreateSpaceSentence*>(sentence);
}


Status CreateSpaceExecutor::prepare() {
    spaceName_ = sentence_->spaceName();
    for (auto &item : sentence_->getOpts()) {
        switch (item->getOptType()) {
            case SpaceOptItem::PARTITION_NUM:
                partNum_ = item->get_partition_num();
                break;
            case SpaceOptItem::REPLICA_FACTOR:
                replicaFactor_ = item->get_replica_factor();
                break;
        }
    }
    return Status::OK();
}


void CreateSpaceExecutor::execute() {
    CHECK_GT(partNum_, 0) << "partition_num value illegal";
    CHECK_GT(replicaFactor_, 0) << "replica_factor value illegal";
    InitMetaClient();
    auto ret = metaClient_->createSpace(*spaceName_, partNum_, replicaFactor_);
    CHECK(ret.ok()) << ret.status();

    DCHECK(onFinish_);
    onFinish_();
}
}   // namespace graph
}   // namespace nebula
