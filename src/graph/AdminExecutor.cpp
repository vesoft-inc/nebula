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
    switch (show_kind) {
        case ShowKind::kShowHosts:
            showHostsExecute();
            break;
        case ShowKind::kUnknown:
            onError_(Status::Error("Show Sentence kind unknown"));
            break;
        // intentionally no `default'
    }
}


void ShowExecutor::showHostsExecute() {
    auto future = ectx()->getMetaClient()->listHosts();
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(Status::Error("remove hosts failed"));
            return;
        }

        auto retShowHosts = resp.value();
        std::vector<cpp2::RowValue> rows;
        std::vector<cpp2::ColumnValue> row;
        std::vector<std::string> header;
        resp_ = std::make_unique<cpp2::ExecutionResponse>();

        header.clear();
        header.push_back("Ip");
        header.push_back("Port");
        resp_->set_column_names(std::move(header));

        for (auto &host : retShowHosts) {
            row.clear();
            row.resize(2);
            row[0].set_str(NetworkUtils::ipFromHostAddr(host));
            row[1].set_str(folly::to<std::string>(NetworkUtils::portFromHostAddr(host)));
            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }
        resp_->set_rows(std::move(rows));

        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
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
        return Status::Error("Add hosts Sentence host address illegal");
    }
    return Status::OK();
}


void AddHostsExecutor::execute() {
    auto future = ectx()->getMetaClient()->addHosts(host_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        auto ret = resp.value();
        if (!ret) {
            DCHECK(onError_);
            onError_(Status::Error("add hosts failed"));
            return;
        }
        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}


RemoveHostsExecutor::RemoveHostsExecutor(Sentence *sentence,
                                         ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<RemoveHostsSentence*>(sentence);
}


Status RemoveHostsExecutor::prepare() {
    host_ = sentence_->hosts();
    if (host_.size() == 0) {
        return Status::Error("Remove hosts Sentence host address illegal");
    }
    return Status::OK();
}


void RemoveHostsExecutor::execute() {
    auto future = ectx()->getMetaClient()->removeHosts(host_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        auto ret = resp.value();
        if (!ret) {
            DCHECK(onError_);
            onError_(Status::Error("remove hosts failed"));
            return;
        }
        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
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
    if (partNum_ == 0)
        return Status::Error("partition_num value illegal");
    if (replicaFactor_ == 0)
        return Status::Error("replica_factor value illegal");
    return Status::OK();
}


void CreateSpaceExecutor::execute() {
    auto future = ectx()->getMetaClient()->createSpace(*spaceName_, partNum_, replicaFactor_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        auto spaceId = resp.value();
        if (spaceId <= 0) {
            DCHECK(onError_);
            onError_(Status::Error("Create space failed"));
            return;
        }
        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}


DropSpaceExecutor::DropSpaceExecutor(Sentence *sentence,
                                     ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DropSpaceSentence*>(sentence);
}


Status DropSpaceExecutor::prepare() {
    spaceName_ = sentence_->spaceName();
    return Status::OK();
}


void DropSpaceExecutor::execute() {
    auto future = ectx()->getMetaClient()->dropSpace(*spaceName_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        auto  ret = resp.value();
        if (!ret) {
            DCHECK(onError_);
            onError_(Status::Error("Drop space failed"));
            return;
        }
        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };

    std::move(future).via(runner).thenValue(cb).thenError(error);
}
}   // namespace graph
}   // namespace nebula
