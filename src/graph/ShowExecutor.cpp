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
    if (sentence_->showType() == ShowSentence::ShowType::kShowTags ||
        sentence_->showType() == ShowSentence::ShowType::kShowEdges) {
        return checkIfGraphSpaceChosen();
    } else {
        return Status::OK();
    }
}


void ShowExecutor::execute() {
    auto showType = sentence_->showType();
    switch (showType) {
        case ShowSentence::ShowType::kShowHosts:
            showHosts();
            break;
        case ShowSentence::ShowType::kShowSpaces:
            showSpaces();
            break;
        case ShowSentence::ShowType::kShowTags:
            showTags();
            break;
        case ShowSentence::ShowType::kShowEdges:
            showEdges();
            break;
        case ShowSentence::ShowType::kShowUsers:
            showUsers();
            break;
        case ShowSentence::ShowType::kShowUser: {
            const auto &user = sentence_->getName();
            showUser(std::move(user));
            break;
        }
        case ShowSentence::ShowType::kShowRoles: {
            const auto &space = sentence_->getName();
            showRoles(std::move(space));
            break;
        }
        case ShowSentence::ShowType::kUnknown:
            onError_(Status::Error("Type unknown"));
            break;
        // intentionally no `default'
    }
}


void ShowExecutor::showHosts() {
    auto future = ectx()->getMetaClient()->listHosts();
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        auto retShowHosts = std::move(resp).value();
        std::vector<cpp2::RowValue> rows;
        std::vector<std::string> header;
        resp_ = std::make_unique<cpp2::ExecutionResponse>();

        header.push_back("Ip");
        header.push_back("Port");
        resp_->set_column_names(std::move(header));

        for (auto &host : retShowHosts) {
            std::vector<cpp2::ColumnValue> row;
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
        std::vector<std::string> header;
        resp_ = std::make_unique<cpp2::ExecutionResponse>();

        header.push_back("Name");
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
        onFinish_();
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
        std::vector<std::string> header{"Name"};
        resp_->set_column_names(std::move(header));

        for (auto &tag : value) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(1);
            row[0].set_str(tag.get_tag_name());
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
        std::vector<std::string> header{"Name"};
        resp_->set_column_names(std::move(header));

        for (auto &edge : value) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(1);
            row[0].set_str(edge.get_edge_name());
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
        onError_(Status::Error(folly::stringPrintf("Internal error : %s",
                                                   e.what().c_str())));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void ShowExecutor::showUsers() {
    // TODO(boshengchen) , get data from client cache.
    auto future = ectx()->getMetaClient()->listUsers();
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        auto retShowUsers = std::move(resp).value();
        std::vector<cpp2::RowValue> rows;
        std::vector<std::string> header;
        resp_ = std::make_unique<cpp2::ExecutionResponse>();

        header.push_back("Account");
        header.push_back("Status");
        header.push_back("max_queries_per_hour");
        header.push_back("max_updates_per_hour");
        header.push_back("max_connections_per_hour");
        header.push_back("max_user_connections");
        resp_->set_column_names(std::move(header));

        for (auto it = retShowUsers.begin(); it != retShowUsers.end(); it++) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(6);
            row[0].set_str(it->second.get_account());
            row[1].set_str(*(it->second.get_is_lock()) ? "locked" : "unlock");
            row[2].set_str(folly::to<std::string>(*(it->second.get_max_queries_per_hour())));
            row[3].set_str(folly::to<std::string>(*(it->second.get_max_updates_per_hour())));
            row[4].set_str(folly::to<std::string>(*(it->second.get_max_connections_per_hour())));
            row[5].set_str(folly::to<std::string>(*(it->second.get_max_user_connections())));
            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }

        resp_->set_rows(std::move(rows));
        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception : " << e.what();
        DCHECK(onError_);
        onError_(Status::Error(folly::stringPrintf("Internal error : %s",
                                                   e.what().c_str())));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void ShowExecutor::showUser(const std::string *user) {
    // TODO(boshengchen) , get data from client cache.
    auto future = ectx()->getMetaClient()->getUser(*user);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        auto userItem = std::move(resp).value();
        std::vector<cpp2::RowValue> rows;
        std::vector<std::string> header;
        resp_ = std::make_unique<cpp2::ExecutionResponse>();

        header.push_back("Account");
        header.push_back("First Name");
        header.push_back("Last Name");
        header.push_back("Status");
        resp_->set_column_names(std::move(header));
        std::vector<cpp2::ColumnValue> row;
        row.resize(6);
        row[0].set_str(userItem.get_account());
        row[1].set_str(*(userItem.get_is_lock()) ? "locked" : "unlock");
        row[2].set_str(folly::to<std::string>(*(userItem.get_max_queries_per_hour())));
        row[3].set_str(folly::to<std::string>(*(userItem.get_max_updates_per_hour())));
        row[4].set_str(folly::to<std::string>(*(userItem.get_max_connections_per_hour())));
        row[5].set_str(folly::to<std::string>(*(userItem.get_max_user_connections())));
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
        resp_->set_rows(std::move(rows));

        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception : " << e.what();
        DCHECK(onError_);
        onError_(Status::Error(folly::stringPrintf("Internal error : %s",
                                                   e.what().c_str())));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void ShowExecutor::showRoles(const std::string *space) {
    // TODO(boshengchen) , get data from client cache.
    auto spaceRet = ectx()->getMetaClient()->getSpaceIdByNameFromCache(space->data());
    if (!spaceRet.ok()) {
        onError_(Status::Error("Space not found : '%s'", space));
        return;
    }
    auto future = ectx()->getMetaClient()->listRoles(spaceRet.value());
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        auto roles = std::move(resp).value();
        std::vector<cpp2::RowValue> rows;
        std::vector<std::string> header;
        resp_ = std::make_unique<cpp2::ExecutionResponse>();

        header.push_back("User");
        header.push_back("Role");
        resp_->set_column_names(std::move(header));

        for (auto &role : roles) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(2);
            auto account = ectx()->getMetaClient()->getUserNameByIdFromCache(role.get_user_id());
            if (!account.ok()) {
                DCHECK(onError_);
                onError_(Status::Error("List roles failed"));
                return;
            }
            row[0].set_str(std::move(account.value()));
            row[1].set_str(roleStr(role.get_role_type()));
            rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }
        resp_->set_rows(std::move(rows));

        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception : " << e.what();
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

std::string ShowExecutor::roleStr(meta::cpp2::RoleType type) {
    switch (type) {
        case meta::cpp2::RoleType::GOD:
            return std::string("god");
        case meta::cpp2::RoleType::ADMIN:
            return std::string("admin");
        case meta::cpp2::RoleType::USER:
            return std::string("user");
        case meta::cpp2::RoleType::GUEST:
            return std::string("guest");
        default:
            return "Unknown";
    }
}

}   // namespace graph
}   // namespace nebula

