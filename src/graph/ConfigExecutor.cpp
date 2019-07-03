/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/ConfigExecutor.h"
#include "meta/GflagsManager.h"

namespace nebula {
namespace graph {

using meta::GflagsManager;

ConfigExecutor::ConfigExecutor(Sentence *sentence,
                               ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<ConfigSentence*>(sentence);
}

Status ConfigExecutor::prepare() {
    configItem_ = sentence_->configItem();
    return Status::OK();
}

void ConfigExecutor::execute() {
    auto showType = sentence_->subType();
    switch (showType) {
        case ConfigSentence::SubType::kShow:
            showVariables();
            break;
        case ConfigSentence::SubType::kSet:
            setVariables();
            break;
        case ConfigSentence::SubType::kGet:
            getVariables();
            break;
        case ConfigSentence::SubType::kDeclare:
            declareVariables();
            break;
        case ConfigSentence::SubType::kUnknown:
            onError_(Status::Error("Type unknown"));
            break;
    }
}

void ConfigExecutor::showVariables() {
    meta::cpp2::ConfigModule module = meta::cpp2::ConfigModule::ALL;
    if (configItem_ != nullptr) {
        if (configItem_->getModule() != nullptr) {
            module = toThriftConfigModule(*configItem_->getModule());
        }
    }

    if (module == meta::cpp2::ConfigModule::UNKNOWN) {
        DCHECK(onError_);
        onError_(Status::Error("Parse config module error"));
        return;
    }

    auto future = GflagsManager::instance()->listConfigs(module);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp.status()));
            return;
        }

        std::vector<std::string> header{"module", "name", "type", "mode", "value"};
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(header));

        auto configs = std::move(resp.value());
        std::vector<cpp2::RowValue> rows;
        for (const auto &item : configs) {
            auto row = genRow(item);
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
        onError_(Status::Error(folly::stringPrintf("Internal error : %s", e.what().c_str())));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void ConfigExecutor::setVariables() {
    meta::cpp2::ConfigModule module = meta::cpp2::ConfigModule::UNKNOWN;
    std::string name;
    VariantType value;
    if (configItem_ != nullptr) {
        if (configItem_->getModule() != nullptr) {
            module = toThriftConfigModule(*configItem_->getModule());
        }
        if (configItem_->getName() != nullptr) {
            name = *configItem_->getName();
        }
        if (configItem_->getValue() != nullptr) {
            value = configItem_->getValue()->eval();
        }
    }

    if (module == meta::cpp2::ConfigModule::UNKNOWN) {
        DCHECK(onError_);
        onError_(Status::Error("Parse config module error"));
        return;
    }

    VariantTypeEnum eType = static_cast<VariantTypeEnum>(value.which());
    meta::cpp2::ConfigType type;
    switch (eType) {
        case VariantTypeEnum::INT64:
            type = meta::cpp2::ConfigType::INT64;
            break;
        case VariantTypeEnum::DOUBLE:
            type = meta::cpp2::ConfigType::DOUBLE;
            break;
        case VariantTypeEnum::BOOL:
            type = meta::cpp2::ConfigType::BOOL;
            break;
        case VariantTypeEnum::STRING:
            type = meta::cpp2::ConfigType::STRING;
            break;
        default:
            DCHECK(onError_);
            onError_(Status::Error("Parse value type error"));
            return;
    }

    auto future = GflagsManager::instance()->setConfig(module, name, type, value);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto && resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp));
            return;
        }

        resp_ = std::make_unique<cpp2::ExecutionResponse>();

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

void ConfigExecutor::getVariables() {
    meta::cpp2::ConfigModule module = meta::cpp2::ConfigModule::UNKNOWN;
    std::string name;
    if (configItem_ != nullptr) {
        if (configItem_->getModule() != nullptr) {
            module = toThriftConfigModule(*configItem_->getModule());
        }
        if (configItem_->getName() != nullptr) {
            name = *configItem_->getName();
        }
    }

    if (module == meta::cpp2::ConfigModule::UNKNOWN) {
        DCHECK(onError_);
        onError_(Status::Error("Parse config module error"));
        return;
    }

    auto future = GflagsManager::instance()->getConfig(module, name);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto && resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp.status()));
            return;
        }

        std::vector<std::string> header{"module", "name", "type", "mode", "value"};
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(header));

        auto configs = std::move(resp.value());
        std::vector<cpp2::RowValue> rows;
        for (const auto &item : configs) {
            auto row = genRow(item);
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
        onError_(Status::Error(folly::stringPrintf("Internal error : %s", e.what().c_str())));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void ConfigExecutor::declareVariables() {
    meta::cpp2::ConfigModule module = meta::cpp2::ConfigModule::UNKNOWN;
    std::string name;
    ColumnType colType;
    VariantType defaultValue;
    meta::cpp2::ConfigMode mode = meta::cpp2::ConfigMode::IMMUTABLE;
    if (configItem_ != nullptr) {
        if (configItem_->getModule() != nullptr) {
            module = toThriftConfigModule(*configItem_->getModule());
        }
        if (configItem_->getName() != nullptr) {
            name = *configItem_->getName();
        }
        if (configItem_->getType() != nullptr) {
            colType = *configItem_->getType();
        }
        if (configItem_->getMode() != nullptr) {
            mode = toThriftConfigMode(*configItem_->getMode());
        }
        if (configItem_->getValue() != nullptr) {
            defaultValue = configItem_->getValue()->eval();
        }
    }

    if (module == meta::cpp2::ConfigModule::UNKNOWN) {
        DCHECK(onError_);
        onError_(Status::Error("Parse config module error"));
        return;
    }

    meta::cpp2::ConfigType type;
    switch (colType) {
        case ColumnType::INT:
            type = meta::cpp2::ConfigType::INT64;
            break;
        case ColumnType::DOUBLE:
            type = meta::cpp2::ConfigType::DOUBLE;
            break;
        case ColumnType::BOOL:
            type = meta::cpp2::ConfigType::BOOL;
            break;
        case ColumnType::STRING:
            type = meta::cpp2::ConfigType::STRING;
            break;
        default:
            DCHECK(onError_);
            onError_(Status::Error("Parse value type error"));
            return;
    }

    // check config has been reigstered or not
    auto status = GflagsManager::instance()->isCfgRegistered(module, name, type);
    if (status.code() != Status::kCfgNotFound) {
        DCHECK(onError_);
        onError_(status);
        return;
    }

    auto future = GflagsManager::instance()->registerConfig(module, name, type, mode,
                                                            defaultValue);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto && resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp));
            return;
        }

        resp_ = std::make_unique<cpp2::ExecutionResponse>();

        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error(folly::stringPrintf("Internal error : %s", e.what().c_str())));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

std::vector<cpp2::ColumnValue> ConfigExecutor::genRow(const meta::ConfigItem& item) {
    std::vector<cpp2::ColumnValue> row;
    row.resize(5);
    row[0].set_str(meta::ConfigModuleToString(item.module_));
    row[1].set_str(item.name_);
    row[2].set_str(meta::ConfigTypeToString(item.type_));
    row[3].set_str(meta::ConfigModeToString(item.mode_));
    // TODO: Console must have same type of the same column over different lines,
    //       so we transform all kinds of value to string for now.
    switch (item.type_) {
        case meta::cpp2::ConfigType::INT64:
            row[4].set_str(std::to_string(boost::get<int64_t>(item.value_)));
            break;
        case meta::cpp2::ConfigType::DOUBLE:
            row[4].set_str(std::to_string(boost::get<double>(item.value_)));
            break;
        case meta::cpp2::ConfigType::BOOL:
            row[4].set_str(boost::get<bool>(item.value_) ? "True" : "False");
            break;
        case meta::cpp2::ConfigType::STRING:
            row[4].set_str(boost::get<std::string>(item.value_));
            break;
    }
    return row;
}

void ConfigExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

meta::cpp2::ConfigModule toThriftConfigModule(const nebula::ConfigModule& mode) {
    switch (mode) {
        case nebula::ConfigModule::ALL:
            return meta::cpp2::ConfigModule::ALL;
        case nebula::ConfigModule::GRAPH:
            return meta::cpp2::ConfigModule::GRAPH;
        case nebula::ConfigModule::META:
            return meta::cpp2::ConfigModule::META;
        case nebula::ConfigModule::STORAGE:
            return meta::cpp2::ConfigModule::STORAGE;
        default:
            return meta::cpp2::ConfigModule::UNKNOWN;
    }
}

meta::cpp2::ConfigMode toThriftConfigMode(const nebula::ConfigMode& mode) {
    switch (mode) {
        case nebula::ConfigMode::IMMUTABLE:
            return meta::cpp2::ConfigMode::IMMUTABLE;
        case nebula::ConfigMode::REBOOT:
            return meta::cpp2::ConfigMode::REBOOT;
        case nebula::ConfigMode::MUTABLE:
            return meta::cpp2::ConfigMode::MUTABLE;
        default:
            return meta::cpp2::ConfigMode::IMMUTABLE;
    }
}

}   // namespace graph
}   // namespace nebula
