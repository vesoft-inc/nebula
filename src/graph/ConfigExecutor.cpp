/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/ConfigExecutor.h"
#include "meta/ConfigManager.h"

namespace nebula {
namespace graph {

const std::string kConfigModuleUnknown = "UNKNOWN"; // NOLINT

using meta::ConfigManager;

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
    std::string space;
    std::string module;
    meta::cpp2::ConfigModule moduleEnum = meta::cpp2::ConfigModule::ALL;
    if (configItem_ != nullptr) {
        if (configItem_->getSpace() != nullptr) {
            space = *configItem_->getSpace();
        }
        if (configItem_->getModule() != nullptr) {
            module = *configItem_->getModule();
            moduleEnum = StringToConfigModule(module);
            if (moduleEnum == meta::cpp2::ConfigModule::UNKNOWN) {
                DCHECK(onError_);
                onError_(Status::Error("Illegal module"));
                return;
            }
        }
    }

    auto future = ConfigManager::instance()->listConfigs(space, moduleEnum);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        std::vector<std::string> header{"space", "module", "name", "type", "value"};
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(header));

        auto configs = std::move(resp).value();
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
        onError_(Status::Error(folly::stringPrintf("Internal error : %s",
                                                   e.what().c_str())));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void ConfigExecutor::setVariables() {
    std::string space;
    std::string module;
    std::string name;
    VariantType value;
    meta::cpp2::ConfigModule moduleEnum;
    if (configItem_ != nullptr) {
        if (configItem_->getSpace() != nullptr) {
            space = *configItem_->getSpace();
        }
        if (configItem_->getModule() != nullptr) {
            module = *configItem_->getModule();
            moduleEnum = StringToConfigModule(module);
            if (moduleEnum == meta::cpp2::ConfigModule::UNKNOWN) {
                DCHECK(onError_);
                onError_(Status::Error("Illegal module"));
                return;
            }
        }
        if (configItem_->getName() != nullptr) {
            name = *configItem_->getName();
        }
        if (configItem_->getValue() != nullptr) {
            value = configItem_->getValue()->eval();
        }
    }

    auto future = ConfigManager::instance()->setConfig(space, moduleEnum, name, value);
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
    std::string space;
    std::string module;
    std::string name;
    ColumnType type;
    meta::cpp2::ConfigModule moduleEnum;
    if (configItem_ != nullptr) {
        if (configItem_->getSpace() != nullptr) {
            space = *configItem_->getSpace();
        }
        if (configItem_->getModule() != nullptr) {
            module = *configItem_->getModule();
            moduleEnum = StringToConfigModule(module);
            if (moduleEnum == meta::cpp2::ConfigModule::UNKNOWN) {
                DCHECK(onError_);
                onError_(Status::Error("Illegal module"));
                return;
            }
        }
        if (configItem_->getName() != nullptr) {
            name = *configItem_->getName();
        }
        if (configItem_->getType() != nullptr) {
            type = *configItem_->getType();
        }
    }

    auto future = [&] () -> folly::Future<StatusOr<meta::ConfigItem>> {
        switch (type) {
            case ColumnType::INT:
                return ConfigManager::instance()->getConfig(space, moduleEnum, name,
                                                            VariantTypeEnum::INT64);
            case ColumnType::DOUBLE:
                return ConfigManager::instance()->getConfig(space, moduleEnum, name,
                                                            VariantTypeEnum::DOUBLE);
            case ColumnType::BOOL:
                return ConfigManager::instance()->getConfig(space, moduleEnum, name,
                                                            VariantTypeEnum::BOOL);
            case ColumnType::STRING:
                return ConfigManager::instance()->getConfig(space, moduleEnum, name,
                                                            VariantTypeEnum::STRING);
            default:
                return Status::Error("parse value type error");
        }
    }();
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto && resp) {
        if (!resp.ok()) {
            DCHECK(onError_);
            onError_(std::move(resp).status());
            return;
        }

        std::vector<std::string> header{"space", "module", "name", "type", "value"};
        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        resp_->set_column_names(std::move(header));

        auto item = std::move(resp).value();
        std::vector<cpp2::RowValue> rows;
        auto row = genRow(item);
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
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

void ConfigExecutor::declareVariables() {
    std::string space;
    std::string module;
    std::string name;
    ColumnType type;
    VariantType defaultValue;
    meta::cpp2::ConfigModule moduleEnum;
    if (configItem_ != nullptr) {
        if (configItem_->getSpace() != nullptr) {
            space = *configItem_->getSpace();
        }
        if (configItem_->getModule() != nullptr) {
            module = *configItem_->getModule();
            moduleEnum = StringToConfigModule(module);
            if (moduleEnum == meta::cpp2::ConfigModule::UNKNOWN) {
                DCHECK(onError_);
                onError_(Status::Error("Illegal module"));
                return;
            }
        }
        if (configItem_->getName() != nullptr) {
            name = *configItem_->getName();
        }
        if (configItem_->getType() != nullptr) {
            type = *configItem_->getType();
        }
        if (configItem_->getValue() != nullptr) {
            defaultValue = configItem_->getValue()->eval();
        }
    }

    VariantTypeEnum eType;
    switch (type) {
        case ColumnType::INT:
            eType = VariantTypeEnum::INT64;
            break;
        case ColumnType::DOUBLE:
            eType = VariantTypeEnum::DOUBLE;
            break;
        case ColumnType::BOOL:
            eType = VariantTypeEnum::BOOL;
            break;
        case ColumnType::STRING:
            eType = VariantTypeEnum::STRING;
            break;
        default:
            DCHECK(onError_);
            onError_(Status::Error("Parse value type error"));
            return;
    }

    auto future = ConfigManager::instance()->registerConfig(space, moduleEnum, name,
                                                          eType, defaultValue);
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

std::vector<cpp2::ColumnValue> ConfigExecutor::genRow(const meta::ConfigItem& item) {
    std::vector<cpp2::ColumnValue> row;
    row.resize(5);
    row[0].set_str(item.space_);
    row[1].set_str(ConfigModuleToString(item.module_));
    row[2].set_str(item.name_);
    switch (item.type_) {
        case VariantTypeEnum::INT64:
            row[3].set_str("int64");
            row[4].set_integer(boost::get<int64_t>(item.value_));
            break;
        case VariantTypeEnum::DOUBLE:
            row[3].set_str("double");
            row[4].set_double_precision(boost::get<double>(item.value_));
            break;
        case VariantTypeEnum::BOOL:
            row[3].set_str("bool");
            row[4].set_bool_val(boost::get<bool>(item.value_));
            break;
        case VariantTypeEnum::STRING:
            row[3].set_str("string");
            row[4].set_str(boost::get<std::string>(item.value_));
            break;
    }
    return row;
}

meta::cpp2::ConfigModule ConfigExecutor::StringToConfigModule(std::string module) {
    std::transform(module.begin(), module.end(), module.begin(), ::toupper);
    auto it = meta::cpp2::_ConfigModule_NAMES_TO_VALUES.find(module.data());
    if (it == meta::cpp2::_ConfigModule_NAMES_TO_VALUES.end()) {
        return meta::cpp2::ConfigModule::UNKNOWN;
    } else {
        return it->second;
    }
}

std::string ConfigExecutor::ConfigModuleToString(meta::cpp2::ConfigModule module) {
    auto it = meta::cpp2::_ConfigModule_VALUES_TO_NAMES.find(module);
    if (it == meta::cpp2::_ConfigModule_VALUES_TO_NAMES.end()) {
        return kConfigModuleUnknown;
    } else {
        return it->second;
    }
}

void ConfigExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
