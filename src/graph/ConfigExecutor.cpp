/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/ConfigExecutor.h"
#include "base/Configuration.h"

namespace nebula {
namespace graph {

const std::string kConfigUnknown = "UNKNOWN"; // NOLINT

ConfigExecutor::ConfigExecutor(Sentence *sentence,
                               ExecutionContext *ectx)
    : Executor(ectx, "config") {
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
        case ConfigSentence::SubType::kUnknown:
            doError(Status::Error("Type unknown"));
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
        doError(Status::Error("Parse config module error"));
        return;
    }

    auto future = ectx()->gflagsManager()->listConfigs(module);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        if (!resp.ok()) {
            doError(std::move(resp.status()));
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

        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Show config exception: " << e.what();
        doError(Status::Error("Show config exception : %s", e.what().c_str()));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void ConfigExecutor::setVariables() {
    meta::cpp2::ConfigModule module = meta::cpp2::ConfigModule::UNKNOWN;
    std::string name;
    VariantType value;
    meta::cpp2::ConfigType type;
    if (configItem_ != nullptr) {
        if (configItem_->getModule() != nullptr) {
            module = toThriftConfigModule(*configItem_->getModule());
        }
        if (configItem_->getName() != nullptr) {
            name = *configItem_->getName();
        }
        if (configItem_->getValue() != nullptr) {
            Getters getters;
            auto v = configItem_->getValue()->eval(getters);
            if (!v.ok()) {
                doError(v.status());
                return;
            }
            value = v.value();
            switch (value.which()) {
                case VAR_INT64:
                    type = meta::cpp2::ConfigType::INT64;
                    break;
                case VAR_DOUBLE:
                    type = meta::cpp2::ConfigType::DOUBLE;
                    break;
                case VAR_BOOL:
                    type = meta::cpp2::ConfigType::BOOL;
                    break;
                case VAR_STR:
                    type = meta::cpp2::ConfigType::STRING;
                    break;
                default:
                    doError(Status::Error("Parse value type error"));
                    return;
            }
        } else if (configItem_->getUpdateItems() != nullptr) {
            auto status = configItem_->getUpdateItems()->toEvaledString();
            if (!status.ok()) {
                doError(status.status());
                return;
            }
            value = status.value();
            // all nested options are regarded as string
            type = meta::cpp2::ConfigType::NESTED;
        }
    }

    if (module == meta::cpp2::ConfigModule::UNKNOWN) {
        doError(Status::Error("Parse config module error"));
        return;
    }

    auto future = ectx()->gflagsManager()->setConfig(module, name, type, value);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this, name] (auto && resp) {
        if (!resp.ok()) {
            doError(Status::Error("Set config `%s' failed: %s.",
                                    name.c_str(), resp.status().toString().c_str()));
            return;
        }

        resp_ = std::make_unique<cpp2::ExecutionResponse>();
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this, name] (auto &&e) {
        auto msg = folly::stringPrintf("Set congfig `%s' exception: %s",
                                        name.c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
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
        doError(Status::Error("Parse config module error"));
        return;
    }

    auto future = ectx()->gflagsManager()->getConfig(module, name);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this, name] (auto && resp) {
        if (!resp.ok()) {
            doError(Status::Error("Get config `%s' failed: %s.",
                                    name.c_str(), resp.status().toString().c_str()));
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
        doFinish(Executor::ProcessControl::kNext);
    };

    auto error = [this, name] (auto &&e) {
        auto msg = folly::stringPrintf("Get config `%s' exception: %s",
                                        name.c_str(), e.what().c_str());
        LOG(ERROR) << msg;
        doError(Status::Error(std::move(msg)));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

std::vector<cpp2::ColumnValue> ConfigExecutor::genRow(const meta::cpp2::ConfigItem& item) {
    std::vector<cpp2::ColumnValue> row;
    row.resize(5);
    row[0].set_str(ConfigModuleToString(item.get_module()));
    row[1].set_str(item.get_name());
    row[2].set_str(ConfigTypeToString(item.get_type()));
    row[3].set_str(ConfigModeToString(item.get_mode()));
    // TODO: Console must have same type of the same column over different lines,
    //       so we transform all kinds of value to string for now.
    VariantType value;
    switch (item.get_type()) {
        case meta::cpp2::ConfigType::INT64:
            value = *reinterpret_cast<const int64_t*>(item.get_value().data());
            row[4].set_str(std::to_string(boost::get<int64_t>(value)));
            break;
        case meta::cpp2::ConfigType::DOUBLE:
            value = *reinterpret_cast<const double*>(item.get_value().data());
            row[4].set_str(std::to_string(boost::get<double>(value)));
            break;
        case meta::cpp2::ConfigType::BOOL:
            value = *reinterpret_cast<const bool*>(item.get_value().data());
            row[4].set_str(boost::get<bool>(value) ? "True" : "False");
            break;
        case meta::cpp2::ConfigType::STRING:
            value = item.get_value();
            row[4].set_str(boost::get<std::string>(value));
            break;
        case meta::cpp2::ConfigType::NESTED:
            value = item.get_value();
            Configuration conf;
            auto status = conf.parseFromString(boost::get<std::string>(value));
            if (!status.ok()) {
                row[4].set_str(boost::get<std::string>(value));
            } else {
                row[4].set_str(conf.dumpToPrettyString());
            }
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

std::string ConfigModuleToString(const meta::cpp2::ConfigModule& module) {
    auto it = meta::cpp2::_ConfigModule_VALUES_TO_NAMES.find(module);
    if (it == meta::cpp2::_ConfigModule_VALUES_TO_NAMES.end()) {
        return kConfigUnknown;
    } else {
        return it->second;
    }
}

std::string ConfigModeToString(const meta::cpp2::ConfigMode& mode) {
    auto it = meta::cpp2::_ConfigMode_VALUES_TO_NAMES.find(mode);
    if (it == meta::cpp2::_ConfigMode_VALUES_TO_NAMES.end()) {
        return kConfigUnknown;
    } else {
        return it->second;
    }
}

std::string ConfigTypeToString(const meta::cpp2::ConfigType& type) {
    auto it = meta::cpp2::_ConfigType_VALUES_TO_NAMES.find(type);
    if (it == meta::cpp2::_ConfigType_VALUES_TO_NAMES.end()) {
        return kConfigUnknown;
    } else {
        return it->second;
    }
}

}   // namespace graph
}   // namespace nebula
