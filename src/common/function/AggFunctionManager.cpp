/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "AggFunctionManager.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Set.h"

namespace nebula {

// static
AggFunctionManager &AggFunctionManager::instance() {
    static AggFunctionManager instance;
    return instance;
}

AggFunctionManager::AggFunctionManager() {
    // TODO: private ctor to construct built-in aggregate functions
    {
        auto &func = functions_[""];
        func = [](AggData* aggData, const Value& val) {
            aggData->setResult(val);
        };
    }
    {
        auto &func = functions_["COUNT"];
        func = [](AggData* aggData, const Value& val) {
            auto& res = aggData->result();
            if (res.isBadNull()) {
                return;
            }
            if (res.isNull()) {
                res = 0;
            }
            if (val.isNull() || val.empty()) {
                return;
            }

            res = res + 1;
        };
    }
    {
        auto &func = functions_["SUM"];
        func = [](AggData* aggData, const Value& val) {
            auto& res = aggData->result();
            if (res.isBadNull()) {
                return;
            }
            if (res.isNull()) {
                res = 0;
            }
            if (UNLIKELY(!val.isNull() && !val.empty() && !val.isNumeric())) {
                res = Value::kNullBadType;
                return;
            }

            if (val.isNull() || val.empty()) {
                return;
            }
            if (res.isNull()) {
                res = val;
                return;
            }
            res = res + val;
        };
    }
    {
        auto &func = functions_["AVG"];
        func =   [](AggData* aggData, const Value& val) {
            auto& res = aggData->result();
            if (res.isBadNull()) {
                return;
            }
            if (UNLIKELY(!val.isNull() && !val.empty() && !val.isNumeric())) {
                res = Value::kNullBadType;
                return;
            }

            auto& sum = aggData->sum();
            auto& cnt = aggData->cnt();
            if (val.isNull() || val.empty()) {
                return;
            }
            if (res.isNull()) {
                res = 0.0;
                sum = 0.0;
                cnt = 0.0;
            }

            sum = sum + val;
            cnt = cnt + 1;
            res = sum / cnt;
        };
    }
    {
        auto &func = functions_["MAX"];
        func = [](AggData* aggData, const Value& val) {
            auto& res = aggData->result();
            if (res.isBadNull()) {
                return;
            }
            if (val.isNull() || val.empty()) {
                return;
            }
            if (res.isNull()) {
                res = val;
                return;
            }

            if (val > res) {
                res = val;
            }
        };
    }
    {
        auto &func = functions_["MIN"];
        func = [](AggData* aggData, const Value& val) {
            auto& res = aggData->result();
            if (res.isBadNull()) {
                return;
            }
            if (val.isNull() || val.empty()) {
                return;
            }
            if (res.isNull()) {
                res = val;
                return;
            }
            if (val < res) {
                res = val;
            }
        };
    }
    {
        auto &func = functions_["STD"];
        func = [](AggData* aggData, const Value& val) {
            auto& res = aggData->result();
            if (res.isBadNull()) {
                return;
            }
            if (UNLIKELY(!val.isNull() && !val.empty() && !val.isNumeric())) {
                res = Value::kNullBadType;
                return;
            }
            if (val.isNull() || val.empty()) {
                return;
            }

            auto& cnt = aggData->cnt();
            auto& avg = aggData->avg();
            auto& deviation = aggData->deviation();
            if (res.isNull()) {
                res = 0.0;
                cnt = 0.0;
                avg = 0.0;
                deviation = 0.0;
            }

            cnt = cnt + 1;
            deviation = (cnt - 1) / (cnt * cnt) * ((val - avg) * (val - avg))
                + (cnt - 1) / cnt * deviation;
            avg = avg + (val - avg) / cnt;
            auto stdev = deviation.isFloat() ?
                         std::sqrt(deviation.getFloat()) : Value::kNullBadType;
            res = stdev;
        };
    }
    {
        auto &func = functions_["BIT_AND"];
        func = [](AggData* aggData, const Value& val) {
            auto& res = aggData->result();
            if (res.isBadNull()) {
                return;
            }
            if (UNLIKELY(!val.isNull() && !val.empty() && !val.isInt())) {
                res = Value::kNullBadType;
                return;
            }
            if (val.isNull() || val.empty()) {
                return;
            }
            if (res.isNull() && val.isInt()) {
                res = val;
                return;
            }

            res = res & val;
        };
    }
    {
        auto &func = functions_["BIT_OR"];
        func = [](AggData* aggData, const Value& val) {
            auto& res = aggData->result();
            if (res.isBadNull()) {
                return;
            }
            if (UNLIKELY(!val.isNull() && !val.empty() && !val.isInt())) {
                res = Value::kNullBadType;
                return;
            }
            if (val.isNull() || val.empty()) {
                return;
            }
            if (res.isNull() && val.isInt()) {
                res = val;
                return;
            }

            res = res | val;
        };
    }
    {
        auto &func = functions_["BIT_XOR"];
        func = [](AggData* aggData, const Value& val) {
            auto& res = aggData->result();
            if (res.isBadNull()) {
                return;
            }
            if (UNLIKELY(!val.isNull() && !val.empty() && !val.isInt())) {
                res = Value::kNullBadType;
                return;
            }
            if (val.isNull() || val.empty()) {
                return;
            }
            if (res.isNull() && val.isInt()) {
                res = val;
                return;
            }

            res = res ^ val;
        };
    }
    {
        auto &func = functions_["COLLECT"];
        func = [](AggData* aggData, const Value& val) {
            auto& res = aggData->result();
            if (res.isBadNull()) {
                return;
            }
            if (res.isNull()) {
                res = List();
            }
            if (val.isNull() || val.empty()) {
                return;
            }
            if (!res.isList()) {
                res = Value::kNullBadData;
                return;
            }
            auto &list = res.mutableList();
            list.emplace_back(val);
        };
    }
    {
        auto &func = functions_["COLLECT_SET"];
        func = [](AggData* aggData, const Value& val) {
            auto& res = aggData->result();
            if (res.isBadNull()) {
                return;
            }
            if (res.isNull()) {
                res = Set();
            }
            if (val.isNull() || val.empty()) {
                return;
            }

            if (!res.isSet()) {
                res = Value::kNullBadData;
                return;
            }
            auto& set = res.mutableSet();
            set.values.emplace(val);
        };
    }
}

StatusOr<AggFunctionManager::AggFunction> AggFunctionManager::get(const std::string &func) {
    auto result = instance().getInternal(func);
    NG_RETURN_IF_ERROR(result);
    return result.value();
}

Status AggFunctionManager::find(const std::string &func) {
    auto result = instance().getInternal(func);
    NG_RETURN_IF_ERROR(result);
    return Status::OK();
}

StatusOr<AggFunctionManager::AggFunction> AggFunctionManager::getInternal(std::string func) const {
    std::transform(func.begin(), func.end(), func.begin(), ::toupper);
    // check existence
    auto iter = functions_.find(func);
    if (iter == functions_.end()) {
        return Status::Error("Unknown aggregate function `%s'", func.c_str());
    }

    return iter->second;
}

Status AggFunctionManager::load(const std::string &soname, const std::vector<std::string> &funcs) {
    return instance().loadInternal(soname, funcs);
}

Status AggFunctionManager::loadInternal(const std::string &, const std::vector<std::string> &) {
    return Status::Error("Dynamic aggregate function loading not supported yet");
}

Status AggFunctionManager::unload(const std::string &soname,
                                  const std::vector<std::string> &funcs) {
    return instance().loadInternal(soname, funcs);
}

Status AggFunctionManager::unloadInternal(const std::string &, const std::vector<std::string> &) {
    return Status::Error("Dynamic aggregate function unloading not supported yet");
}

}   // namespace nebula
