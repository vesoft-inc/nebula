/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "filter/FunctionManager.h"
#include "time/TimeUtils.h"

namespace nebula {

// static
FunctionManager& FunctionManager::instance() {
    static FunctionManager instance;
    return instance;
}


FunctionManager::FunctionManager() {
    {
        // absolute value
        auto &attr = functions_["abs"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::abs(Expression::asDouble(args[0]));
        };
    }
    {
        // to nearest integer value not greater than x
        auto &attr = functions_["floor"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::floor(Expression::asDouble(args[0]));
        };
    }
    {
        // to nearest integer value not less than x
        auto &attr = functions_["ceil"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::ceil(Expression::asDouble(args[0]));
        };
    }
    {
        // to nearest integer value
        auto &attr = functions_["round"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::round(Expression::asDouble(args[0]));
        };
    }
    {
        // square root
        auto &attr = functions_["sqrt"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::sqrt(Expression::asDouble(args[0]));
        };
    }
    {
        // cubic root
        auto &attr = functions_["cbrt"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::cbrt(Expression::asDouble(args[0]));
        };
    }
    {
        // sqrt(x^2 + y^2)
        auto &attr = functions_["hypot"];
        attr.minArity_ = 2;
        attr.maxArity_ = 2;
        attr.body_ = [] (const auto &args) {
            auto x = Expression::asDouble(args[0]);
            auto y = Expression::asDouble(args[1]);
            return std::hypot(x, y);
        };
    }
    {
        // base^exp
        auto &attr = functions_["pow"];
        attr.minArity_ = 2;
        attr.maxArity_ = 2;
        attr.body_ = [] (const auto &args) {
            auto base = Expression::asDouble(args[0]);
            auto exp = Expression::asDouble(args[1]);
            return std::pow(base, exp);
        };
    }
    {
        // e^x
        auto &attr = functions_["exp"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::exp(Expression::asDouble(args[0]));
        };
    }
    {
        // 2^x
        auto &attr = functions_["exp2"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::exp2(Expression::asDouble(args[0]));
        };
    }
    {
        // e-based logarithm
        auto &attr = functions_["log"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::log(Expression::asDouble(args[0]));
        };
    }
    {
        // 2-based logarithm
        auto &attr = functions_["log2"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::log2(Expression::asDouble(args[0]));
        };
    }
    {
        // 10-based logarithm
        auto &attr = functions_["log10"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::log10(Expression::asDouble(args[0]));
        };
    }
    {
        auto &attr = functions_["sin"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::sin(Expression::asDouble(args[0]));
        };
    }
    {
        auto &attr = functions_["asin"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::asin(Expression::asDouble(args[0]));
        };
    }
    {
        auto &attr = functions_["cos"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::cos(Expression::asDouble(args[0]));
        };
    }
    {
        auto &attr = functions_["acos"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::acos(Expression::asDouble(args[0]));
        };
    }
    {
        auto &attr = functions_["tan"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::tan(Expression::asDouble(args[0]));
        };
    }
    {
        auto &attr = functions_["atan"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            return std::atan(Expression::asDouble(args[0]));
        };
    }
    {
        // rand32(), rand32(max), rand32(min, max)
        auto &attr = functions_["rand32"];
        attr.minArity_ = 0;
        attr.maxArity_ = 2;
        attr.body_ = [] (const auto &args) {
            if (args.empty()) {
                return static_cast<int64_t>(folly::Random::rand32());
            } else if (args.size() == 1UL) {
                auto max = Expression::asInt(args[0]);
                return static_cast<int64_t>(folly::Random::rand32(max));
            }
            DCHECK_EQ(2UL, args.size());
            auto min = Expression::asInt(args[0]);
            auto max = Expression::asInt(args[1]);
            return static_cast<int64_t>(folly::Random::rand32(min, max));
        };
    }
    {
        // rand64(), rand64(max), rand64(min, max)
        auto &attr = functions_["rand64"];
        attr.minArity_ = 0;
        attr.maxArity_ = 2;
        attr.body_ = [] (const auto &args) {
            if (args.empty()) {
                return static_cast<int64_t>(folly::Random::rand64());
            } else if (args.size() == 1UL) {
                auto max = Expression::asInt(args[0]);
                return static_cast<int64_t>(folly::Random::rand64(max));
            }
            DCHECK_EQ(2UL, args.size());
            auto min = Expression::asInt(args[0]);
            auto max = Expression::asInt(args[1]);
            return static_cast<int64_t>(folly::Random::rand64(min, max));
        };
    }
    {
        // unix timestamp
        auto &attr = functions_["now"];
        attr.minArity_ = 0;
        attr.maxArity_ = 0;
        attr.body_ = [] (const auto &args) {
            UNUSED(args);
            return time::TimeUtils::nowInSeconds();
        };
    }
    {
        auto &attr = functions_["strcasecmp"];
        attr.minArity_ = 2;
        attr.maxArity_ = 2;
        attr.body_ = [] (const auto &args) {
            auto &left = Expression::asString(args[0]);
            auto &right = Expression::asString(args[1]);
            return static_cast<int64_t>(::strcasecmp(left.c_str(), right.c_str()));
        };
    }
    {
        // 64bit signed hash value
        auto &attr = functions_["hash"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            auto &str = Expression::asString(args[0]);
            return static_cast<int64_t>(std::hash<std::string>()(str));
        };
    }
}


// static
StatusOr<FunctionManager::Function>
FunctionManager::get(const std::string &func, size_t arity) {
    return instance().getInternal(func, arity);
}


StatusOr<FunctionManager::Function>
FunctionManager::getInternal(const std::string &func, size_t arity) const {
    auto status = Status::OK();
    folly::RWSpinLock::ReadHolder holder(lock_);
    // check existence
    auto iter = functions_.find(func);
    if (iter == functions_.end()) {
        return Status::Error("Function `%s' not defined", func.c_str());
    }
    // check arity
    auto minArity = iter->second.minArity_;
    auto maxArity = iter->second.maxArity_;
    if (arity < minArity || arity > maxArity) {
        return Status::Error("Arity not match for function `%s': %lu vs. [%lu-%lu]",
                              func.c_str(), arity, minArity, maxArity);
    }
    return iter->second.body_;
}


// static
Status FunctionManager::load(const std::string &name,
                             const std::vector<std::string> &funcs) {
    return instance().loadInternal(name, funcs);
}


Status FunctionManager::loadInternal(const std::string &name,
                                     const std::vector<std::string> &funcs) {
    UNUSED(name);
    UNUSED(funcs);
    return Status::Error("Dynamic function loading not supported yet");
}


// static
Status FunctionManager::unload(const std::string &name,
                               const std::vector<std::string> &funcs) {
    return instance().loadInternal(name, funcs);
}


Status FunctionManager::unloadInternal(const std::string &name,
                                       const std::vector<std::string> &funcs) {
    UNUSED(name);
    UNUSED(funcs);
    return Status::Error("Dynamic function unloading not supported yet");
}

}   // namespace nebula
