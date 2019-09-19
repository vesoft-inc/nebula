/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "filter/FunctionManager.h"
#include "time/WallClock.h"

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
            return time::WallClock::fastNowInSec();
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
        auto &attr = functions_["lower"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            auto value = Expression::asString(args[0]);
            std::transform(value.begin(), value.end(), value.begin(),
                           [](unsigned char c){ return std::tolower(c);});
            return value;
        };
    }
    {
        auto &attr = functions_["upper"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            auto value = Expression::asString(args[0]);
            std::transform(value.begin(), value.end(), value.begin(),
                           [](unsigned char c){ return std::toupper(c);});
            return value;
        };
    }
    {
        auto &attr = functions_["length"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            auto value = Expression::asString(args[0]);
            return static_cast<int64_t>(value.length());
        };
    }
    {
        auto &attr = functions_["trim"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            auto value = Expression::asString(args[0]);
            value.erase(0, value.find_first_not_of(" "));
            value.erase(value.find_last_not_of(" ") + 1);
            return value;
        };
    }
    {
        auto &attr = functions_["ltrim"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            auto value = Expression::asString(args[0]);
            value.erase(0, value.find_first_not_of(" "));
            return value;
        };
    }
    {
        auto &attr = functions_["rtrim"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            auto value = Expression::asString(args[0]);
            value.erase(value.find_last_not_of(" ") + 1);
            return value;
        };
    }
    {
        auto &attr = functions_["left"];
        attr.minArity_ = 2;
        attr.maxArity_ = 2;
        attr.body_ = [] (const auto &args) {
            auto value = Expression::asString(args[0]);
            auto length = Expression::asInt(args[1]);
            if (length < 0) {
                length = 0;
            }
            return value.substr(0, length);
        };
    }
    {
        auto &attr = functions_["right"];
        attr.minArity_ = 2;
        attr.maxArity_ = 2;
        attr.body_ = [] (const auto &args) {
            auto value  = Expression::asString(args[0]);
            auto length = Expression::asInt(args[1]);
            if (length <= 0) {
                length = 0;
            }
            return value.substr(value.size() - length);
        };
    }
    {
        auto &attr = functions_["lpad"];
        attr.minArity_ = 3;
        attr.maxArity_ = 3;
        attr.body_ = [] (const auto &args) {
            auto value = Expression::asString(args[0]);
            size_t size  = Expression::asInt(args[1]);

            if (size < 0) {
                return std::string("");
            } else if (size < value.size()) {
                return value.substr(0, static_cast<int32_t>(size));
            } else {
                auto extra = Expression::asString(args[2]);
                size -= value.size();
                std::stringstream stream;
                while (size > extra.size()) {
                    stream << extra;
                    size -= extra.size();
                }
                stream << extra.substr(0, size);
                stream << value;
                return stream.str();
            }
        };
    }
    {
        auto &attr = functions_["rpad"];
        attr.minArity_ = 3;
        attr.maxArity_ = 3;
        attr.body_ = [] (const auto &args) {
            auto value = Expression::asString(args[0]);
            size_t size  = Expression::asInt(args[1]);
            if (size < 0) {
                return std::string("");
            } else if (size < value.size()) {
                return value.substr(0, static_cast<int32_t>(size));
            } else {
                auto extra = Expression::asString(args[2]);
                std::stringstream stream;
                stream << value;
                size -= value.size();
                while (size > extra.size()) {
                    stream << extra;
                    size -= extra.size();
                }
                stream << extra.substr(0, size);
                return stream.str();
            }
        };
    }
    {
        auto &attr = functions_["substr"];
        attr.minArity_ = 3;
        attr.maxArity_ = 3;
        attr.body_ = [] (const auto &args) {
            auto value   = Expression::asString(args[0]);
            auto start   = Expression::asInt(args[1]);
            auto length  = Expression::asInt(args[2]);
            if (static_cast<size_t>(std::abs(start)) > value.size() ||
                length <= 0 || start == 0) {
                return std::string("");
            }

            if (start > 0) {
                return value.substr(start - 1, length);
            } else {
                return value.substr(value.size() + start, length);
            }
        };
    }
    {
        // 64bit signed hash value
        auto &attr = functions_["hash"];
        attr.minArity_ = 1;
        attr.maxArity_ = 1;
        attr.body_ = [] (const auto &args) {
            switch (args[0].which()) {
                case 0: {
                    auto v = Expression::asInt(args[0]);
                    return static_cast<int64_t>(std::hash<int64_t>()(v));
                }
                case 1: {
                    auto v = Expression::asDouble(args[0]);
                    return static_cast<int64_t>(std::hash<double>()(v));
                }
                case 2: {
                    auto v = Expression::asBool(args[0]);
                    return static_cast<int64_t>(std::hash<bool>()(v));
                }
                case 3: {
                    auto &v = Expression::asString(args[0]);
                    return static_cast<int64_t>(std::hash<std::string>()(v));
                }
                default:
                    return INT64_MIN;
            }
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
        if (minArity == maxArity) {
            return Status::Error("Arity not match for function `%s': "
                                 "provided %lu but %lu expected.",
                                 func.c_str(), arity, minArity);
        } else {
            return Status::Error("Arity not match for function `%s': "
                                 "provided %lu but %lu-%lu expected.",
                                 func.c_str(), arity, minArity, maxArity);
        }
    }
    return iter->second.body_;
}

// static
Status FunctionManager::load(const std::string &name,
                             const std::vector<std::string> &funcs) {
    return instance().loadInternal(name, funcs);
}


Status FunctionManager::loadInternal(const std::string &,
                                     const std::vector<std::string> &) {
    return Status::Error("Dynamic function loading not supported yet");
}


// static
Status FunctionManager::unload(const std::string &name,
                               const std::vector<std::string> &funcs) {
    return instance().loadInternal(name, funcs);
}


Status FunctionManager::unloadInternal(const std::string &,
                                       const std::vector<std::string> &) {
    return Status::Error("Dynamic function unloading not supported yet");
}

}   // namespace nebula
