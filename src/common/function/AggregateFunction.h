/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_FUNCTION_AGGREGATEFUNCTION_H_
#define COMMON_FUNCTION_AGGREGATEFUNCTION_H_

#include "common/base/Base.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Set.h"

namespace nebula {

class AggFun {
public:
    enum class Function : uint8_t {
        kNone,
        kCount,
        kSum,
        kAvg,
        kMax,
        kMin,
        kStdev,
        kBitAnd,
        kBitOr,
        kBitXor,
        kCollect,
        kCollectSet,
    };
    explicit AggFun(const bool distinct) : distinct_(distinct) {}
    virtual ~AggFun() {}

public:
    virtual void apply(const Value &val) = 0;
    virtual Value getResult() = 0;

    static std::unordered_map<Function, std::function<std::unique_ptr<AggFun>(bool)>> aggFunMap_;
    static std::unordered_map<std::string, AggFun::Function> nameIdMap_;

protected:
    const bool                  distinct_{false};
    std::unordered_set<Value>   uniques_;
};


class Group final : public AggFun {
public:
    Group() : AggFun(false) {}

    void apply(const Value &val) override {
        col_ = val;
    }

    Value getResult() override {
        return col_;
    }

private:
    Value   col_;
};


class Count final : public AggFun {
public:
    explicit Count(const bool distinct = false) : AggFun(distinct) {}

    void apply(const Value &val) override {
        if (val.isNull() || val.empty()) {
            return;
        }
        if (distinct_) {
            uniques_.emplace(val);
        } else {
            cnt_ = cnt_ + 1;
        }
    }

    Value getResult() override {
        if (distinct_) {
            return static_cast<int64_t>(uniques_.size());
        } else {
            return cnt_;
        }
    }

private:
    Value   cnt_{0};
};


class Sum final : public AggFun {
public:
    explicit Sum(const bool distinct = false) : AggFun(distinct) {}

    void apply(const Value &val) override {
        if (val.isNull() || val.empty()) {
            return;
        }
        if (UNLIKELY(!val.isNumeric())) {
            sum_ = Value(NullType::BAD_TYPE);
            return;
        }
        if (sum_.isBadNull()) {
            return;
        }

        if (distinct_) {
            uniques_.emplace(val);
        } else {
            if (!applied_) {
                sum_ = val;
                applied_ = true;
            } else {
                // TODO: Support += for value.
                sum_ = sum_ + val;
            }
        }
    }

    Value getResult() override {
        if (sum_.isBadNull() || !distinct_ || uniques_.empty()) {
            return sum_;
        }

        DCHECK(distinct_);
        Value sum(0.0);
        for (auto& v : uniques_) {
            sum = sum + v;
        }
        return sum;
    }

private:
    bool    applied_{false};
    Value   sum_{NullType::__NULL__};
};


class Avg final : public AggFun {
public:
    explicit Avg(const bool distinct = false) : AggFun(distinct) {}

    void apply(const Value &val) override {
        if (val.isNull() || val.empty()) {
            return;
        }
        if (UNLIKELY(!val.isNumeric())) {
            avg_ = Value(NullType::BAD_TYPE);
            return;
        }
        if (avg_.isBadNull()) {
            return;
        }

        cnt_ = cnt_ + 1;
        if (distinct_) {
            uniques_.emplace(val);
        } else {
            avg_ = avg_ + (val - avg_) / cnt_;
        }
    }

    Value getResult() override {
        if (cnt_ == 0) {
            return Value::kNullValue;
        }
        if (avg_.isBadNull() || !distinct_) {
            return avg_;
        }
        DCHECK(distinct_);
        Value avg(0.0);
        Value cnt(0.0);
        for (auto& v : uniques_) {
            cnt = cnt + 1;
            avg = avg + (v - avg) / cnt;
        }
        return avg;
    }

private:
    Value               avg_{0.0};
    Value               cnt_{0.0};
};


class Max final : public AggFun {
public:
    explicit Max(const bool distinct = false) : AggFun(distinct) {}

    void apply(const Value &val) override {
        if (val.isNull() || val.empty()) {
            return;
        }

        if (distinct_) {
            uniques_.emplace(val);
        } else {
            if (!applied_ || val > max_) {
                max_ = val;
                applied_ = true;
            }
        }
    }

    Value getResult() override {
        if (distinct_) {
            Value max = Value::kNullValue;
            for (auto& v : uniques_) {
                if (max.isNull() || v > max) {
                    max = v;
                }
            }
            return max;
        } else {
            return max_;
        }
    }

private:
    bool      applied_{false};
    Value     max_{NullType::__NULL__};
};


class Min final : public AggFun {
public:
    explicit Min(const bool distinct) : AggFun(distinct) {}

    void apply(const Value &val) override {
        if (val.isNull() || val.empty()) {
            return;
        }
        if (distinct_) {
            uniques_.emplace(val);
        } else {
            if (!applied_ || val < min_) {
                min_ = val;
                applied_ = true;
            }
        }
    }

    Value getResult() override {
        if (distinct_) {
            Value min = Value::kNullValue;
            for (auto& v : uniques_) {
                if (min.isNull() || v < min) {
                    min = v;
                }
            }
            return min;
        } else {
            return min_;
        }
    }

private:
    bool        applied_{false};
    Value       min_{NullType::__NULL__};
};


class Stdev final : public AggFun {
public:
    explicit Stdev(const bool distinct) : AggFun(distinct) {}

    void apply(const Value &val) override {
        if (val.isNull() || val.empty()) {
            return;
        }
        if (UNLIKELY(!val.isNumeric())) {
            avg_ = Value(NullType::BAD_TYPE);
            var_ = Value(NullType::BAD_TYPE);
            return;
        }
        cnt_ = cnt_ + 1;
        if (distinct_) {
            uniques_.emplace(val);
        } else {
            var_ = (cnt_ - 1) / (cnt_ * cnt_) * ((val - avg_) * (val - avg_))
                + (cnt_ - 1) / cnt_ * var_;
            avg_ = avg_ + (val - avg_) / cnt_;
        }
    }

    Value getResult() override {
        if (cnt_ <= 0) {
            return Value::kNullValue;
        } else if (var_.empty() || var_.isNull()) {
            return var_;
        }

        if (distinct_) {
            Value cnt(0.0);
            Value avg(0.0);
            Value var(0.0);
            for (auto& v : uniques_) {
                cnt = cnt + 1;
                var = (cnt - 1) / (cnt * cnt) * ((v - avg) * (v - avg))
                    + (cnt - 1) / cnt * var;
                avg = avg + (v - avg) / cnt;
            }
            return var.isFloat() ? std::sqrt(var.getFloat()) : Value::kNullBadType;
        } else {
            return var_.isFloat() ? std::sqrt(var_.getFloat()) : Value::kNullBadType;
        }
    }

private:
    Value   avg_{0.0};
    Value   cnt_{0.0};
    Value   var_{0.0};
};


class BitAnd final : public AggFun {
public:
    explicit BitAnd(const bool distinct) : AggFun(distinct) {}

    void apply(const Value &val) override {
        if (val.isNull() || val.empty()) {
            return;
        }
        if (!val.isInt()) {
            result_ = Value::kNullBadType;
            return;
        }
        if (distinct_) {
            uniques_.emplace(val);
        } else {
            if (!applied_) {
                result_ = val;
                applied_ = true;
            } else {
                result_  = result_ & val;
            }
        }
    }

    Value getResult() override {
        if (result_.isBadNull()) return result_;
        if (distinct_ && !uniques_.empty()) {
            Value result = *uniques_.begin();
            for (auto& v : uniques_) {
                result = result & v;
            }
            return result;
        } else {
            return result_;
        }
    }

private:
    bool    applied_{false};
    Value   result_{NullType::__NULL__};
};


class BitOr final : public AggFun {
public:
    explicit BitOr(const bool distinct) : AggFun(distinct) {}

    void apply(const Value &val) override {
        if (val.isNull() || val.empty()) {
            return;
        }
        if (!val.isInt()) {
            result_ = Value::kNullBadType;
            return;
        }
        if (distinct_) {
            uniques_.emplace(val);
        } else {
            if (!applied_) {
                result_ = val;
                applied_ = true;
            } else {
                result_  = result_ | val;
            }
        }
    }

    Value getResult() override {
        if (result_.isBadNull()) return result_;
        if (distinct_ && !uniques_.empty()) {
            Value result = *uniques_.begin();
            for (auto& v : uniques_) {
                result = result | v;
            }
            return result;
        } else {
            return result_;
        }
    }

private:
    bool    applied_{false};
    Value   result_{NullType::__NULL__};
};


class BitXor final : public AggFun {
public:
    explicit BitXor(const bool distinct) : AggFun(distinct) {}

    void apply(const Value &val) override {
        if (val.isNull() || val.empty()) {
            return;
        }
        if (!val.isInt()) {
            result_ = Value::kNullBadType;
            return;
        }
        if (distinct_) {
            uniques_.emplace(val);
        } else {
            if (!applied_) {
                result_ = val;
                applied_ = true;
            } else {
                result_  = result_ ^ val;
            }
        }
    }

    Value getResult() override {
        if (result_.isBadNull()) {
            return result_;
        }
        if (distinct_ && !uniques_.empty()) {
            Value result = *uniques_.begin();
            std::for_each(++uniques_.begin(), uniques_.end(), [&result] (auto& v) {
                result = result ^ v;
            });
            return result;
        } else {
            return result_;
        }
    }

private:
    bool    applied_{false};
    Value   result_{NullType::__NULL__};
};

class Collect final : public AggFun {
public:
    explicit Collect(bool distinct) : AggFun(distinct) {}

    void apply(const Value &val) override {
        if (val.isNull() || val.empty()) {
            return;
        }
        if (distinct_) {
            uniques_.emplace(val);
        } else {
            list_.values.emplace_back(val);
        }
    }

    Value getResult() override {
        if (distinct_ && !uniques_.empty()) {
            list_.values.resize(uniques_.size());
            std::transform(uniques_.begin(), uniques_.end(), list_.values.begin(), [] (auto&& val) {
                return std::move(val);
            });
        }
        return Value(std::move(list_));
    }

private:
    List    list_;
};

class CollectSet final : public AggFun {
public:
    explicit CollectSet(bool distinct) : AggFun(distinct) {}

    void apply(const Value &val) override {
        if (val.isNull() || val.empty()) {
            return;
        }
        result_.values.emplace(val);
    }

    Value getResult() override {
        return Value(std::move(result_));
    }

private:
    Set    result_;
};

}  // namespace nebula

#endif  // COMMON_FUNCTION_AGGREGATEFUNCTION_H_
