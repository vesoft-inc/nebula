/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_AGGREGATEFUNCTION_H
#define GRAPH_AGGREGATEFUNCTION_H

#include "base/Base.h"
#include "base/FlattenList.h"

namespace nebula {
namespace graph {

namespace ColumnType {
    const auto bool_type = cpp2::ColumnValue::Type::bool_val;
    const auto int_type = cpp2::ColumnValue::Type::integer;
    const auto id_type = cpp2::ColumnValue::Type::id;
    const auto float_type = cpp2::ColumnValue::Type::single_precision;
    const auto double_type = cpp2::ColumnValue::Type::double_precision;
    const auto str_type = cpp2::ColumnValue::Type::str;
    const auto timestamp_type = cpp2::ColumnValue::Type::timestamp;
    const auto year_type = cpp2::ColumnValue::Type::year;
    const auto month_type = cpp2::ColumnValue::Type::month;
    const auto date_type = cpp2::ColumnValue::Type::date;
    const auto datetime_type = cpp2::ColumnValue::Type::datetime;
    const auto path_type = cpp2::ColumnValue::Type::path;
    const auto empty_type = cpp2::ColumnValue::Type::__EMPTY__;
}  // namespace ColumnType

constexpr char kCount[] = "COUNT";
constexpr char kCountDist[] = "COUNT_DISTINCT";
constexpr char kSum[] = "SUM";
constexpr char kAvg[] = "AVG";
constexpr char kMax[] = "MAX";
constexpr char kMin[] = "MIN";
constexpr char kStd[] = "STD";
constexpr char kBitAnd[] = "BIT_AND";
constexpr char kBitOr[] = "BIT_OR";
constexpr char kBitXor[] = "BIT_XOR";
constexpr char kCollectList[] = "COLLECT_LIST";
constexpr char kCollectSet[] = "COLLECT_SET";

class GroupByHash {
public:
    GroupByHash() = default;
    ~GroupByHash() = default;

    static std::size_t getHashVal(const cpp2::ColumnValue &col) {
        switch (col.getType()) {
            case ColumnType::bool_type: {
                return std::hash<bool>()(col.get_bool_val());
            }
            case ColumnType::int_type: {
                return std::hash<int64_t>()(col.get_integer());
            }
            case ColumnType::id_type: {
                return std::hash<int64_t>()(col.get_id());
            }
            case ColumnType::float_type: {
                return std::hash<double>()(col.get_single_precision());
            }
            case ColumnType::double_type: {
                return std::hash<double>()(col.get_double_precision());
            }
            case ColumnType::str_type: {
                return std::hash<std::string>()(col.get_str());
            }
            case ColumnType::timestamp_type: {
                return std::hash<int64_t>()(col.get_timestamp());
            }
            case ColumnType::empty_type: {
                return 0;
            }
            default: {
                LOG(ERROR) << "Untreated value type: " << static_cast<int32_t>(col.getType());
                return 0;
            }
        }
        return 0;
    }
};


struct ColVal {
    cpp2::ColumnValue col;

    bool operator==(const ColVal &other) const {
        return col == other.col;
    }
};


struct ColVals {
    std::vector<cpp2::ColumnValue> vec;

    bool operator==(const ColVals &other) const {
        return vec == other.vec;
    }
};


struct ColHasher {
    std::size_t operator()(const ColVal& k) const {
        return GroupByHash::getHashVal(k.col);
    }
};


struct ColsHasher {
    std::size_t operator()(const ColVals& k) const {
        auto hash_val = GroupByHash::getHashVal(k.vec[0]);
        for (auto i = 1u; i< k.vec.size(); i++) {
            hash_val ^= (GroupByHash::getHashVal(k.vec[i]) << 1);
        }
        return hash_val;
    }
};


class AggFun {
public:
    AggFun() {}
    virtual ~AggFun() {}

public:
    virtual void apply(cpp2::ColumnValue &val) = 0;
    virtual cpp2::ColumnValue getResult() = 0;
};


class Group final : public AggFun {
public:
    void apply(cpp2::ColumnValue &val) override {
        col_ = val;
    }

    cpp2::ColumnValue getResult() override {
        return col_;
    }

private:
    cpp2::ColumnValue         col_;
};


class Count final : public AggFun {
public:
    void apply(cpp2::ColumnValue &val) override {
        UNUSED(val);
        count_++;
    }

    cpp2::ColumnValue getResult() override {
        cpp2::ColumnValue col;
        col.set_integer(count_);
        return col;
    }

private:
    uint64_t            count_{0};
};


class Sum final : public AggFun {
public:
    void apply(cpp2::ColumnValue &val) override {
        if (val.getType() != ColumnType::int_type &&
            val.getType() != ColumnType::double_type &&
            val.getType() != ColumnType::id_type &&
            val.getType() != ColumnType::timestamp_type) {
            return;
        }
        if (!has_) {
            sum_ = val;
            has_ = true;
        } else {
            if (sum_.getType() != val.getType()) {
                return;
            }
            switch (sum_.getType()) {
                case ColumnType::int_type:
                    sum_.set_integer(sum_.get_integer() + val.get_integer());
                    break;
                case ColumnType::id_type:
                    sum_.set_id(sum_.get_id() + val.get_id());
                    break;
                case ColumnType::double_type:
                    sum_.set_double_precision(sum_.get_double_precision() +
                                              val.get_double_precision());
                    break;
                case ColumnType::timestamp_type:
                    sum_.set_timestamp(sum_.get_timestamp() + val.get_timestamp());
                    break;
                default:
                    break;
            }
        }
    }

    cpp2::ColumnValue getResult() override {
        if (sum_.getType() == cpp2::ColumnValue::Type::__EMPTY__) {
            sum_.set_integer(0);
        }
        return sum_;
    }

private:
    cpp2::ColumnValue     sum_;
    bool                  has_{false};
};


class Avg final : public AggFun {
public:
    void apply(cpp2::ColumnValue &val) override {
        sum_.apply(val);
        count_++;
    }

    cpp2::ColumnValue getResult() override {
        auto sum = sum_.getResult();
        if (sum.getType() == ColumnType::empty_type) {
            sum.set_double_precision(0.0);
            return sum;
        }
        double dSum = 0.0;
        if (sum.getType() == ColumnType::int_type) {
            dSum = static_cast<double>(sum.get_integer());
        } else {
            dSum = sum.get_double_precision();
        }
        sum.set_double_precision(dSum / count_);
        return sum;
    }

private:
    Sum                 sum_;
    uint64_t            count_{0};
};


class CountDistinct final : public AggFun {
public:
    void apply(cpp2::ColumnValue &val) override {
        ColVal col;
        col.col = val;
        vec_.emplace(std::move(col));
    }

    cpp2::ColumnValue getResult() override {
        cpp2::ColumnValue col;
        col.set_integer(vec_.size());
        return col;
    }

private:
    std::unordered_set<ColVal, ColHasher> vec_;
};


class Max final : public AggFun {
public:
    void apply(cpp2::ColumnValue &val) override {
        if (!has_) {
            max_ = val;
            has_ = true;
        } else if (val > max_) {
            max_ = val;
        }
    }

    cpp2::ColumnValue getResult() override {
        return max_;
    }

private:
    bool                  has_{false};
    cpp2::ColumnValue     max_;
};


class Min final : public AggFun {
public:
    void apply(cpp2::ColumnValue &val) override {
        if (!has_) {
            min_ = val;
            has_ = true;
        } else if (val < min_) {
            min_ = val;
        }
    }

    cpp2::ColumnValue getResult() override {
        return min_;
    }

private:
    bool                    has_{false};
    cpp2::ColumnValue       min_;
};


class Stdev final : public AggFun {
public:
    void apply(cpp2::ColumnValue &val) override {
        avg_.apply(val);
        if (val.getType() == ColumnType::int_type) {
            auto doubleVal = static_cast<double>(val.get_integer());
            elems_.emplace_back(doubleVal);
        } else if (val.getType() == ColumnType::double_type) {
            auto doubleVal = val.get_double_precision();
            elems_.emplace_back(doubleVal);
        }
    }

    cpp2::ColumnValue getResult() override {
        cpp2::ColumnValue result;
        if (elems_.empty()) {
            result.set_integer(0);
            return result;
        }
        double acc = 0.0;
        double avg = avg_.getResult().get_double_precision();
        std::for_each(std::begin(elems_), std::end(elems_), [&](const double elem) {
            acc += (elem - avg) * (elem - avg);
        });

        double stdev = std::sqrt(acc/elems_.size());
        result.set_double_precision(stdev);
        return result;
    }

private:
    Avg                          avg_;
    std::vector<double>          elems_;
};


class BitAnd final : public AggFun {
public:
    void apply(cpp2::ColumnValue &val) override {
        if (val.getType() != ColumnType::int_type) {
            return;
        }
        if (!has_) {
            bitAnd_ = val;
            has_ = true;
        } else {
            auto temp = bitAnd_.get_integer();
            temp &= val.get_integer();
            bitAnd_.set_integer(temp);
        }
    }

    cpp2::ColumnValue getResult() override {
        if (bitAnd_.getType() == ColumnType::empty_type) {
            bitAnd_.set_integer(0);
        }
        return bitAnd_;
    }

private:
    bool                 has_{false};
    cpp2::ColumnValue    bitAnd_;
};


class BitOr final : public AggFun {
public:
    void apply(cpp2::ColumnValue &val) override {
        if (val.getType() != ColumnType::int_type) {
            return;
        }
        if (!has_) {
            bitOr_ = val;
            has_ = true;
        } else {
            auto temp = bitOr_.get_integer();
            temp |= val.get_integer();
            bitOr_.set_integer(temp);
        }
    }

    cpp2::ColumnValue getResult() override {
        if (bitOr_.getType() == ColumnType::empty_type) {
            bitOr_.set_integer(0);
        }
        return bitOr_;
    }

private:
    bool                 has_{false};
    cpp2::ColumnValue    bitOr_;
};


class BitXor final : public AggFun {
public:
    void apply(cpp2::ColumnValue &val) override {
        if (val.getType() != ColumnType::int_type) {
            return;
        }
        if (!has_) {
            bitXor_ = val;
            has_ = true;
        } else {
            auto temp = bitXor_.get_integer();
            temp ^= val.get_integer();
            bitXor_.set_integer(temp);
        }
    }

    cpp2::ColumnValue getResult() override {
        if (bitXor_.getType() == ColumnType::empty_type) {
            bitXor_.set_integer(0);
        }
        return bitXor_;
    }

private:
    bool                  has_{false};
    cpp2::ColumnValue     bitXor_;
};

class CollectList final : public AggFun {
public:
    void apply(cpp2::ColumnValue &val) override {
        switch(val.getType()) {
            case ColumnType::int_type:
                writer_ << val.get_integer();
                break;
            case ColumnType::id_type:
                writer_ << val.get_id();
                break;
            case ColumnType::timestamp_type:
                writer_ << val.get_timestamp();
                break;
            case ColumnType::bool_type:
                writer_ << val.get_bool_val();
                break;
            case ColumnType::double_type:
                writer_ << val.get_double_precision();
                break;
            case ColumnType::str_type:
                writer_ << val.get_str();
                break;
            default:
                return;
        }
    }

    cpp2::ColumnValue getResult() override {
        cpp2::ColumnValue result;
        result.set_str(writer_.finish());
        return result;
    }
private:
    FlattenListWriter writer_;
};

class CollectSet final : public AggFun {
public:
    void apply(cpp2::ColumnValue &val) override {
        switch(val.getType()) {
            case ColumnType::int_type:
                int_values_.insert(val.get_integer());
                break;
            case ColumnType::id_type:
                int_values_.insert(val.get_id());
                break;
            case ColumnType::timestamp_type:
                int_values_.insert(val.get_timestamp());
                break;
            case ColumnType::bool_type:
                bool_values_.insert(static_cast<int64_t>(val.get_bool_val()));
                break;
            case ColumnType::double_type:
                double_values_.insert(val.get_double_precision());
                break;
            case ColumnType::str_type:
                str_values_.insert(val.get_str());
                break;
            default:
                return;
        }
    }

    cpp2::ColumnValue getResult() override {
        for (auto& item : int_values_) {
            writer_ << item;
        }

        for (auto& item : bool_values_) {
            writer_ << item;
        }

        for (auto& item : double_values_) {
            writer_ << item;
        }

        for (auto& item : str_values_) {
            writer_ << item;
        }

        cpp2::ColumnValue result;
        result.set_str(writer_.finish());
        return result;
    }
private:
    std::set<int64_t>       int_values_;
    std::set<bool>          bool_values_;
    std::set<double>        double_values_;
    std::set<std::string>   str_values_;
    FlattenListWriter       writer_;
};

static std::unordered_map<std::string, std::function<std::shared_ptr<AggFun>()>> funVec = {
    { "", []() -> auto { return std::make_shared<Group>();} },
    { kCount, []() -> auto { return std::make_shared<Count>();} },
    { kCountDist, []() -> auto { return std::make_shared<CountDistinct>();} },
    { kSum, []() -> auto { return std::make_shared<Sum>();} },
    { kAvg, []() -> auto { return std::make_shared<Avg>();} },
    { kMax, []() -> auto { return std::make_shared<Max>();} },
    { kMin, []() -> auto { return std::make_shared<Min>();} },
    { kStd, []() -> auto { return std::make_shared<Stdev>();} },
    { kBitAnd, []() -> auto { return std::make_shared<BitAnd>();} },
    { kBitOr, []() -> auto { return std::make_shared<BitOr>();} },
    { kBitXor, []() -> auto { return std::make_shared<BitXor>();} },
    { kCollectList, []() -> auto { return std::make_shared<CollectList>();} },
    { kCollectSet, []() -> auto { return std::make_shared<CollectSet>();} },
};


}  // namespace graph
}  // namespace nebula


#endif  // GRAPH_AGGREGATEFUNCTION_H
