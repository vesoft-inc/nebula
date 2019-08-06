/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_GROUPBYEXECUTOR_H_
#define GRAPH_GROUPBYEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"

namespace nebula {
namespace graph {

class GroupByExecutor final : public TraverseExecutor {
public:
    GroupByExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "GroupByExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void feedResult(std::unique_ptr<InterimResult> result) override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

    static std::size_t getHashVal(const cpp2::ColumnValue &iCol) {
        using Type = cpp2::ColumnValue::Type;
        cpp2::ColumnValue col = iCol;
        switch (col.getType()) {
            case Type::bool_val: {
                return std::hash<bool>()(col.get_bool_val());
            }
            case Type::integer: {
                return std::hash<int64_t>()(col.get_integer());
            }
            case Type::id: {
                return std::hash<int64_t>()(col.get_id());
            }
            case Type::single_precision: {
                return std::hash<double>()(col.get_single_precision());
            }
            case Type::double_precision: {
                return std::hash<double>()(col.get_double_precision());
            }
            case Type::str: {
                return std::hash<std::string>()(col.get_str());
            }
            case Type::timestamp: {
                return std::hash<int64_t>()(col.get_timestamp());
            }
            // TODO:
            case Type::__EMPTY__:
            case Type::year:
            case Type::month:
            case Type::date:
            case Type::datetime: {
                return 0;
            }
        }
        return 0;
    }

private:
    Status prepareGroup();
    Status prepareYield();
    Status buildIndex();

    void GroupingData();
    void generateOutputSchema();

    std::vector<std::string> getResultColumnNames() const;
    std::unique_ptr<InterimResult> setupInterimResult();

private:
    struct ColVal {
        cpp2::ColumnValue col;
        bool operator==(const ColVal &other) const {
            if (col != other.col) {
                return false;
            }
            return true;
        }
    };

    struct ColVals {
        std::vector<cpp2::ColumnValue> vec;
        bool operator==(const ColVals &other) const {
            if (vec.size() != other.vec.size()) {
                return false;
            }
            for (auto i = 0u; i < vec.size(); i++) {
                if (vec[i] != other.vec[i]) {
                    return false;
                }
            }
            return true;
        }
    };

    struct ColHasher {
        std::size_t operator()(const ColVal& k) const {
            return GroupByExecutor::getHashVal(k.col);
        }
    };

    struct ColsHasher {
        std::size_t operator()(const ColVals& k) const {
            auto hash_val = GroupByExecutor::getHashVal(k.vec[0]);
            for (auto i = 1u; i< k.vec.size(); i++) {
                hash_val ^= (GroupByExecutor::getHashVal(k.vec[i]) << 1);
            }
            return hash_val;
        }
    };

    class AggFun {
    public:
        virtual void apply(cpp2::ColumnValue &val) { UNUSED(val); }
        virtual void apply() {}
        virtual cpp2::ColumnValue getResult() = 0;
    };

    class Group: public AggFun {
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

    class Count: public AggFun {
    public:
        void apply() override {
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

    class Sum: public AggFun{
    public:
        void apply(cpp2::ColumnValue &val) override {
            if (!has_) {
                if (val.getType() == cpp2::ColumnValue::Type::integer) {
                    sum_.set_integer(val.get_integer());
                } else if (val.getType() == cpp2::ColumnValue::Type::double_precision) {
                    sum_.set_double_precision(val.get_double_precision());
                }
                has_ = true;
            } else {
                if (val.getType() == cpp2::ColumnValue::Type::integer) {
                    sum_.set_integer(sum_.get_integer() + val.get_integer());
                } else if (val.getType() == cpp2::ColumnValue::Type::double_precision) {
                    sum_.set_integer(sum_.get_integer() + val.get_double_precision());
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
        cpp2::ColumnValue   sum_;
        bool                has_{false};
    };

    class Avg: public AggFun{
    public:
        void apply(cpp2::ColumnValue &val) override {
            if (!has_) {
                if (val.getType() == cpp2::ColumnValue::Type::integer) {
                    sum_.set_integer(val.get_integer());
                } else if (val.getType() == cpp2::ColumnValue::Type::double_precision) {
                    sum_.set_double_precision(val.get_double_precision());
                }
                has_ = true;
            } else {
                if (val.getType() == cpp2::ColumnValue::Type::integer) {
                    sum_.set_integer(sum_.get_integer() + val.get_integer());
                } else if (val.getType() == cpp2::ColumnValue::Type::double_precision) {
                    sum_.set_double_precision(sum_.get_double_precision() +
                                                val.get_double_precision());
                }
            }
            count_++;
        }

        cpp2::ColumnValue getResult() override {
            if (sum_.getType() == cpp2::ColumnValue::Type::integer) {
                sum_.set_integer(sum_.get_integer() / count_);
            } else if (sum_.getType() == cpp2::ColumnValue::Type::double_precision) {
                sum_.set_double_precision(sum_.get_double_precision() / count_);
            }
            if (sum_.getType() == cpp2::ColumnValue::Type::__EMPTY__) {
                sum_.set_integer(0);
            }
            return sum_;
        }

    private:
        cpp2::ColumnValue sum_;
        uint64_t          count_{0};
        bool              has_{false};
    };

    class CountDistinct: public AggFun{
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

    class Max: public AggFun{
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
        bool        has_{false};
        cpp2::ColumnValue max_;
    };

    class Min: public AggFun{
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
        bool        has_{false};
        cpp2::ColumnValue min_;
    };


private:
    GroupBySentence                                          *sentence_{nullptr};
    std::shared_ptr<const meta::SchemaProviderIf>             schema_;
    std::vector<cpp2::RowValue>                               rows_;
    std::vector<YieldColumn*>                                 yields_;
    std::vector<GroupColumn*>                                 groups_;

    struct ColType {
        std::string                      fieldName_;
        nebula::cpp2::SupportedType      type_;                // type
        uint64_t                         index_;               // the index of input data
        FunKind                          fun_;                 // fun to cal the col
    };

    // fieldName -> index of input data
    using GroupsCols = std::unordered_map<std::string, uint64_t >;
    using YieldsCols = std::vector<ColType>;

    GroupsCols                                                 groupCols_;
    YieldsCols                                                 yieldCols_;
    std::shared_ptr<SchemaWriter>                              resultSchema_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_GROUPBYEXECUTOR_H
