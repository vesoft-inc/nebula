/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/function/AggregateFunction.h"

namespace nebula {
class AggregateFunctionTest : public ::testing::Test {
public:
    static void SetUpTestCase() {
        vals1_ = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        vals2_ = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        vals2_.emplace_back(Value(NullType::__NULL__));
        vals2_.emplace_back(Value());
        vals3_ = std::vector<Value>(10, Value::kNullValue);
        vals4_ = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9};
        vals4_.emplace_back(Value(NullType::__NULL__));
        vals4_.emplace_back(Value());
    }
protected:
    static std::vector<Value>  vals1_;
    static std::vector<Value>  vals2_;
    static std::vector<Value>  vals3_;
    static std::vector<Value>  vals4_;
};

std::vector<Value>  AggregateFunctionTest::vals1_;
std::vector<Value>  AggregateFunctionTest::vals2_;
std::vector<Value>  AggregateFunctionTest::vals3_;
std::vector<Value>  AggregateFunctionTest::vals4_;

TEST_F(AggregateFunctionTest, Group) {
    auto group = AggFun::aggFunMap_[AggFun::Function::kNone](false);
    group->apply(1);
    EXPECT_EQ(group->getResult(), 1);
}

TEST_F(AggregateFunctionTest, Count) {
    {
        auto cnt = AggFun::aggFunMap_[AggFun::Function::kCount](false);
        for (auto& val : vals1_) {
            cnt->apply(val);
        }
        EXPECT_EQ(cnt->getResult(), 10);
    }
    {
        auto cnt = AggFun::aggFunMap_[AggFun::Function::kCount](false);
        for (auto& val : vals2_) {
            cnt->apply(val);
        }
        EXPECT_EQ(cnt->getResult(), 10);
    }
    {
        auto cnt = AggFun::aggFunMap_[AggFun::Function::kCount](false);
        for (auto& val : vals3_) {
            cnt->apply(val);
        }
        EXPECT_EQ(cnt->getResult(), 0);
    }
    {
        auto cnt = AggFun::aggFunMap_[AggFun::Function::kCount](true);
        for (auto& val : vals1_) {
            cnt->apply(val);
        }
        EXPECT_EQ(cnt->getResult(), 10);
    }
    {
        auto cnt = AggFun::aggFunMap_[AggFun::Function::kCount](true);
        for (auto& val : vals2_) {
            cnt->apply(val);
        }
        EXPECT_EQ(cnt->getResult(), 10);
    }
    {
        auto cnt = AggFun::aggFunMap_[AggFun::Function::kCount](true);
        for (auto& val : vals3_) {
            cnt->apply(val);
        }
        EXPECT_EQ(cnt->getResult(), 0);
    }
    {
        auto cnt = AggFun::aggFunMap_[AggFun::Function::kCount](true);
        for (auto& val : vals4_) {
            cnt->apply(val);
        }
        EXPECT_EQ(cnt->getResult(), 10);
    }
}

TEST_F(AggregateFunctionTest, Sum) {
    {
        auto sum = AggFun::aggFunMap_[AggFun::Function::kSum](false);
        for (auto& val : vals1_) {
            sum->apply(val);
        }
        EXPECT_EQ(sum->getResult(), 45);
    }
    {
        auto sum = AggFun::aggFunMap_[AggFun::Function::kSum](false);
        for (auto& val : vals2_) {
            sum->apply(val);
        }
        EXPECT_EQ(sum->getResult(), 45);
    }
    {
        auto sum = AggFun::aggFunMap_[AggFun::Function::kSum](false);
        for (auto& val : vals3_) {
            sum->apply(val);
        }
        EXPECT_EQ(sum->getResult(), Value::kNullValue);
    }
    {
        auto sum = AggFun::aggFunMap_[AggFun::Function::kSum](true);
        for (auto& val : vals1_) {
            sum->apply(val);
        }
        EXPECT_EQ(sum->getResult(), 45);
    }
    {
        auto sum = AggFun::aggFunMap_[AggFun::Function::kSum](true);
        for (auto& val : vals2_) {
            sum->apply(val);
        }
        EXPECT_EQ(sum->getResult(), 45);
    }
    {
        auto sum = AggFun::aggFunMap_[AggFun::Function::kSum](true);
        for (auto& val : vals3_) {
            sum->apply(val);
        }
        EXPECT_EQ(sum->getResult(), Value::kNullValue);
    }
    {
        auto sum = AggFun::aggFunMap_[AggFun::Function::kSum](true);
        for (auto& val : vals4_) {
            sum->apply(val);
        }
        EXPECT_EQ(sum->getResult(), 45);
    }
}

TEST_F(AggregateFunctionTest, Avg) {
    {
        auto avg = AggFun::aggFunMap_[AggFun::Function::kAvg](false);
        for (auto& val : vals1_) {
            avg->apply(val);
        }
        EXPECT_EQ(avg->getResult(), 4.5);
    }
    {
        auto avg = AggFun::aggFunMap_[AggFun::Function::kAvg](false);
        for (auto& val : vals2_) {
            avg->apply(val);
        }
        EXPECT_EQ(avg->getResult(), 4.5);
    }
    {
        auto avg = AggFun::aggFunMap_[AggFun::Function::kAvg](false);
        for (auto& val : vals3_) {
            avg->apply(val);
        }
        EXPECT_EQ(avg->getResult(), Value::kNullValue);
    }
    {
        auto avg = AggFun::aggFunMap_[AggFun::Function::kAvg](true);
        for (auto& val : vals1_) {
            avg->apply(val);
        }
        EXPECT_EQ(avg->getResult(), 4.5);
    }
    {
        auto avg = AggFun::aggFunMap_[AggFun::Function::kAvg](true);
        for (auto& val : vals2_) {
            avg->apply(val);
        }
        EXPECT_EQ(avg->getResult(), 4.5);
    }
    {
        auto avg = AggFun::aggFunMap_[AggFun::Function::kAvg](true);
        for (auto& val : vals3_) {
            avg->apply(val);
        }
        EXPECT_EQ(avg->getResult(), Value::kNullValue);
    }
    {
        auto avg = AggFun::aggFunMap_[AggFun::Function::kAvg](true);
        for (auto& val : vals4_) {
            avg->apply(val);
        }
        EXPECT_EQ(avg->getResult(), 4.5);
    }
}

TEST_F(AggregateFunctionTest, Max) {
    {
        auto max = AggFun::aggFunMap_[AggFun::Function::kMax](false);
        for (auto& val : vals1_) {
            max->apply(val);
        }
        EXPECT_EQ(max->getResult(), 9);
    }
    {
        auto max = AggFun::aggFunMap_[AggFun::Function::kMax](false);
        for (auto& val : vals2_) {
            max->apply(val);
        }
        EXPECT_EQ(max->getResult(), 9);
    }
    {
        auto max = AggFun::aggFunMap_[AggFun::Function::kMax](false);
        for (auto& val : vals3_) {
            max->apply(val);
        }
        EXPECT_EQ(max->getResult(), Value::kNullValue);
    }
    {
        auto max = AggFun::aggFunMap_[AggFun::Function::kMax](true);
        for (auto& val : vals1_) {
            max->apply(val);
        }
        EXPECT_EQ(max->getResult(), 9);
    }
    {
        auto max = AggFun::aggFunMap_[AggFun::Function::kMax](true);
        for (auto& val : vals2_) {
            max->apply(val);
        }
        EXPECT_EQ(max->getResult(), 9);
    }
    {
        auto max = AggFun::aggFunMap_[AggFun::Function::kMax](true);
        for (auto& val : vals3_) {
            max->apply(val);
        }
        EXPECT_EQ(max->getResult(), Value::kNullValue);
    }
    {
        auto max = AggFun::aggFunMap_[AggFun::Function::kMax](true);
        for (auto& val : vals4_) {
            max->apply(val);
        }
        EXPECT_EQ(max->getResult(), 9);
    }
}

TEST_F(AggregateFunctionTest, Min) {
    {
        auto min = AggFun::aggFunMap_[AggFun::Function::kMin](false);
        for (auto& val : vals1_) {
            min->apply(val);
        }
        EXPECT_EQ(min->getResult(), 0);
    }
    {
        auto min = AggFun::aggFunMap_[AggFun::Function::kMin](false);
        for (auto& val : vals2_) {
            min->apply(val);
        }
        EXPECT_EQ(min->getResult(), 0);
    }
    {
        auto min = AggFun::aggFunMap_[AggFun::Function::kMin](false);
        for (auto& val : vals3_) {
            min->apply(val);
        }
        EXPECT_EQ(min->getResult(), Value::kNullValue);
    }
    {
        auto min = AggFun::aggFunMap_[AggFun::Function::kMin](true);
        for (auto& val : vals1_) {
            min->apply(val);
        }
        EXPECT_EQ(min->getResult(), 0);
    }
    {
        auto min = AggFun::aggFunMap_[AggFun::Function::kMin](true);
        for (auto& val : vals2_) {
            min->apply(val);
        }
        EXPECT_EQ(min->getResult(), 0);
    }
    {
        auto min = AggFun::aggFunMap_[AggFun::Function::kMin](true);
        for (auto& val : vals3_) {
            min->apply(val);
        }
        EXPECT_EQ(min->getResult(), Value::kNullValue);
    }
    {
        auto min = AggFun::aggFunMap_[AggFun::Function::kMin](true);
        for (auto& val : vals4_) {
            min->apply(val);
        }
        EXPECT_EQ(min->getResult(), 0);
    }
}

TEST_F(AggregateFunctionTest, Stdev) {
    {
        auto stdev = AggFun::aggFunMap_[AggFun::Function::kStdev](false);
        for (auto& val : vals1_) {
            stdev->apply(val);
        }
        EXPECT_EQ(stdev->getResult(), 2.87228132327);
    }
    {
        auto stdev = AggFun::aggFunMap_[AggFun::Function::kStdev](false);
        for (auto& val : vals2_) {
            stdev->apply(val);
        }
        EXPECT_EQ(stdev->getResult(), 2.87228132327);
    }
    {
        auto stdev = AggFun::aggFunMap_[AggFun::Function::kStdev](false);
        for (auto& val : vals3_) {
            stdev->apply(val);
        }
        EXPECT_EQ(stdev->getResult(), Value::kNullValue);
    }
    {
        auto stdev = AggFun::aggFunMap_[AggFun::Function::kStdev](true);
        for (auto& val : vals1_) {
            stdev->apply(val);
        }
        EXPECT_EQ(stdev->getResult(), 2.87228132327);
    }
    {
        auto stdev = AggFun::aggFunMap_[AggFun::Function::kStdev](true);
        for (auto& val : vals2_) {
            stdev->apply(val);
        }
        EXPECT_EQ(stdev->getResult(), 2.87228132327);
    }
    {
        auto stdev = AggFun::aggFunMap_[AggFun::Function::kStdev](true);
        for (auto& val : vals3_) {
            stdev->apply(val);
        }
        EXPECT_EQ(stdev->getResult(), Value::kNullValue);
    }
    {
        auto stdev = AggFun::aggFunMap_[AggFun::Function::kStdev](true);
        for (auto& val : vals4_) {
            stdev->apply(val);
        }
        EXPECT_EQ(stdev->getResult(), 2.87228132327);
    }
}

TEST_F(AggregateFunctionTest, BitAnd) {
    {
        auto bitAnd = AggFun::aggFunMap_[AggFun::Function::kBitAnd](false);
        for (auto& val : vals1_) {
            bitAnd->apply(val);
        }
        EXPECT_EQ(bitAnd->getResult(), 0);
    }
    {
        auto bitAnd = AggFun::aggFunMap_[AggFun::Function::kBitAnd](false);
        for (auto& val : vals2_) {
            bitAnd->apply(val);
        }
        EXPECT_EQ(bitAnd->getResult(), 0);
    }
    {
        auto bitAnd = AggFun::aggFunMap_[AggFun::Function::kBitAnd](false);
        for (auto& val : vals3_) {
            bitAnd->apply(val);
        }
        EXPECT_EQ(bitAnd->getResult(), Value::kNullValue);
    }
    {
        auto bitAnd = AggFun::aggFunMap_[AggFun::Function::kBitAnd](true);
        for (auto& val : vals1_) {
            bitAnd->apply(val);
        }
        EXPECT_EQ(bitAnd->getResult(), 0);
    }
    {
        auto bitAnd = AggFun::aggFunMap_[AggFun::Function::kBitAnd](true);
        for (auto& val : vals2_) {
            bitAnd->apply(val);
        }
        EXPECT_EQ(bitAnd->getResult(), 0);
    }
    {
        auto bitAnd = AggFun::aggFunMap_[AggFun::Function::kBitAnd](true);
        for (auto& val : vals3_) {
            bitAnd->apply(val);
        }
        EXPECT_EQ(bitAnd->getResult(), Value::kNullValue);
    }
    {
        auto bitAnd = AggFun::aggFunMap_[AggFun::Function::kBitAnd](true);
        for (auto& val : vals4_) {
            bitAnd->apply(val);
        }
        EXPECT_EQ(bitAnd->getResult(), 0);
    }
}

TEST_F(AggregateFunctionTest, BitOr) {
    {
        auto bitOr = AggFun::aggFunMap_[AggFun::Function::kBitOr](false);
        for (auto& val : vals1_) {
            bitOr->apply(val);
        }
        EXPECT_EQ(bitOr->getResult(), 15);
    }
    {
        auto bitOr = AggFun::aggFunMap_[AggFun::Function::kBitOr](false);
        for (auto& val : vals2_) {
            bitOr->apply(val);
        }
        EXPECT_EQ(bitOr->getResult(), 15);
    }
    {
        auto bitOr = AggFun::aggFunMap_[AggFun::Function::kBitOr](false);
        for (auto& val : vals3_) {
            bitOr->apply(val);
        }
        EXPECT_EQ(bitOr->getResult(), Value::kNullValue);
    }
    {
        auto bitOr = AggFun::aggFunMap_[AggFun::Function::kBitOr](true);
        for (auto& val : vals1_) {
            bitOr->apply(val);
        }
        EXPECT_EQ(bitOr->getResult(), 15);
    }
    {
        auto bitOr = AggFun::aggFunMap_[AggFun::Function::kBitOr](true);
        for (auto& val : vals2_) {
            bitOr->apply(val);
        }
        EXPECT_EQ(bitOr->getResult(), 15);
    }
    {
        auto bitOr = AggFun::aggFunMap_[AggFun::Function::kBitOr](true);
        for (auto& val : vals3_) {
            bitOr->apply(val);
        }
        EXPECT_EQ(bitOr->getResult(), Value::kNullValue);
    }
    {
        auto bitOr = AggFun::aggFunMap_[AggFun::Function::kBitOr](true);
        for (auto& val : vals4_) {
            bitOr->apply(val);
        }
        EXPECT_EQ(bitOr->getResult(), 15);
    }
}

TEST_F(AggregateFunctionTest, BitXor) {
    {
        auto bitXor = AggFun::aggFunMap_[AggFun::Function::kBitXor](false);
        for (auto& val : vals1_) {
            bitXor->apply(val);
        }
        EXPECT_EQ(bitXor->getResult(), 1);
    }
    {
        auto bitXor = AggFun::aggFunMap_[AggFun::Function::kBitXor](false);
        for (auto& val : vals2_) {
            bitXor->apply(val);
        }
        EXPECT_EQ(bitXor->getResult(), 1);
    }
    {
        auto bitXor = AggFun::aggFunMap_[AggFun::Function::kBitXor](false);
        for (auto& val : vals3_) {
            bitXor->apply(val);
        }
        EXPECT_EQ(bitXor->getResult(), Value::kNullValue);
    }
    {
        auto bitXor = AggFun::aggFunMap_[AggFun::Function::kBitXor](true);
        for (auto& val : vals1_) {
            bitXor->apply(val);
        }
        EXPECT_EQ(bitXor->getResult(), 1);
    }
    {
        auto bitXor = AggFun::aggFunMap_[AggFun::Function::kBitXor](true);
        for (auto& val : vals2_) {
            bitXor->apply(val);
        }
        EXPECT_EQ(bitXor->getResult(), 1);
    }
    {
        auto bitXor = AggFun::aggFunMap_[AggFun::Function::kBitXor](true);
        for (auto& val : vals3_) {
            bitXor->apply(val);
        }
        EXPECT_EQ(bitXor->getResult(), Value::kNullValue);
    }
    {
        auto bitXor = AggFun::aggFunMap_[AggFun::Function::kBitXor](true);
        for (auto& val : vals4_) {
            bitXor->apply(val);
        }
        EXPECT_EQ(bitXor->getResult(), 1);
    }
}

TEST_F(AggregateFunctionTest, Collect) {
    {
        auto collect = AggFun::aggFunMap_[AggFun::Function::kCollect](false);
        for (auto& val : vals1_) {
            collect->apply(val);
        }
        Value expected = Value(List(vals1_));
        EXPECT_EQ(collect->getResult(), expected);
    }
    {
        auto collect = AggFun::aggFunMap_[AggFun::Function::kCollect](false);
        for (auto& val : vals2_) {
            collect->apply(val);
        }
        Value expected = Value(List(vals1_));
        EXPECT_EQ(collect->getResult(), expected);
    }
    {
        auto collect = AggFun::aggFunMap_[AggFun::Function::kCollect](false);
        for (auto& val : vals3_) {
            collect->apply(val);
        }
        Value expected = Value(List());
        EXPECT_EQ(collect->getResult(), expected);
    }
    {
        auto collect = AggFun::aggFunMap_[AggFun::Function::kCollect](true);
        for (auto& val : vals1_) {
            collect->apply(val);
        }
        List result = collect->getResult().getList();
        std::sort(result.values.begin(), result.values.end());
        List expected = List(vals1_);
        EXPECT_EQ(result, expected);
    }
    {
        auto collect = AggFun::aggFunMap_[AggFun::Function::kCollect](true);
        for (auto& val : vals2_) {
            collect->apply(val);
        }
        List result = collect->getResult().getList();
        std::sort(result.values.begin(), result.values.end());
        List expected = List(vals1_);
        EXPECT_EQ(result, expected);
    }
    {
        auto collect = AggFun::aggFunMap_[AggFun::Function::kCollect](true);
        for (auto& val : vals3_) {
            collect->apply(val);
        }
        Value expected = Value(List());
        EXPECT_EQ(collect->getResult(), expected);
    }
    {
        auto collect = AggFun::aggFunMap_[AggFun::Function::kCollect](true);
        for (auto& val : vals4_) {
            collect->apply(val);
        }
        List result = collect->getResult().getList();
        std::sort(result.values.begin(), result.values.end());
        List expected = List(vals1_);
        EXPECT_EQ(result, expected);
    }
}
}  // namespace nebula
