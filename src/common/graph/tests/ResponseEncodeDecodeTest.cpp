/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <vector>

#include "common/graph/Response.h"
#include "common/graph/GraphCpp2Ops.h"

namespace nebula {

using serializer = apache::thrift::CompactSerializer;

TEST(ResponseEncodDecodeTest, Basic) {
    // auth response
    {
        std::vector<AuthResponse> resps;
        resps.emplace_back(AuthResponse{});
        resps.emplace_back(AuthResponse{ErrorCode::SUCCEEDED, std::make_unique<int64_t>(1)});
        resps.emplace_back(AuthResponse{ErrorCode::E_DISCONNECTED,
                                        std::make_unique<int64_t>(1),
                                        std::make_unique<std::string>("Error Test.")});
        resps.emplace_back(AuthResponse{ErrorCode::E_DISCONNECTED,
                                        std::make_unique<int64_t>(1),
                                        std::make_unique<std::string>("Error Test."),
                                        std::make_unique<int32_t>(233),
                                        std::make_unique<std::string>("time zone")});
        for (const auto &resp : resps) {
            std::string buf;
            buf.reserve(128);
            serializer::serialize(resp, &buf);
            AuthResponse copy;
            std::size_t s = serializer::deserialize(buf, copy);
            ASSERT_EQ(s, buf.size());
            EXPECT_EQ(resp, copy);
        }
    }
    // response
    {
        std::vector<ExecutionResponse> resps;
        resps.emplace_back(ExecutionResponse{});
        resps.emplace_back(ExecutionResponse{ErrorCode::SUCCEEDED, 233});
        resps.emplace_back(ExecutionResponse{ErrorCode::SUCCEEDED,
                                             233,
                                             std::make_unique<DataSet>(),
                                             std::make_unique<std::string>("test_space")});
        resps.emplace_back(ExecutionResponse{ErrorCode::SUCCEEDED,
                                             233,
                                             nullptr,
                                             std::make_unique<std::string>("test_space"),
                                             nullptr,
                                             std::make_unique<PlanDescription>()});
        resps.emplace_back(ExecutionResponse{ErrorCode::E_SYNTAX_ERROR,
                                             233,
                                             nullptr,
                                             std::make_unique<std::string>("test_space"),
                                             std::make_unique<std::string>("Error Msg.")});
        for (const auto &resp : resps) {
            std::string buf;
            buf.reserve(128);
            serializer::serialize(resp, &buf);
            ExecutionResponse copy;
            std::size_t s = serializer::deserialize(buf, copy);
            ASSERT_EQ(s, buf.size());
            EXPECT_EQ(resp, copy);
        }
    }
    // plan description
    {
        std::vector<PlanDescription> pds;
        pds.emplace_back(PlanDescription{});
        pds.emplace_back(PlanDescription{std::vector<PlanNodeDescription>{},
                                         std::unordered_map<int64_t, int64_t>{{1, 2}, {4, 7}},
                                         "format"});
        for (const auto &pd : pds) {
            std::string buf;
            buf.reserve(128);
            serializer::serialize(pd, &buf);
            PlanDescription copy;
            std::size_t s = serializer::deserialize(buf, copy);
            ASSERT_EQ(s, buf.size());
            EXPECT_EQ(pd, copy);
        }
    }
}

}   // namespace nebula
