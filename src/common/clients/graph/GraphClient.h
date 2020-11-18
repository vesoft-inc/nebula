/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_CLIENTS_GRAPH_GRAPHCLIENT_H_
#define COMMON_CLIENTS_GRAPH_GRAPHCLIENT_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/GraphServiceAsyncClient.h"

namespace nebula {
namespace graph {

class GraphClient {
public:
    GraphClient(const std::string& addr, uint16_t port);
    virtual ~GraphClient();

    // Authenticate the user
    nebula::ErrorCode connect(const std::string& username,
                              const std::string& password);
    void disconnect();

    nebula::ErrorCode execute(folly::StringPiece stmt,
                              nebula::ExecutionResponse& resp);

private:
    std::unique_ptr<cpp2::GraphServiceAsyncClient> client_;
    const std::string addr_;
    const uint16_t port_;
    int64_t sessionId_;
};

}  // namespace graph
}  // namespace nebula
#endif  // COMMON_CLIENTS_GRAPH_GRAPHCLIENT_H_
