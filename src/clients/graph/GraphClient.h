/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENTS_GRAPH_GRAPHCLIENT_H_
#define CLIENTS_GRAPH_GRAPHCLIENT_H_

#include "base/Base.h"
#include "interface/gen-cpp2/GraphServiceAsyncClient.h"

namespace nebula {
namespace graph {

class GraphClient {
public:
    GraphClient(const std::string& addr, uint16_t port);
    virtual ~GraphClient();

    // Authenticate the user
    cpp2::ErrorCode connect(const std::string& username,
                            const std::string& password);
    void disconnect();

    cpp2::ErrorCode execute(folly::StringPiece stmt,
                            cpp2::ExecutionResponse& resp);

private:
    std::unique_ptr<cpp2::GraphServiceAsyncClient> client_;
    const std::string addr_;
    const uint16_t port_;
    int64_t sessionId_;
};

}  // namespace graph
}  // namespace nebula
#endif  // CLIENTS_GRAPH_GRAPHCLIENT_H_
