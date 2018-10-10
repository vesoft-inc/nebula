/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef CLIENT_CPP_GRAPHDBCLIENT_H_
#define CLIENT_CPP_GRAPHDBCLIENT_H_

#include "base/Base.h"
#include "gen-cpp2/GraphDbServiceAsyncClient.h"

namespace vesoft {
namespace vgraph {

class GraphDbClient {
public:
    GraphDbClient(const std::string& addr, uint16_t port);
    virtual ~GraphDbClient();

    // Authenticate the user
    cpp2::ErrorCode connect(const std::string& username,
                            const std::string& password);
    void disconnect();

    cpp2::ErrorCode execute(folly::StringPiece stmt,
                            cpp2::ExecutionResponse& resp);

private:
    std::unique_ptr<cpp2::GraphDbServiceAsyncClient> client_;
    const std::string addr_;
    const uint16_t port_;
    int64_t sessionId_;
};

}  // namespace vgraph
}  // namespace vesoft
#endif  // CLIENT_CPP_GRAPHDBCLIENT_H_
