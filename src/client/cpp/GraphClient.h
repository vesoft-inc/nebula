/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef CLIENT_CPP_GRAPHCLIENT_H_
#define CLIENT_CPP_GRAPHCLIENT_H_

#include "base/Base.h"
#include "gen-cpp2/GraphServiceAsyncClient.h"

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
#endif  // CLIENT_CPP_GRAPHCLIENT_H_
