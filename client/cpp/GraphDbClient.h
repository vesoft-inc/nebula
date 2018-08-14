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

class RowSetReader;

class GraphDbClient {
public:
    GraphDbClient(const std::string& addr, uint16_t port);
    virtual ~GraphDbClient();

    // When authentication succeeds, the method returns 0, otherwise
    // an negative error code will be returned
    //
    // When the method returns error, getErrorStr() can be called to
    // get the human readable error message
    int32_t connect(const std::string& username,
                    const std::string& password);
    void disconnect();

    // When execution succeeds, the method returns 0, otherwise
    // a negative error code will be returned
    //
    // When the method returns error, getErrorStr() can be called to
    // get the human readable error message
    int32_t execute(folly::StringPiece stmt,
                    std::unique_ptr<RowSetReader>& rowsetReader);

    // Get the server latency in milliseconds
    int32_t getServerLatency() const;

    // Return the last human readable error message
    const char* getErrorStr() const;

private:
    std::unique_ptr<cpp2::GraphDbServiceAsyncClient> client_;
    const std::string addr_;
    const uint16_t port_;
    int64_t sessionId_;

    std::string errorStr_;
    int32_t latencyInMs_;
};

}  // namespace vgraph
}  // namespace vesoft
#endif  // CLIENT_CPP_GRAPHDBCLIENT_H_
