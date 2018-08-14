/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef CONSOLE_CMDPROCESSOR_H_
#define CONSOLE_CMDPROCESSOR_H_

#include "base/Base.h"
#include "client/cpp/GraphDbClient.h"

namespace vesoft {
namespace vgraph {

class CmdProcessor final {
public:
    explicit CmdProcessor(std::unique_ptr<GraphDbClient> client)
        : client_(std::move(client)) {}
    ~CmdProcessor() = default;

    // Process the given command
    //
    // The method returns false when it's ready to exit, otherwise
    // the method returns true
    bool process(folly::StringPiece cmd);

private:
    std::unique_ptr<GraphDbClient> client_;

    // The method returns true if the given command is a client command
    // and has been processed. Otherwise, the method returns false
    bool processClientCmd(folly::StringPiece cmd, bool& readyToExit);

    void processServerCmd(folly::StringPiece cmd);

    std::vector<int16_t> calColumnWidths(const RowSetReader* dataReader) const;

    int32_t printResult(const RowSetReader* dataReader) const;
    std::vector<std::string> printHeader(const RowSetReader* dataReader,
                                         const std::vector<int16_t>& widths) const;
    // The method returns the total number of rows
    int32_t printData(const RowSetReader* dataReader,
                      const std::string& rowLine,
                      const std::vector<std::string>& formats) const;
};

}  // namespace vgraph
}  // namespace vesoft
#endif  // CONSOLE_CMDPROCESSOR_H_
