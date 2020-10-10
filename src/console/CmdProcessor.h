/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONSOLE_CMDPROCESSOR_H_
#define CONSOLE_CMDPROCESSOR_H_

#include "base/Base.h"
#include "client/cpp/GraphClient.h"

namespace nebula {
namespace graph {

class CmdProcessor final {
public:
    explicit CmdProcessor(std::unique_ptr<GraphClient> client)
        : client_(std::move(client)) {}
    ~CmdProcessor() = default;

    // Process the given command
    //
    // The method returns false when it's ready to exit, otherwise
    // the method returns true
    bool process(folly::StringPiece cmd);

    const std::string& getSpaceName() const;

private:
    std::unique_ptr<GraphClient> client_;

    std::string curSpaceName_{"(none)"};

    // The method returns true if the given command is a client command
    // and has been processed. Otherwise, the method returns false
    bool processClientCmd(folly::StringPiece cmd, bool& readyToExit);

    void processServerCmd(folly::StringPiece cmd);

    void calColumnWidths(const cpp2::ExecutionResponse& resp,
                         std::vector<size_t>& widths,
                         std::vector<std::string>& formats) const;

    void printResult(const cpp2::ExecutionResponse& resp) const;
    void printHeader(const cpp2::ExecutionResponse& resp,
                     const std::vector<size_t>& widths) const;
    void printData(const cpp2::ExecutionResponse& resp,
                   const std::string& rowLine,
                   const std::vector<size_t>& widths,
                   const std::vector<std::string>& formats) const;
    // Print the time of machine running console
    void printTime() const;

    void normalize(folly::StringPiece &command);
};

}  // namespace graph
}  // namespace nebula
#endif  // CONSOLE_CMDPROCESSOR_H_
