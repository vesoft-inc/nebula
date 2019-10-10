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
        : client_(std::move(client)) {
        localCommandProcessor_ = std::make_unique<LocalCommandProcessor>();
    }
    ~CmdProcessor() = default;

    // Process the given command
    //
    // The method returns false when it's ready to exit, otherwise
    // the method returns true
    bool process(folly::StringPiece cmd);

    const std::string& getSpaceName() const;

private:
    // All user inputs prefixed with ":" are considered as local commands,
    // which won't be sent on wire to the remote server.
    class LocalCommandProcessor final {
    public:
        LocalCommandProcessor();
        ~LocalCommandProcessor() = default;
        bool isLocalCommand(folly::StringPiece command) const;
        bool process(folly::StringPiece command) const;

    private:
        bool doExit(folly::StringPiece args) const;
        bool doShell(folly::StringPiece args) const;
        bool doHistory(folly::StringPiece args) const;

    private:
        std::string localCommandPrefix_ = ":";
        std::vector<std::pair<std::string, std::function<bool(folly::StringPiece)>>> commands_;
    };

private:
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

private:
    std::string curSpaceName_{"(none)"};
    std::unique_ptr<GraphClient> client_;
    std::unique_ptr<LocalCommandProcessor> localCommandProcessor_;
};

}  // namespace graph
}  // namespace nebula
#endif  // CONSOLE_CMDPROCESSOR_H_
