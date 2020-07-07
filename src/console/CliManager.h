/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONSOLE_CLIMANAGER_H_
#define CONSOLE_CLIMANAGER_H_

#include "base/Base.h"
#include "console/CmdProcessor.h"

namespace nebula {
namespace graph {

class CliManager final {
public:
    CliManager();
    ~CliManager() = default;

    bool connect();

    void batch(const std::string& filename);

    void loop();

private:
    bool readLine(std::string &line, bool linebreak = false);

    void updateHistory(const char *line);

    void saveHistory();

    void loadHistory();

    void initAutoCompletion();

private:
    std::string addr_;
    uint16_t port_;
    std::string username_;
    bool enableHistroy_{true};
    bool isInteractive_{true};

    std::unique_ptr<CmdProcessor> cmdProcessor_;
};

}  // namespace graph
}  // namespace nebula
#endif  // CONSOLE_CLIMANAGER_H_
