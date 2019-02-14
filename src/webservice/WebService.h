/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef WEBSERVICE_WEBSERVICE_H_
#define WEBSERVICE_WEBSERVICE_H_

#include "base/Base.h"
#include <proxygen/httpserver/HTTPServer.h>
#include "thread/NamedThread.h"

namespace nebula {

using HandlerGen = std::unordered_map<
    std::string,
    std::function<proxygen::RequestHandler*()>>;

class WebService final {
public:
    static void start();
    static void stop();

    // To register a handler generator for a specific path
    // All registrations have to be done before calling start().
    // Anything registered after start() being called will not take
    // effect
    //
    // By default, web service will register the handler for getting/setting
    // the flags
    static void registerHandler(const std::string& path,
                                std::function<proxygen::RequestHandler*()>&& gen);

private:
    WebService() = delete;

    static std::unique_ptr<proxygen::HTTPServer> server_;
    static std::unique_ptr<thread::NamedThread> wsThread_;

    static HandlerGen handlerGenMap_;
};

}  // namespace nebula
#endif  // WEBSERVICE_WEBSERVICE_H_
