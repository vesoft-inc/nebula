/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <memory>
#include <regex>
#include <unordered_map>
#include <vector>

#include <proxygen/lib/http/HTTPMethod.h>

#include "common/cpp/helpers.h"

namespace proxygen {
class RequestHandler;
class HTTPMessage;
}   // namespace proxygen

namespace nebula {
namespace web {

using PathParams = std::unordered_map<std::string, std::string>;
using ReqHandlerGenerator = std::function<proxygen::RequestHandler *(PathParams &&)>;

class Route final {
public:
    Route(proxygen::HTTPMethod method, const std::string &path) : method_(method) {
        checkPath(path);
        setPath(path);
    }

    void setNext(Route *next) {
        next_ = next;
    }

    Route *next() const {
        return next_;
    }

    bool matches(proxygen::HTTPMethod method, const std::string &path) const;

    // Register handlers for the route
    // TODO(yee): manage handlers chain later and now only allowed to register once
    Route &handler(ReqHandlerGenerator generator);

    proxygen::RequestHandler *generateHandler(const std::string &path) const;

private:
    static void checkPath(const std::string &path);
    void setPath(const std::string &path);
    void createPattenRegex(const std::string &path);

    static std::unique_ptr<std::regex> reToken_;

    Route *next_{nullptr};
    proxygen::HTTPMethod method_;
    std::string path_;
    std::unique_ptr<std::regex> pattern_;
    std::vector<ReqHandlerGenerator> generators_;
    std::vector<std::string> groups_;
};

class Router final : public cpp::NonCopyable, public cpp::NonMovable {
public:
    explicit Router(const std::string &prefix) : prefix_(prefix) {}
    ~Router();

    proxygen::RequestHandler *dispatch(const proxygen::HTTPMessage *msg) const;

    Route &get(const std::string &path) {
        return route(proxygen::HTTPMethod::GET, path);
    }

    Route &post(const std::string &path) {
        return route(proxygen::HTTPMethod::POST, path);
    }

    Route &put(const std::string &path) {
        return route(proxygen::HTTPMethod::PUT, path);
    }

    Route &del(const std::string &path) {
        return route(proxygen::HTTPMethod::DELETE, path);
    }

    Route &route(proxygen::HTTPMethod method, const std::string &path) {
        Route *next = nullptr;
        if (!prefix_.empty()) {
            next = new Route(method, "/" + prefix_ + (path.empty() ? "/" : path));
        } else {
            next = new Route(method, path.empty() ? "/" : path);
        }
        append(next);
        return *next;
    }

private:
    void append(Route *route);

    std::string prefix_;
    Route *head_{nullptr};
    Route *tail_{nullptr};
};

}   // namespace web
}   // namespace nebula
