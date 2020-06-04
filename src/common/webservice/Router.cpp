/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/webservice/Router.h"

#include <sstream>

#include <proxygen/httpserver/RequestHandler.h>

#include "common/webservice/NotFoundHandler.h"
#include "common/webservice/WebService.h"

namespace nebula {
namespace web {

void Route::checkPath(const std::string &path) {
    CHECK(!path.empty() && path[0] == '/') << "Path must start with '/'";
}

void Route::setPath(const std::string &path) {
    auto loc = path.find(':');
    if (loc != std::string::npos) {
        createPattenRegex(path);
        this->path_ = path;
    } else {
        // exact path, remove the last / of path
        if (path.size() > 1 && path.back() == '/') {
            this->path_ = path.substr(0, path.length() - 1);
        } else {
            this->path_ = path.empty() ? "/" : path;
        }
    }
}

// static
std::unique_ptr<std::regex> Route::reToken_ =
    std::make_unique<std::regex>(":([A-Za-z][A-Za-z_0-9]*)");

void Route::createPattenRegex(const std::string &path) {
    int pos = 0;
    std::stringstream ss;
    for (std::sregex_iterator next(path.begin(), path.end(), *reToken_), end; next != end; next++) {
        std::string str = next->str();
        if (!str.empty() && str.front() == ':') {
            str = str.substr(1);
        }
        for (auto &group : groups_) {
            CHECK_NE(str, group) << "Cannot use identifier " << group
                                 << " more than once in pattern string";
        }
        groups_.emplace_back(std::move(str));
        ss << path.substr(pos, next->position() - pos) << "([^/]+)";
        pos = next->position() + next->str().length();
    }
    ss << path.substr(pos);
    this->pattern_ = std::make_unique<std::regex>(ss.str());
}

bool Route::matches(proxygen::HTTPMethod method, const std::string &path) const {
    if (method_ != method) {
        return false;
    }
    std::string p = path;
    if (p.empty()) {
        p = "/";
    } else if (p.size() > 1 && p.back() == '/') {
        p.pop_back();
    }
    if (pattern_) {
        return std::regex_search(p, *pattern_);
    } else {
        return path_ == p;
    }
}

Router::~Router() {
    while (head_ != nullptr) {
        auto next = head_->next();
        delete head_;
        head_ = next;
    }
}

void Route::handler(ReqHandlerGenerator generator) {
    CHECK(!generator_) << "Only allowed to register handler generator once for a route";
    generator_ = generator;
}

proxygen::RequestHandler *Route::generateHandler(const std::string &path) const {
    if (!pattern_) {
        return generator_({});
    }
    std::smatch m;
    std::regex_search(path, m, *pattern_);
    PathParams params;
    CHECK_EQ(groups_.size(), m.size() - 1UL) << "groups is not equal to matches";
    for (std::size_t i = 0; i < groups_.size(); i++) {
        params.emplace(groups_[i], m[i + 1]);
    }
    return generator_(std::move(params));
}

proxygen::RequestHandler *Router::dispatch(const proxygen::HTTPMessage *msg) const {
    for (Route *r = head_; r != nullptr; r = r->next()) {
        if (r->matches(msg->getMethod().value(), msg->getPath())) {
            return r->generateHandler(msg->getPath());
        }
    }
    return new NotFoundHandler();
}

Route &Router::route(proxygen::HTTPMethod method, const std::string &path) {
    if (webSvc_) {
        CHECK(!webSvc_->started()) << "Don't add routes after starting web server!";
    }
    Route *next = nullptr;
    if (!prefix_.empty()) {
        next = new Route(method, "/" + prefix_ + (path.empty() ? "/" : path));
    } else {
        next = new Route(method, path.empty() ? "/" : path);
    }
    append(next);
    return *next;
}

void Router::append(Route *route) {
    if (tail_ != nullptr) {
        tail_->setNext(route);
        tail_ = route;
    } else {
        head_ = tail_ = route;
    }
}

}   // namespace web
}   // namespace nebula
