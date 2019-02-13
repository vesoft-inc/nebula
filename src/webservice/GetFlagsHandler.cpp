/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "webservice/GetFlagsHandler.h"
#include <folly/String.h>
#include <folly/json.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

namespace nebula {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;

void GetFlagsHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = ErrorCode::E_UNSUPPORTED_METHOD;
        return;
    }

    if (headers->getQueryParamPtr("verbose") != nullptr) {
        verbose_ = true;
    }

    if (headers->getQueryParamPtr("returnjson") != nullptr) {
        returnJson_ = true;
    }

    auto* flagsStr = headers->getQueryParamPtr("flags");
    if (flagsStr != nullptr) {
        folly::split(",", *flagsStr, flagnames_, true);
    }
}


void GetFlagsHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void GetFlagsHandler::onEOM() noexcept {
    switch (err_) {
        case ErrorCode::E_UNSUPPORTED_METHOD:
            ResponseBuilder(downstream_)
                .status(405, "Method Not Allowed")
                .sendWithEOM();
            return;
        default:
            break;
    }

    folly::dynamic vals = getFlags();
    if (returnJson_) {
        ResponseBuilder(downstream_)
            .status(200, "OK")
            .body(folly::toJson(vals))
            .sendWithEOM();
    } else {
        ResponseBuilder(downstream_)
            .status(200, "OK")
            .body(toStr(vals))
            .sendWithEOM();
    }
}


void GetFlagsHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void GetFlagsHandler::requestComplete() noexcept {
    delete this;
}


void GetFlagsHandler::onError(ProxygenError err) noexcept {
    LOG(ERROR) << "Web service GetFlagsHandler got error: "
               << proxygen::getErrorString(err);
    delete this;
}


void GetFlagsHandler::addOneFlag(folly::dynamic& vals,
                                 const std::string& flagname,
                                 const gflags::CommandLineFlagInfo* info) {
    folly::dynamic flag = folly::dynamic::object();
    flag["name"] = flagname;
    if (info != nullptr) {
        auto& type = info->type;
        if (type == "int32") {
            flag["value"] = folly::to<int32_t>(info->current_value);
            if (verbose_) {
                flag["default"] = folly::to<int32_t>(info->default_value);
            }
        } else if (type == "int64") {
            flag["value"] = folly::to<int64_t>(info->current_value);
            if (verbose_) {
                flag["default"] = folly::to<int64_t>(info->default_value);
            }
        } else if (type == "bool") {
            flag["value"] = folly::to<bool>(info->current_value);
            if (verbose_) {
                flag["default"] = folly::to<bool>(info->default_value);
            }
        } else if (type == "double") {
            flag["value"] = folly::to<double>(info->current_value);
            if (verbose_) {
                flag["default"] = folly::to<double>(info->default_value);
            }
        } else if (type == "string") {
            flag["value"] = info->current_value;
            if (verbose_) {
                flag["default"] = info->default_value;
            }
        }
        if (verbose_) {
            flag["type"] = info->type;
            flag["description"] = info->description;
            flag["file"] = info->filename;
            flag["is_default"] = info->is_default;
        }
    } else {
        flag["value"] = nullptr;
    }

    vals.push_back(std::move(flag));
}


folly::dynamic GetFlagsHandler::getFlags() {
    auto flags = folly::dynamic::array();
    if (flagnames_.empty()) {
        // Read all flags
        std::vector<gflags::CommandLineFlagInfo> allFlags;
        gflags::GetAllFlags(&allFlags);
        for (auto& f : allFlags) {
            addOneFlag(flags, f.name, &f);
        }
    } else {
        for (auto& fn : flagnames_) {
            gflags::CommandLineFlagInfo info;
            if (gflags::GetCommandLineFlagInfo(fn.c_str(), &info)) {
                // Found the flag
                addOneFlag(flags, fn, &info);
            } else {
                // Not found
                addOneFlag(flags, fn, nullptr);
            }
        }
    }

    return flags;
}


std::string GetFlagsHandler::toStr(folly::dynamic& vals) {
    std::stringstream ss;
    for (auto& fi : vals) {
        if (verbose_) {
            bool isString = fi["type"].asString() == "string";
            ss << "--" << fi["name"].asString() << ": "
               << fi["description"].asString() << "\n";
            ss << "  file: " << fi["file"].asString()
               << ", type: " << fi["type"].asString()
               << ", default: "
               << (isString ? "\"" : "")
               << fi["default"].asString()
               << (isString ? "\"" : "")
               << ", current: "
               << (isString ? "\"" : "")
               << fi["value"].asString()
               << (isString ? "\"" : "")
               << (fi["is_default"].asBool() ? "(default)" : "")
               << "\n";
        } else {
            auto& val = fi["value"];
            ss << fi["name"].asString() << "="
               << (val.isString() ? "\"" : "")
               << val.asString()
               << (val.isString() ? "\"\n" : "\n");
        }
    }
    return ss.str();
}

}  // namespace nebula
