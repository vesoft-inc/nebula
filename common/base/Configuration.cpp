/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "base/Base.h"
#include "base/Configuration.h"

namespace vesoft {

using Status = Configuration::Status;

const Status Status::OK{kOk, "OK"};
const Status Status::NO_FILE{kNoSuchFile, "File not found"};
const Status Status::NO_PERM{kNoPermission, "No permission"};
const Status Status::ILL_FORMAT{kIllFormat, "Illegal format"};
const Status Status::WRONG_TYPE{kWrongType, "Wrong value type"};
const Status Status::EMPTY_FILE{kEmptyFile, "File is empty"};
const Status Status::ITEM_NOT_FOUND{kEmptyFile, "File is empty"};
const Status Status::TYPE_NOT_MATCH{kEmptyFile, "File is empty"};
const Status Status::UNKNOWN{kUnknown, "Unknown error"};

Status::Status() : Status(kOk, "OK") {
}

Status::Status(Code code, const char *msg) {
    code_ = code;
    msg_ = msg;
}

Status& Status::operator=(const Status &rhs) {
    if (this != &rhs) {
        code_ = rhs.code_;
        msg_ = rhs.msg_;
    }
    return *this;
}

bool Status::operator==(const Status &rhs) const {
    return code_ == rhs.code_;
}

bool Status::ok() const {
    return *this == Status::OK;
}

const char* Status::msg() const {
    return msg_;
}

Status Configuration::parseFromFile(const std::string &filename) {
    auto fd = ::open(filename.c_str(), O_RDONLY);
    auto status = Status::OK;
    std::string content;
    do {
        if (fd == -1) {
            if (errno == ENOENT) {
                status = Status::NO_FILE;
                break;
            }
            if (errno == EPERM) {
                status = Status::NO_PERM;
                break;
            }
            status = Status::UNKNOWN;
            break;
        }
        // get the file size
        auto len = ::lseek(fd, 0, SEEK_END);
        if (len == 0) {
            status = Status::EMPTY_FILE;
            break;
        }
        ::lseek(fd, 0, SEEK_SET);
        // read the whole content
        // TODO(dutor) ::read might be interrupted by signals
        auto buffer = std::make_unique<char[]>(len + 1);
        ::read(fd, buffer.get(), len);
        buffer[len] = '\0';
        // strip off all comments
        static const std::regex comment("//.*|#.*");
        content = std::regex_replace(buffer.get(), comment, "");
    } while (false);

    if (fd != -1) {
        ::close(fd);
    }

    if (!status.ok()) {
        return status;
    }

    return parseFromString(content);
}

Status Configuration::parseFromString(const std::string &content) {
    try {
        auto json = folly::parseJson(content);
        content_ = std::make_unique<folly::dynamic>(std::move(json));
    } catch (std::exception &e) {
        LOG(ERROR) << e.what();
        return Status::ILL_FORMAT;
    }
    return Status::OK;
}

Status Configuration::fetchAsInt(const char *key, int64_t &val) const {
    DCHECK(content_ != nullptr);
    auto iter = content_->find(key);
    if (iter == content_->items().end()) {
        return Status::ITEM_NOT_FOUND;
    }
    if (!iter->second.isInt()) {
        return Status::TYPE_NOT_MATCH;
    }
    val = iter->second.getInt();
    return Status::OK;
}

Status Configuration::fetchAsDouble(const char *key, double &val) const {
    DCHECK(content_ != nullptr);
    auto iter = content_->find(key);
    if (iter == content_->items().end()) {
        return Status::ITEM_NOT_FOUND;
    }
    if (!iter->second.isDouble()) {
        return Status::TYPE_NOT_MATCH;
    }
    val = iter->second.getDouble();
    return Status::OK;
}

Status Configuration::fetchAsBool(const char *key, bool &val) const {
    DCHECK(content_ != nullptr);
    auto iter = content_->find(key);
    if (iter == content_->items().end()) {
        return Status::ITEM_NOT_FOUND;
    }
    if (!iter->second.isBool()) {
        return Status::TYPE_NOT_MATCH;
    }
    val = iter->second.getBool();
    return Status::OK;
}

Status Configuration::fetchAsString(const char *key, std::string &val) const {
    DCHECK(content_ != nullptr);
    auto iter = content_->find(key);
    if (iter == content_->items().end()) {
        return Status::ITEM_NOT_FOUND;
    }
    if (!iter->second.isString()) {
        return Status::TYPE_NOT_MATCH;
    }
    val = iter->second.getString();
    return Status::OK;
}

Status Configuration::fetchAsSubConf(const char *key, Configuration &subconf) const {
    DCHECK(content_ != nullptr);
    auto iter = content_->find(key);
    if (iter == content_->items().end()) {
        return Status::ITEM_NOT_FOUND;
    }
    if (!iter->second.isObject()) {
        return Status::TYPE_NOT_MATCH;
    }
    subconf.content_ = std::make_unique<folly::dynamic>(iter->second);
    return Status::OK;
}

}
