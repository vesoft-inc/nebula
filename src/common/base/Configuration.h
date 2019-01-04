/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef COMMON_BASE_CONFIGURATION_H_
#define COMMON_BASE_CONFIGURATION_H_

#include "base/Base.h"

/**
 * Configuration loads config infos from a json file or string buffer.
 * For a json file, both the C++ and shell style comments are supported.
 */


namespace nebula {

class Configuration final {
public:
    Configuration() = default;
    ~Configuration() = default;

    class Status;
    /**
     * Parse from a file
     */
    Status MUST_USE_RESULT parseFromFile(const std::string &filename);
    /**
     * Parse from a string buffer
     */
    Status MUST_USE_RESULT parseFromString(const std::string &content);
    /**
     * Fetch the config item.
     * @key     item key
     * @val     to hold the item value.
     */
    Status MUST_USE_RESULT fetchAsInt(const char *key, int64_t &val) const;
    Status MUST_USE_RESULT fetchAsDouble(const char *key, double &val) const;
    Status MUST_USE_RESULT fetchAsBool(const char *key, bool &val) const;
    Status MUST_USE_RESULT fetchAsString(const char *key, std::string &val) const;
    Status MUST_USE_RESULT fetchAsSubConf(const char *key, Configuration &val) const;
    // TODO(dutor) add support for array
    /**
     * Status to indicate whether a operation was successful.
     */
    class Status final {
    public:
        Status();
        Status& operator=(const Status &rhs);
        bool operator==(const Status &rhs) const;
        /**
         * Whether this status indicates OK.
         */
        bool ok() const;
        /**
         * A short message to describe this status.
         */
        const char* msg() const;

    public:
        static const Status                     OK;
        static const Status                     NO_FILE;
        static const Status                     NO_PERM;
        static const Status                     ILL_FORMAT;
        static const Status                     WRONG_TYPE;
        static const Status                     EMPTY_FILE;
        static const Status                     ITEM_NOT_FOUND;
        static const Status                     TYPE_NOT_MATCH;
        static const Status                     UNKNOWN;

    private:
        enum Code {
            kOk, kNoSuchFile, kNoPermission, kIllFormat, kWrongType,
            kEmptyFile, kItemNotFound, kTypeNotMatch, kUnknown,
        };
        Status(Code code, const char *msg);

    private:
        Code                                    code_;
        const char                             *msg_;   // only for storing literal strings
    };

private:
    std::unique_ptr<folly::dynamic>             content_;
};

}   // namespace nebula


#endif  // COMMON_BASE_CONFIGURATION_H_
