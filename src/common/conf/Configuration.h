/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_CONF_CONFIGURATION_H_
#define COMMON_CONF_CONFIGURATION_H_

#include "common/base/Base.h"
#include "common/base/Status.h"

/**
 * Configuration loads config infos from a json file or string buffer.
 * For a json file, both the C++ and shell style comments are supported.
 */


namespace nebula {
namespace conf {

class Configuration final {
public:
    Configuration();
    explicit Configuration(folly::dynamic content);

    ~Configuration() = default;

    /**
     * Parse from a file
     */
    Status MUST_USE_RESULT parseFromFile(const std::string &filename);
    /**
     * Parse from a string buffer
     */
    Status MUST_USE_RESULT parseFromString(const std::string &content);

    std::string dumpToString() const;

    std::string dumpToPrettyString() const;

    /**
     * Fetch the config item.
     * @key     item key
     * @val     to hold the item value.
     */
    Status MUST_USE_RESULT fetchAsInt(const char *key, int64_t &val) const;
    Status MUST_USE_RESULT fetchAsDouble(const char *key, double &val) const;
    Status MUST_USE_RESULT fetchAsBool(const char *key, bool &val) const;
    Status MUST_USE_RESULT fetchAsString(const char *key, std::string &val) const;

    Status MUST_USE_RESULT fetchAsIntArray(const char *key,
                                           std::vector<int64_t> &val) const;
    Status MUST_USE_RESULT fetchAsDoubleArray(const char *key,
                                              std::vector<double> &val) const;
    Status MUST_USE_RESULT fetchAsBoolArray(const char *key,
                                            std::vector<bool> &val) const;
    Status MUST_USE_RESULT fetchAsStringArray(const char *key,
                                              std::vector<std::string> &val) const;

    Status MUST_USE_RESULT fetchAsSubConf(const char *key, Configuration &val) const;

    Status MUST_USE_RESULT upsertStringField(const char* key, const std::string& val);

    // Iterate through every key in the configuration
    Status forEachKey(std::function<void(const std::string&)> processor) const;
    // Iterate through every key/value pair in the configuration
    Status forEachItem(
        std::function<void(const std::string&, const folly::dynamic&)> processor) const;

private:
    std::unique_ptr<folly::dynamic>             content_;
};

}   // namespace conf
}   // namespace nebula
#endif  // COMMON_CONF_CONFIGURATION_H_
