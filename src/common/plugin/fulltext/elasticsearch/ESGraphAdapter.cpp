/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/process/ProcessUtils.h"
#include "common/plugin/fulltext/elasticsearch/ESGraphAdapter.h"

namespace nebula {
namespace plugin {

std::unique_ptr<FTGraphAdapter> ESGraphAdapter::kAdapter =
    std::unique_ptr<ESGraphAdapter>(new ESGraphAdapter());

StatusOr<bool> ESGraphAdapter::prefix(const HttpClient& client,
                                      const DocItem& item,
                                      const LimitItem& limit,
                                      std::vector<std::string>& rows) const {
    std::string cmd = header(client, item, limit) +
                      body(item, limit.maxRows_, FT_SEARCH_OP::kPrefix);
    auto ret = nebula::ProcessUtils::runCommand(cmd.c_str());
    if (!ret.ok() || ret.value().empty()) {
        LOG(ERROR) << "Http GET Failed: " << cmd;
        return Status::Error("command failed : %s", cmd.c_str());
    }
    return result(ret.value(), rows);
}

StatusOr<bool> ESGraphAdapter::wildcard(const HttpClient& client,
                              const DocItem& item,
                              const LimitItem& limit,
                              std::vector<std::string>& rows) const {
    std::string cmd = header(client, item, limit) +
                      body(item, limit.maxRows_, FT_SEARCH_OP::kWildcard);
    auto ret = nebula::ProcessUtils::runCommand(cmd.c_str());
    if (!ret.ok() || ret.value().empty()) {
        LOG(ERROR) << "Http GET Failed: " << cmd;
        return Status::Error("command failed : %s", cmd.c_str());
    }
    return result(ret.value(), rows);
}

StatusOr<bool> ESGraphAdapter::regexp(const HttpClient& client,
                                      const DocItem& item,
                                      const LimitItem& limit,
                                      std::vector<std::string>& rows) const {
    std::string cmd = header(client, item, limit) +
                      body(item, limit.maxRows_, FT_SEARCH_OP::kRegexp);
    auto ret = nebula::ProcessUtils::runCommand(cmd.c_str());
    if (!ret.ok() || ret.value().empty()) {
        LOG(ERROR) << "Http GET Failed: " << cmd;
        return Status::Error("command failed : %s", cmd.c_str());
    }
    return result(ret.value(), rows);
}

StatusOr<bool> ESGraphAdapter::fuzzy(const HttpClient& client,
                                     const DocItem& item,
                                     const LimitItem& limit,
                                     const folly::dynamic& fuzziness,
                                     const std::string& op,
                                     std::vector<std::string>& rows) const {
    std::string cmd = header(client, item, limit) +
                      body(item, limit.maxRows_, FT_SEARCH_OP::kFuzzy, fuzziness, op);
    auto ret = nebula::ProcessUtils::runCommand(cmd.c_str());
    if (!ret.ok() || ret.value().empty()) {
        LOG(ERROR) << "Http GET Failed: " << cmd;
        return Status::Error("command failed : %s", cmd.c_str());
    }
    return result(ret.value(), rows);
}

std::string ESGraphAdapter::header() const noexcept {
    std::stringstream os;
    os << CURL << CURL_CONTENT_JSON;
    return os.str();
}

std::string ESGraphAdapter::header(const HttpClient& client,
                                   const DocItem& item,
                                   const LimitItem& limit) const noexcept {
    //    curl -H "Content-Type: application/json; charset=utf-8"
    //    -XGET http://127.0.0.1:9200/my_temp_index_3/_search?timeout=10ms
    std::stringstream os;
    os << CURL << CURL_CONTENT_JSON << XGET;
    os << client.toString() << item.index << "/_search?timeout=" << limit.timeout_ << "ms" << "\"";
    return os.str();
}

folly::dynamic ESGraphAdapter::columnBody(const std::string& col) const noexcept {
    // "term": {"column_id": "col1"}
    folly::dynamic itemColumn = folly::dynamic::object("column_id", DocIDTraits::column(col));
    return folly::dynamic::object("term", itemColumn);
}

std::string ESGraphAdapter::body(const DocItem& item,
                                 int32_t maxRows,
                                 FT_SEARCH_OP type,
                                 const folly::dynamic& fuzziness,
                                 const std::string& op) const noexcept {
    folly::dynamic obj;
    switch (type) {
        case FT_SEARCH_OP::kPrefix : {
            obj = prefixBody(item.val);
            break;
        }
        case FT_SEARCH_OP::kWildcard : {
            obj = wildcardBody(item.val);
            break;
        }
        case FT_SEARCH_OP::kRegexp : {
            obj = regexpBody(item.val);
            break;
        }
        case FT_SEARCH_OP::kFuzzy : {
            obj = fuzzyBody(item.val, fuzziness, op);
        }
    }
    auto itemArray = folly::dynamic::array(columnBody(item.column), obj);
    folly::dynamic itemMust = folly::dynamic::object("must", itemArray);
    folly::dynamic itemBool = folly::dynamic::object("bool", itemMust);
    folly::dynamic itemQuery = folly::dynamic::object("query", itemBool)
                                                     ("_source", "value")
                                                     ("size", maxRows)
                                                     ("from", 0);
    std::stringstream os;
    os << " -d'" << DocIDTraits::normalizedJson(folly::toJson(itemQuery)) << "'";
    return os.str();
}

folly::dynamic ESGraphAdapter::prefixBody(const std::string& prefix) const noexcept {
    // {"prefix": {"value": "a"}}
    folly::dynamic itemValue = folly::dynamic::object("value", prefix);
    return folly::dynamic::object("prefix", itemValue);
}

folly::dynamic ESGraphAdapter::wildcardBody(const std::string& wildcard) const noexcept {
    // {"wildcard": {"value": "*a"}}
    folly::dynamic itemValue = folly::dynamic::object("value", wildcard);
    return folly::dynamic::object("wildcard", itemValue);
}

folly::dynamic ESGraphAdapter::regexpBody(const std::string& regexp) const noexcept {
    // {"regexp": {"value": "c+"}}
    folly::dynamic itemValue = folly::dynamic::object("value", regexp);
    return folly::dynamic::object("regexp", itemValue);
}

folly::dynamic ESGraphAdapter::fuzzyBody(const std::string& regexp,
                                         const folly::dynamic& fuzziness,
                                         const std::string& op) const noexcept {
    // {"match": {"value": {"query":"ccc aaa","fuzziness": "AUTO","operator":  "and"}}}
    folly::dynamic items = folly::dynamic::object("query", regexp)
                                                 ("fuzziness", fuzziness)
                                                 ("operator", op);
    folly::dynamic value = folly::dynamic::object("value", items);
    return folly::dynamic::object("match", value);
}


StatusOr<bool> ESGraphAdapter::createIndex(const HttpClient& client,
                                           const std::string& index,
                                           const std::string&) const {
    // curl -H "Content-Type: application/json; charset=utf-8"
    //      -XPUT "http://127.0.0.1:9200/index_exist"
    std::string cmd = createIndexCmd(client, index);
    auto ret = nebula::ProcessUtils::runCommand(cmd.c_str());
    if (!ret.ok() || ret.value().empty()) {
        LOG(ERROR) << "Http PUT Failed: " << cmd;
        return Status::Error("command failed : %s", cmd.c_str());
    }
    return statusCheck(ret.value());
}

std::string ESGraphAdapter::createIndexCmd(const HttpClient& client,
    const std::string& index, const std::string&) const noexcept {
    std::stringstream os;
    os << header() << XPUT << client.toString() << index << "\"";
    return os.str();
}

StatusOr<bool> ESGraphAdapter::dropIndex(const HttpClient& client,
                                         const std::string& index) const {
    // curl -H "Content-Type: application/json; charset=utf-8"
    //      -XDELETE "http://127.0.0.1:9200/index_exist"
    std::string cmd = dropIndexCmd(client, index);
    auto ret = nebula::ProcessUtils::runCommand(cmd.c_str());
    if (!ret.ok() || ret.value().empty()) {
        LOG(ERROR) << "Http DELETE Failed: " << cmd;
        return Status::Error("command failed : %s", cmd.c_str());
    }
    return statusCheck(ret.value());
}

std::string ESGraphAdapter::dropIndexCmd(const HttpClient& client,
    const std::string& index) const noexcept {
    std::stringstream os;
    os << header() << XDELETE << client.toString() << index << "\"";
    return os.str();
}

StatusOr<bool> ESGraphAdapter::indexExists(const HttpClient& client,
                                           const std::string& index) const {
    // curl -H "Content-Type: application/json; charset=utf-8"
    // -XGET "http://127.0.0.1:9200/_cat/indices/index_exist?format=json"
    std::string cmd = indexExistsCmd(client, index);
    auto ret = nebula::ProcessUtils::runCommand(cmd.c_str());
    if (!ret.ok() || ret.value().empty()) {
        LOG(ERROR) << "Http GET Failed: " << cmd;
        return Status::Error("Http GET Failed: : %s", cmd.c_str());
    }
    return indexCheck(ret.value());
}

std::string ESGraphAdapter::indexExistsCmd(const HttpClient& client,
                                           const std::string& index) const noexcept {
    std::stringstream os;
    os << header() << XGET << client.toString()
       << "_cat/indices/" << index << "?format=json" << "\"";
    return os.str();
}

bool ESGraphAdapter::result(const std::string& ret, std::vector<std::string>& rows) const {
    try {
        auto root = folly::parseJson(ret);
        auto rootHits = root.find("hits");
        if (rootHits != root.items().end()) {
            auto subHits = rootHits->second.find("hits");
            if (subHits != rootHits->second.items().end()) {
                for (auto& item : subHits->second) {
                    auto s = item.find("_source");
                    if (s != item.items().end()) {
                        auto v = s->second.find("value");
                        if (v != s->second.items().end()) {
                            rows.emplace_back(v->second.getString());
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
            }
            return true;
        }
    } catch (std::exception &e) {
        LOG(ERROR) << "result error : " << e.what();
    }
    LOG(ERROR) << "error reason : " << ret;
    return false;
}

bool ESGraphAdapter::statusCheck(const std::string& ret) const {
    try {
        auto root = folly::parseJson(ret);
        if (root.isArray()) {
            return false;
        }
        auto result = root.find("acknowledged");
        if (result != root.items().end() && result->second.isBool() && result->second.getBool()) {
            return true;
        }
    } catch (const std::exception& ex) {
        LOG(ERROR) << "result error : " << ex.what();
    }

    LOG(ERROR) << "error reason : " << ret;
    return false;
}

bool ESGraphAdapter::indexCheck(const std::string& ret) const {
    try {
        auto root = folly::parseJson(ret);
        if (!root.isArray()) {
            return false;
        }
        for (auto& entry : root) {
            auto exists = entry.find("index");
            if (exists != entry.items().end()) {
                return true;
            }
        }
    } catch (std::exception &e) {
        LOG(ERROR) << "result error : " << e.what();
    }
    LOG(ERROR) << "error reason : " << ret;
    return false;
}
}  // namespace plugin
}  // namespace nebula
