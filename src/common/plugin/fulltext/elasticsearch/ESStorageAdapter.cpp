/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <common/plugin/fulltext/FTUtils.h>
#include "common/process/ProcessUtils.h"
#include "common/plugin/fulltext/elasticsearch/ESStorageAdapter.h"

namespace nebula {
namespace plugin {
std::unique_ptr<FTStorageAdapter> ESStorageAdapter::kAdapter =
    std::unique_ptr<ESStorageAdapter>(new ESStorageAdapter());

bool ESStorageAdapter::checkPut(const std::string& ret, const std::string& cmd) const {
    // For example :
    //     HostAddr localHost_{"127.0.0.1", 9200};
    //     DocItem item("index1", "col1", 1, 2, "aaaa");
    //
    // Command should be :
    //    /usr/bin/curl -H "Content-Type: application/json; charset=utf-8"
    //    -XPUT "http://127.0.0.1:9200/index1/_doc/0000000001_0000000002_8c43de7b01bca674276c43e09b3ec5ba_aaaa"  // NOLINT
    //    -d'{"value":"aaaa","schema_id":2,"column_id":"8c43de7b01bca674276c43e09b3ec5ba"}'
    //
    // If successful, the result is returned:
    //    {
    //        "_primary_term": 1,
    //        "_shards": {
    //                "failed": 0,
    //                "total": 2,
    //                "successful": 1
    //        },
    //        "_id": "0000000001_0000000002_8c43de7b01bca674276c43e09b3ec5ba_aaaa",
    //        "result": "created",
    //        "_seq_no": 0,
    //        "_type": "_doc",
    //        "_index": "index1",
    //        "_version": 1
    //    }
    try {
        auto root = folly::parseJson(ret);
        auto result = root.find("result");
        if (result != root.items().end() &&
            (result->second.getString() == "created" || result->second.getString() == "updated")) {
            return true;
        }
    } catch (std::exception &e) {
        LOG(ERROR) << "result error : " << e.what();
    }
    VLOG(3) << "Command : " <<cmd << "failed : " << ret;
    return false;
}

bool ESStorageAdapter::checkBulk(const std::string& ret) const {
    // For example :
    //     HostAddr localHost_{"127.0.0.1", 9200};
    //     DocItem item("bulk_index", "col1", 1, 2, "row_1")
    //                 ("bulk_index", "col1", 1, 2, "row_2")
    //
    // Command should be :
    //    curl  -H "Content-Type: application/x-ndjson" -XPOST localhost:9200/_bulk -d '
    //    { "index" : { "_index" : "bulk_index", "_id" : "1" } }
    //    { "schema_id" : 1 , "column_id" : "col1", "value" : "row_1"}
    //    { "index" : { "_index" : "bulk_index", "_id" : "2" } }
    //    { "schema_id" : 1 , "column_id" : "col1", "value" : "row_2"}
    //    '
    //
    // If successful, the result is returned:
    //    {
    //        "took": 18,
    //            "errors": false,
    //            "items": [{
    //            "index": {
    //                "_index": "bulk_index",
    //                    "_type": "_doc",
    //                    "_id": "1",
    //                    "_version": 4,
    //                    "result": "updated",
    //                    "_shards": {
    //                    "total": 2,
    //                        "successful": 1,
    //                        "failed": 0
    //                },
    //                "_seq_no": 4,
    //                    "_primary_term": 1,
    //                    "status": 200
    //            }
    //        }, {
    //            "index": {
    //                "_index": "bulk_index",
    //                    "_type": "_doc",
    //                    "_id": "2",
    //                    "_version": 2,
    //                    "result": "updated",
    //                    "_shards": {
    //                    "total": 2,
    //                        "successful": 1,
    //                        "failed": 0
    //                },
    //                "_seq_no": 5,
    //                    "_primary_term": 1,
    //                    "status": 200
    //            }
    //        }]
    //    }
    try {
        auto root = folly::parseJson(ret);
        auto result = root.find("errors");
        if (result != root.items().end() && result->second.isBool() && !result->second.getBool()) {
            return true;
        }
    } catch (std::exception &e) {
        LOG(ERROR) << "result error : " << e.what();
    }
    VLOG(3) << "Bulk insert failed";
    VLOG(3) << ret;
    return false;
}

StatusOr<bool> ESStorageAdapter::put(const HttpClient& client, const DocItem& item) const {
    auto command = putCmd(client, item);
    auto ret = nebula::ProcessUtils::runCommand(command.c_str());
    if (!ret.ok() || ret.value().empty()) {
        LOG(ERROR) << "Http PUT Failed: " << command;
        return Status::Error("command failed : %s", command.c_str());
    }
    return checkPut(ret.value(), command);
}

StatusOr<bool> ESStorageAdapter::bulk(const HttpClient& client,
                                      const std::vector<DocItem>& items) const {
    auto command = bulkCmd(client, items);
    if (command.empty()) {
        return true;
    }
    auto ret = nebula::ProcessUtils::runCommand(command.c_str());
    if (!ret.ok() || ret.value().empty()) {
        VLOG(3) << "Http PUT Failed";
        VLOG(3) << command;
        return Status::Error("bulk command failed");
    }
    return checkBulk(ret.value());
}

std::string ESStorageAdapter::putHeader(const HttpClient& client,
                                        const DocItem& item) const noexcept {
    //    curl -H "Content-Type: application/json; charset=utf-8"
    //    -XPUT "http://127.0.0.1:9200/my_temp_index_3/_doc/part1|tag4|col4|hello"
    std::stringstream os;
    os << CURL << CURL_CONTENT_JSON << XPUT;
    os << client.toString() << item.index << "/_doc/" << DocIDTraits::docId(item) << "\"";
    return os.str();
}

std::string ESStorageAdapter::putBody(const DocItem& item) const noexcept {
    //    -d'{"column_id" : "col4", "value" : "hello"}'
    folly::dynamic d = folly::dynamic::object("column_id", DocIDTraits::column(item.column))
                                             ("value", DocIDTraits::val(item.val));
    std::stringstream os;
    os << " -d'" << DocIDTraits::normalizedJson(folly::toJson(d)) << "'";
    return os.str();
}

std::string ESStorageAdapter::putCmd(const HttpClient& client,
                                     const DocItem& item) const noexcept {
    std::stringstream os;
    os << putHeader(client, item) << putBody(item);
    return os.str();
}

std::string ESStorageAdapter::bulkHeader(const HttpClient& client) const noexcept {
    // curl  -H "Content-Type: application/x-ndjson; charset=utf-8" http://127.0.0.1:9200/_bulk
    std::stringstream os;
    os << CURL << CURL_CONTENT_NDJSON << XPOST;
    os << client.toString() << "_bulk\"";
    return os.str();
}

std::string ESStorageAdapter::bulkBody(const std::vector<DocItem>& items) const noexcept {
    //    -d '
    //    { "index" : { "_index" : "bulk_index", "_id" : "1" } }
    //    { "column_id" : "col1", "value" : "row_1"}
    //    { "index" : { "_index" : "bulk_index", "_id" : "2" } }
    //    { "column_id" : "col1", "value" : "row_2"}
    //    '
    if (items.empty()) {
        return "";
    }
    std::stringstream os;
    os << " -d '" << "\n";
    for (const auto& item : items) {
        folly::dynamic meta = folly::dynamic::object("_id", DocIDTraits::docId(item))
                                                    ("_index", item.index);
        folly::dynamic data = folly::dynamic::object("value", DocIDTraits::val(item.val))
                                                    ("column_id", DocIDTraits::column(item.column));
        os << folly::toJson(folly::dynamic::object("index", meta)) << "\n";
        os << DocIDTraits::normalizedJson(folly::toJson(data)) << "\n";
    }
    os << "'";
    return os.str();
}

std::string ESStorageAdapter::bulkCmd(const HttpClient& client,
                                      const std::vector<DocItem>& items) const noexcept {
    auto body = bulkBody(items);
    if (body.empty()) {
        return "";
    }
    std::stringstream os;
    os << bulkHeader(client) << body;
    return os.str();
}

}  // namespace plugin
}  // namespace nebula
