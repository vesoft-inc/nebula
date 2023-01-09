/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/plugin/fulltext/elasticsearch/ESAdapter.h"

#include <random>

#include "fmt/printf.h"
#include "openssl/sha.h"
namespace nebula::plugin {

using namespace fmt::literals;  // NOLINT

ESQueryResult::Item::Item(const std::string& v, const std::string& t) : vid(v), text(t) {}
ESQueryResult::Item::Item(const std::string& s,
                          const std::string& d,
                          int64_t r,
                          const std::string& t)
    : src(s), dst(d), rank(r), text(t) {}

void ESBulk::put(const std::string& indexName,
                 const std::string& vid,
                 const std::string& src,
                 const std::string& dst,
                 int64_t rank,
                 const std::string& text) {
  folly::dynamic action = folly::dynamic::object();
  folly::dynamic metadata = folly::dynamic::object();
  folly::dynamic body = folly::dynamic::object();
  auto docId = ESAdapter::genDocID(vid, src, dst, rank);
  metadata["_id"] = docId;
  metadata["_type"] = "_doc";
  metadata["_index"] = indexName;
  action["index"] = std::move(metadata);
  body["vid"] = vid;
  body["src"] = src;
  body["dst"] = dst;
  body["rank"] = rank;
  body["text"] = text;
  documents_[indexName].emplace_back(std::move(action));
  documents_[indexName].emplace_back(std::move(body));
}

void ESBulk::delete_(const std::string& indexName,
                     const std::string& vid,
                     const std::string& src,
                     const std::string& dst,
                     int64_t rank) {
  folly::dynamic action = folly::dynamic::object();
  folly::dynamic metadata = folly::dynamic::object();
  auto docId = ESAdapter::genDocID(vid, src, dst, rank);
  metadata["_id"] = docId;
  metadata["_type"] = "_doc";
  metadata["_index"] = indexName;
  action["delete"] = std::move(metadata);
  documents_[indexName].emplace_back(std::move(action));
}

bool ESBulk::empty() {
  return documents_.empty();
}

ESAdapter::ESAdapter(std::vector<ESClient>&& clients) : clients_(clients) {}

void ESAdapter::setClients(std::vector<ESClient>&& clients) {
  clients_ = std::move(clients);
}

Status ESAdapter::createIndex(const std::string& name) {
  folly::dynamic mappings = folly::parseJson(R"(
    {
      "mappings":{
        "properties":{
          "vid": {
            "type": "keyword"
          },
          "src": {
            "type": "keyword"
          },
          "dst": {
            "type": "keyword"
          },
          "rank": {
            "type": "long"
          },
          "text": {
            "type": "keyword"
          }
        }
      }
    }
  )");
  auto result = randomClient().createIndex(name, mappings);
  if (!result.ok()) {
    return result.status();
  }
  auto resp = std::move(result).value();
  if (resp.count("acknowledged") && resp["acknowledged"].isBool() &&
      resp["acknowledged"].getBool()) {
    return Status::OK();
  }
  if (resp.count("error")) {
    return Status::Error(folly::toJson(resp["error"]));
  }
  return Status::Error(folly::toJson(resp));
}

Status ESAdapter::dropIndex(const std::string& name) {
  auto result = randomClient().dropIndex(name);
  if (!result.ok()) {
    return result.status();
  }
  auto resp = std::move(result).value();
  if (resp["acknowledged"].isBool() && resp["acknowledged"].getBool()) {
    return Status::OK();
  }
  auto error = resp["error"];
  if (error.isObject()) {
    return Status::Error(folly::toJson(error));
  }
  return Status::Error(folly::toJson(resp));
}

Status ESAdapter::clearIndex(const std::string& name, bool refresh) {
  folly::dynamic body = folly::dynamic::object();
  body["query"] = folly::dynamic::object("match_all", folly::dynamic::object());
  auto result = randomClient().deleteByQuery(name, body, refresh);
  if (!result.ok()) {
    return result.status();
  }
  auto resp = std::move(result).value();
  if (resp["failures"].isArray() && resp["failures"].size() == 0) {
    return Status::OK();
  }
  auto error = resp["error"];
  if (error.isObject()) {
    return Status::Error(folly::toJson(error));
  }
  return Status::Error(folly::toJson(resp));
}

StatusOr<bool> ESAdapter::isIndexExist(const std::string& name) {
  auto result = randomClient().getIndex(name);
  if (!result.ok()) {
    return result.status();
  }
  auto resp = std::move(result).value();
  if (resp.count(name)) {
    return true;
  }
  if (resp.count("status") && resp["status"].getInt() == 404) {
    return false;
  }
  if (resp.count("error")) {
    return Status::Error(folly::toJson(resp["error"]));
  }
  return Status::Error(folly::toJson(resp));
}

Status ESAdapter::bulk(const ESBulk& bulk, bool refresh) {
  std::vector<folly::dynamic> jsonArray;
  for (auto& [docId, objs] : bulk.documents_) {
    for (auto& obj : objs) {
      jsonArray.push_back(obj);
    }
  }
  auto result = randomClient().bulk(jsonArray, refresh);
  if (!result.ok()) {
    return result.status();
  }
  auto resp = std::move(result).value();
  if (!resp.count("error")) {
    return Status::OK();
  }
  auto error = resp["error"];
  if (error.isObject()) {
    return Status::Error(folly::toJson(error));
  }
  return Status::Error(folly::toJson(resp));
}

StatusOr<ESQueryResult> ESAdapter::prefix(const std::string& index,
                                          const std::string& pattern,
                                          int64_t size,
                                          int64_t timeout) {
  folly::dynamic body = folly::dynamic::object();
  body["query"] = folly::dynamic::object();
  body["query"]["prefix"] = folly::dynamic::object();
  body["query"]["prefix"]["text"] = pattern;
  if (size > 0) {
    body["size"] = size;
    body["from"] = 0;
  }
  return ESAdapter::query(index, body, timeout);
}

StatusOr<ESQueryResult> ESAdapter::wildcard(const std::string& index,
                                            const std::string& pattern,
                                            int64_t size,
                                            int64_t timeout) {
  folly::dynamic body = folly::dynamic::object();
  body["query"] = folly::dynamic::object();
  body["query"]["wildcard"] = folly::dynamic::object("text", pattern);
  if (size > 0) {
    body["size"] = size;
    body["from"] = 0;
  }
  return ESAdapter::query(index, body, timeout);
}

StatusOr<ESQueryResult> ESAdapter::regexp(const std::string& index,
                                          const std::string& pattern,
                                          int64_t size,
                                          int64_t timeout) {
  folly::dynamic body = folly::dynamic::object();
  body["query"] = folly::dynamic::object();
  body["query"]["regexp"] = folly::dynamic::object("text", pattern);
  if (size > 0) {
    body["size"] = size;
    body["from"] = 0;
  }
  return ESAdapter::query(index, body, timeout);
}

StatusOr<ESQueryResult> ESAdapter::fuzzy(const std::string& index,
                                         const std::string& pattern,
                                         const std::string& fuzziness,
                                         int64_t size,
                                         int64_t timeout) {
  folly::dynamic body = folly::dynamic::object();
  body["query"] = folly::dynamic::object();
  body["query"]["fuzzy"] = folly::dynamic::object();
  body["query"]["fuzzy"]["text"] = folly::dynamic::object("value", pattern)("fuzziness", fuzziness);
  if (size > 0) {
    body["size"] = size;
    body["from"] = 0;
  }
  return ESAdapter::query(index, body, timeout);
}

// StatusOr<ESQueryResult> ESAdapter::term(const std::string& index,const std::vector<std::string>&
// words){
//   folly::dynamic query = folly::dynamic::object("query",folly::dynam)
// }

StatusOr<ESQueryResult> ESAdapter::match_all(const std::string& index) {
  folly::dynamic body = folly::dynamic::object();
  body["query"] = folly::dynamic::object();
  body["query"]["match_all"] = folly::dynamic::object();
  return ESAdapter::query(index, body, -1);
}

StatusOr<ESQueryResult> ESAdapter::query(const std::string& index,
                                         const folly::dynamic& query,
                                         int64_t timeout) {
  auto result = randomClient().search(index, query, timeout);
  if (!result.ok()) {
    return std::move(result).status();
  }
  auto resp = std::move(result).value();
  if (resp.count("hits")) {
    auto hits = resp["hits"]["hits"];
    ESQueryResult res;
    for (auto& hit : hits) {
      auto source = hit["_source"];
      ESQueryResult::Item item;
      item.text = source["text"].getString();
      item.src = source["src"].getString();
      item.dst = source["dst"].getString();
      item.rank = source["rank"].getInt();
      item.vid = source["vid"].getString();
      res.items.emplace_back(std::move(item));
    }
    return res;
  }
  auto error = resp["error"];
  if (error.isObject()) {
    return Status::Error(folly::toJson(error));
  }
  return Status::Error(folly::toJson(resp));
}

std::string ESAdapter::genDocID(const std::string& vid,
                                const std::string& src,
                                const std::string& dst,
                                int64_t rank) {
  std::string str;
  unsigned char mdStr[32] = {0};
  if (!vid.empty()) {
    str = vid;
  } else {
    str = src + dst + std::to_string(rank);
  }
  SHA256(reinterpret_cast<unsigned char*>(str.data()), str.size(), mdStr);
  char hex[64] = {0};
  for (int i = 0; i < 32; i++) {
    hex[i * 2] = 'A' + mdStr[i] / 16;
    hex[i * 2 + 1] = 'A' + mdStr[i] % 16;
  }
  return std::string(hex, 64);
}

ESClient& ESAdapter::randomClient() {
  static thread_local std::default_random_engine engine;
  static thread_local std::uniform_int_distribution<size_t> d(0, clients_.size() - 1);
  return clients_[d(engine)];
}
}  // namespace nebula::plugin
