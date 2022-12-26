/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_PLUGIN_FULLTEXT_ELASTICSEARCH_ESADAPTER_H_
#define COMMON_PLUGIN_FULLTEXT_ELASTICSEARCH_ESADAPTER_H_

#include <ostream>

#include "common/base/StatusOr.h"
#include "common/plugin/fulltext/elasticsearch/ESClient.h"
#include "folly/container/F14Map.h"
#include "folly/dynamic.h"
namespace nebula::plugin {

struct ESQueryResult {
  struct Item {
    Item() = default;
    Item(const std::string& vid, const std::string& text);
    Item(const std::string& src, const std::string& dst, int64_t rank, const std::string& text);
    std::string vid;       // for vertex
    std::string src, dst;  // for edge
    int64_t rank = 0;      // for edge
    std::string text;
    bool operator==(const Item& item) const {
      return vid == item.vid && src == item.src && dst == item.dst && rank == item.rank &&
             text == item.text;
    }
    std::ostream& operator<<(std::ostream& out) {
      out << fmt::format("vid={}, src={}, dst={}, rank={}, text={}", vid, src, dst, rank, text);
      return out;
    }
  };
  std::vector<Item> items;
};

class ESBulk {
 public:
  void put(const std::string& indexName,
           const std::string& vid,
           const std::string& src,
           const std::string& dst,
           int64_t rank,
           const std::string& text);
  void delete_(const std::string& indexName,
               const std::string& vid,
               const std::string& src,
               const std::string& dst,
               int64_t rank);

  bool empty();

 private:
  folly::F14FastMap<std::string, std::vector<folly::dynamic>> documents_;
  friend class ESAdapter;
};

class ESAdapter {
 public:
  explicit ESAdapter(std::vector<ESClient>&& clients);
  ESAdapter() = default;
  ESAdapter(const ESAdapter& adapter) : clients_(adapter.clients_) {}
  ESAdapter& operator=(const ESAdapter& adapter) {
    clients_ = adapter.clients_;
    return *this;
  }
  virtual ~ESAdapter() = default;
  virtual void setClients(std::vector<ESClient>&& clients);
  virtual Status createIndex(const std::string& name);
  virtual Status dropIndex(const std::string& name);
  virtual Status clearIndex(const std::string& name, bool refresh = false);
  virtual StatusOr<bool> isIndexExist(const std::string& name);

  virtual Status bulk(const ESBulk& bulk, bool refresh = false);

  /**
   * @brief
   *
   * Query:
   *
   * GET /_search
   * {
   *   "query": {
   *     "prefix": {
   *       "text": {
   *         "value": $pattern
   *       }
   *     }
   *   }
   * }
   *
   * @param index
   * @param pattern
   * @return StatusOr<ESQueryResult>
   */
  virtual StatusOr<ESQueryResult> prefix(const std::string& index,
                                         const std::string& pattern,
                                         int64_t size = -1,
                                         int64_t timeout = -1);

  /**
   * @brief
   *
   *
   * Query:
   *
   * GET /_search
   * {
   *   "query": {
   *     "fuzzy": {
   *       "text": {
   *         "value": $pattern
   *       }
   *     }
   *   }
   * }
   *
   * @param index
   * @param pattern
   * @return StatusOr<ESQueryResult>
   */
  virtual StatusOr<ESQueryResult> fuzzy(const std::string& index,
                                        const std::string& pattern,
                                        const std::string& fuzziness,
                                        int64_t size = -1,
                                        int64_t timeout = -1);

  /**
   * @brief
   *
   * Query:
   *
   * GET /_search
   * {
   *   "query": {
   *     "regexp": {
   *       "text": {
   *         "value": $pattern
   *       }
   *     }
   *   }
   * }
   *
   * @param index
   * @param pattern
   * @return StatusOr<ESQueryResult>
   */
  virtual StatusOr<ESQueryResult> regexp(const std::string& index,
                                         const std::string& pattern,
                                         int64_t size = -1,
                                         int64_t timeout = -1);

  /**
   * @brief
   *
   * Query:
   *
   * GET /_search
   * {
   *   "query": {
   *     "wildcard": {
   *       "text": {
   *         "value": $pattern
   *       }
   *     }
   *   }
   * }
   *
   * @param index
   * @param pattern
   * @return StatusOr<ESQueryResult>
   */
  virtual StatusOr<ESQueryResult> wildcard(const std::string& index,
                                           const std::string& pattern,
                                           int64_t size = -1,
                                           int64_t timeout = -1);

  // /**
  //  * @brief
  //  *
  //  * Query:
  //  *
  //  * GET /_search
  //  * {
  //  *   "query": {
  //  *     "bool": {
  //  *       "must": [
  //  *         {
  //  *           "term": {
  //  *             "text": {
  //  *               "value": $words[i]
  //  *             }
  //  *           }
  //  *         },
  //  *         ...
  //  *       ]
  //  *     }
  //  *   }
  //  * }
  //  *
  //  * @param index
  //  * @param words
  //  * @return StatusOr<ESQueryResult>
  //  */
  // StatusOr<ESQueryResult> term(const std::string& index, const std::vector<std::string>& words);

  virtual StatusOr<ESQueryResult> match_all(const std::string& index);
  StatusOr<ESQueryResult> query(const std::string& index,
                                const folly::dynamic& query,
                                int64_t timeout);

 protected:
  static std::string genDocID(const std::string& vid,
                              const std::string& src,
                              const std::string& dst,
                              int64_t rank);

  ESClient& randomClient();
  std::vector<ESClient> clients_;

  friend class ESBulk;
};

}  // namespace nebula::plugin

#endif
