/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef NEBULA_GRAPH_ALTERSPACEPROCESSOR_H
#define NEBULA_GRAPH_ALTERSPACEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Alter space properties, only support altering zones now.
 *
 */
class AlterSpaceProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static AlterSpaceProcessor* instance(kvstore::KVStore* kvstore) {
    return new AlterSpaceProcessor(kvstore);
  }

  void process(const cpp2::AlterSpaceReq& req);

 private:
  nebula::cpp2::ErrorCode addZones(const std::string& spaceName,
                                   const std::vector<std::string>& zones);

 private:
  explicit AlterSpaceProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // NEBULA_GRAPH_ALTERSPACEPROCESSOR_H
