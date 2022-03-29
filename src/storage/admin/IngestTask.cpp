/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/IngestTask.h"

#include "common/fs/FileUtils.h"

namespace nebula {
namespace storage {

bool IngestTask::check() {
  return env_->kvstore_ != nullptr;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> IngestTask::genSubTasks() {
  std::vector<AdminSubTask> results;
  auto* store = dynamic_cast<kvstore::NebulaStore*>(env_->kvstore_);
  auto errOrSpace = store->space(*ctx_.parameters_.space_id_ref());
  if (!ok(errOrSpace)) {
    LOG(ERROR) << "Space not found";
    return error(errOrSpace);
  }

  auto space = nebula::value(errOrSpace);
  results.emplace_back([space = space]() {
    for (auto& engine : space->engines_) {
      auto parts = engine->allParts();
      for (auto part : parts) {
        auto path = folly::stringPrintf("%s/download/%d", engine->getDataRoot(), part);
        if (!fs::FileUtils::exist(path)) {
          LOG(INFO) << path << " not existed";
          continue;
        }

        auto files = nebula::fs::FileUtils::listAllFilesInDir(path.c_str(), true, "*.sst");
        LOG(INFO) << "Ingest files: " << files.size();
        auto code = engine->ingest(std::vector<std::string>(files));
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
          return code;
        }
      }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  });
  return results;
}

}  // namespace storage
}  // namespace nebula
