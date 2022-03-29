/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/AdminTask.h"

#include "storage/admin/CompactTask.h"
#include "storage/admin/DownloadTask.h"
#include "storage/admin/FlushTask.h"
#include "storage/admin/IngestTask.h"
#include "storage/admin/RebuildEdgeIndexTask.h"
#include "storage/admin/RebuildFTIndexTask.h"
#include "storage/admin/RebuildTagIndexTask.h"
#include "storage/admin/StatsTask.h"

namespace nebula {
namespace storage {

std::shared_ptr<AdminTask> AdminTaskFactory::createAdminTask(StorageEnv* env, TaskContext&& ctx) {
  FLOG_INFO("%s (%d, %d)", __func__, ctx.jobId_, ctx.taskId_);
  std::shared_ptr<AdminTask> ret;
  switch (ctx.jobType_) {
    case meta::cpp2::JobType::COMPACT:
      ret = std::make_shared<CompactTask>(env, std::move(ctx));
      break;
    case meta::cpp2::JobType::FLUSH:
      ret = std::make_shared<FlushTask>(env, std::move(ctx));
      break;
    case meta::cpp2::JobType::REBUILD_TAG_INDEX:
      ret = std::make_shared<RebuildTagIndexTask>(env, std::move(ctx));
      break;
    case meta::cpp2::JobType::REBUILD_EDGE_INDEX:
      ret = std::make_shared<RebuildEdgeIndexTask>(env, std::move(ctx));
      break;
    case meta::cpp2::JobType::REBUILD_FULLTEXT_INDEX:
      ret = std::make_shared<RebuildFTIndexTask>(env, std::move(ctx));
      break;
    case meta::cpp2::JobType::STATS:
      ret = std::make_shared<StatsTask>(env, std::move(ctx));
      break;
    case meta::cpp2::JobType::DOWNLOAD:
      ret = std::make_shared<DownloadTask>(env, std::move(ctx));
      break;
    case meta::cpp2::JobType::INGEST:
      ret = std::make_shared<IngestTask>(env, std::move(ctx));
      break;
    default:
      break;
  }
  return ret;
}

}  // namespace storage
}  // namespace nebula
