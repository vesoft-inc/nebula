/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/AdminTask.h"

#include "storage/admin/CompactTask.h"           // for CompactTask
#include "storage/admin/FlushTask.h"             // for FlushTask
#include "storage/admin/RebuildEdgeIndexTask.h"  // for RebuildEdgeIndexTask
#include "storage/admin/RebuildFTIndexTask.h"    // for RebuildFTIndexTask
#include "storage/admin/RebuildTagIndexTask.h"   // for RebuildTagIndexTask
#include "storage/admin/StatsTask.h"             // for StatsTask

namespace nebula {
namespace storage {
class StorageEnv;

class StorageEnv;

std::shared_ptr<AdminTask> AdminTaskFactory::createAdminTask(StorageEnv* env, TaskContext&& ctx) {
  FLOG_INFO("%s (%d, %d)", __func__, ctx.jobId_, ctx.taskId_);
  std::shared_ptr<AdminTask> ret;
  switch (ctx.cmd_) {
    case meta::cpp2::AdminCmd::COMPACT:
      ret = std::make_shared<CompactTask>(env, std::move(ctx));
      break;
    case meta::cpp2::AdminCmd::FLUSH:
      ret = std::make_shared<FlushTask>(env, std::move(ctx));
      break;
    case meta::cpp2::AdminCmd::REBUILD_TAG_INDEX:
      ret = std::make_shared<RebuildTagIndexTask>(env, std::move(ctx));
      break;
    case meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX:
      ret = std::make_shared<RebuildEdgeIndexTask>(env, std::move(ctx));
      break;
    case meta::cpp2::AdminCmd::REBUILD_FULLTEXT_INDEX:
      ret = std::make_shared<RebuildFTIndexTask>(env, std::move(ctx));
      break;
    case meta::cpp2::AdminCmd::STATS:
      ret = std::make_shared<StatsTask>(env, std::move(ctx));
      break;
    default:
      break;
  }
  return ret;
}

}  // namespace storage
}  // namespace nebula
