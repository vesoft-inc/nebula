/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/GetMetaDirInfoProcessor.h"

#include <boost/filesystem.hpp>

#include "common/fs/FileUtils.h"

namespace nebula {
namespace meta {

void GetMetaDirInfoProcessor::process(const cpp2::GetMetaDirInfoReq& req) {
  UNUSED(req);

  auto datapaths = kvstore_->getDataRoot();
  nebula::cpp2::DirInfo dir;
  dir.data_ref() = datapaths;
  dir.root_ref() = boost::filesystem::current_path().string();
  resp_.dir_ref() = std::move(dir);

  resp_.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  onFinished();
}
}  // namespace meta
}  // namespace nebula
