/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/GetMetaDirInfoProcessor.h"

#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <boost/filesystem/operations.hpp>  // for current_path
#include <boost/filesystem/path.hpp>        // for path
#include <ostream>                          // for operator<<, basic_ostream
#include <string>                           // for basic_string, operator<<
#include <vector>                           // for vector

#include "common/base/Base.h"                 // for UNUSED
#include "common/base/Logging.h"              // for COMPACT_GOOGLE_LOG_INFO
#include "interface/gen-cpp2/common_types.h"  // for DirInfo, ErrorCode, Err...
#include "kvstore/KVStore.h"                  // for KVStore

namespace nebula {
namespace meta {

void GetMetaDirInfoProcessor::process(const cpp2::GetMetaDirInfoReq& req) {
  UNUSED(req);

  auto datapaths = kvstore_->getDataRoot();
  nebula::cpp2::DirInfo dir;
  dir.data_ref() = datapaths;
  dir.root_ref() = boost::filesystem::current_path().string();
  VLOG(1) << "Get meta dir info, data paths size: " << dir.get_data().size()
          << ", root path:" << dir.get_root();

  resp_.dir_ref() = std::move(dir);
  resp_.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  onFinished();
}
}  // namespace meta
}  // namespace nebula
