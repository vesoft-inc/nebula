/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/DownloadJobExecutor.h"

#include "common/hdfs/HdfsHelper.h"
#include "common/utils/MetaKeyUtils.h"
#include "meta/MetaServiceUtils.h"

namespace nebula {
namespace meta {

DownloadJobExecutor::DownloadJobExecutor(GraphSpaceID space,
                                         JobID jobId,
                                         kvstore::KVStore* kvstore,
                                         AdminClient* adminClient,
                                         const std::vector<std::string>& paras)
    : SimpleConcurrentJobExecutor(space, jobId, kvstore, adminClient, paras) {
  helper_ = std::make_unique<nebula::hdfs::HdfsCommandHelper>();
}

nebula::cpp2::ErrorCode DownloadJobExecutor::check() {
  if (paras_.size() != 1) {
    return nebula::cpp2::ErrorCode::E_INVALID_JOB;
  }

  auto& url = paras_[0];
  std::string hdfsPrefix = "hdfs://";
  if (url.find(hdfsPrefix) != 0) {
    LOG(ERROR) << "URL should start with " << hdfsPrefix;
    return nebula::cpp2::ErrorCode::E_INVALID_JOB;
  }

  auto u = url.substr(hdfsPrefix.size(), url.size());
  std::vector<folly::StringPiece> tokens;
  folly::split(":", u, tokens);
  if (tokens.size() == 2) {
    host_ = std::make_unique<std::string>(tokens[0]);
    int32_t position = tokens[1].find_first_of("/");
    if (position != -1) {
      try {
        port_ = folly::to<int32_t>(tokens[1].toString().substr(0, position).c_str());
      } catch (const std::exception& ex) {
        LOG(ERROR) << "URL's port parse failed: " << url;
        return nebula::cpp2::ErrorCode::E_INVALID_JOB;
      }
      path_ =
          std::make_unique<std::string>(tokens[1].toString().substr(position, tokens[1].size()));
    } else {
      LOG(ERROR) << "URL Parse Failed: " << url;
      return nebula::cpp2::ErrorCode::E_INVALID_JOB;
    }
  } else {
    LOG(ERROR) << "URL Parse Failed: " << url;
    return nebula::cpp2::ErrorCode::E_INVALID_JOB;
  }

  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode DownloadJobExecutor::prepare() {
  auto errOrHost = getTargetHost(space_);
  if (!nebula::ok(errOrHost)) {
    LOG(ERROR) << "Can't get any host according to space";
    return nebula::error(errOrHost);
  }

  LOG(INFO) << "HDFS host: " << *host_.get() << " port: " << port_ << " path: " << *path_.get();

  auto listResult = helper_->ls(*host_.get(), port_, *path_.get());
  if (!listResult.ok()) {
    LOG(ERROR) << "Dispatch SSTFile Failed";
    return nebula::cpp2::ErrorCode::E_INVALID_JOB;
  }

  taskParameters_.emplace_back(*host_.get());
  taskParameters_.emplace_back(folly::to<std::string>(port_));
  taskParameters_.emplace_back(*path_.get());
  std::unique_ptr<kvstore::KVIterator> iter;
  auto prefix = MetaKeyUtils::partPrefix(space_);
  auto result = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Fetch Parts Failed";
  }
  return result;
}

folly::Future<Status> DownloadJobExecutor::executeInternal(HostAddr&& address,
                                                           std::vector<PartitionID>&& parts) {
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  adminClient_
      ->addTask(cpp2::JobType::DOWNLOAD,
                jobId_,
                taskId_++,
                space_,
                std::move(address),
                taskParameters_,
                std::move(parts))
      .then([pro = std::move(pro)](auto&& t) mutable {
        CHECK(!t.hasException());
        auto status = std::move(t).value();
        if (status.ok()) {
          pro.setValue(Status::OK());
        } else {
          pro.setValue(status.status());
        }
      });
  return f;
}

}  // namespace meta
}  // namespace nebula
