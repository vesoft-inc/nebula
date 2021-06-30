/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/ListClusterInfoProcessor.h"
#include <boost/filesystem.hpp>
#include "common/fs/FileUtils.h"

namespace nebula {
namespace storage {

void ListClusterInfoProcessor::process(const cpp2::ListClusterInfoReq& req) {
    UNUSED(req);
    CHECK_NOTNULL(env_);

    auto data_root = env_->kvstore_->getDataRoot();

    std::vector<std::string> realpaths;
    bool failed = false;
    std::transform(std::make_move_iterator(data_root.begin()),
                   std::make_move_iterator(data_root.end()),
                   std::back_inserter(realpaths),
                   [&failed](auto f) {
                       if (f[0] == '/') {
                           return f;
                       }
                       auto result = nebula::fs::FileUtils::realPath(f.c_str());
                       if (!result.ok()) {
                           LOG(ERROR) << "Failed to get the absolute path of file: " << f;
                           failed = true;
                           return f;
                       }
                       return std::string(result.value());
                   });
    if (failed) {
        cpp2::PartitionResult thriftRet;
        thriftRet.set_code(nebula::cpp2::ErrorCode::E_FAILED_GET_ABS_PATH);
        codes_.emplace_back(std::move(thriftRet));
        onFinished();
        return;
    }
    nebula::cpp2::DirInfo dir;
    dir.set_data(std::move(realpaths));
    dir.set_root(boost::filesystem::current_path().string());

    resp_.set_dir(std::move(dir));

    onFinished();
}

}   // namespace storage
}   // namespace nebula
