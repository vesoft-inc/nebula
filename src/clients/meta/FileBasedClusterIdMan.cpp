/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/clients/meta/FileBasedClusterIdMan.h"


namespace nebula {
namespace meta {

// static
bool FileBasedClusterIdMan::persistInFile(ClusterID clusterId,
                                          const std::string& filename) {
    auto dirname = fs::FileUtils::dirname(filename.c_str());
    if (!fs::FileUtils::makeDir(dirname)) {
        LOG(ERROR) << "Failed mkdir " << dirname;
        return false;
    }
    if (fs::FileUtils::remove(filename.c_str())) {
        LOG(INFO) << "Remove the existed file " << filename;
    }
    int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        LOG(ERROR) << "Open file error, file " << filename << ", error " << strerror(errno);
        return false;
    }
    int bytes = ::write(fd, reinterpret_cast<const char*>(&clusterId), sizeof(ClusterID));
    if (bytes != sizeof(clusterId)) {
        LOG(ERROR) << "Write clusterId failed!";
        ::close(fd);
        return false;
    }
    LOG(INFO) << "Persiste clusterId " << clusterId << " succeeded!";
    ::close(fd);
    return true;
}


// static
ClusterID FileBasedClusterIdMan::getClusterIdFromFile(const std::string& filename) {
    int fd = ::open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(WARNING) << "Open file failed, error " << strerror(errno);
        return 0;
    }
    ClusterID clusterId = 0;
    int len = ::read(fd, reinterpret_cast<char*>(&clusterId), sizeof(ClusterID));
    if (len != sizeof(ClusterID)) {
        LOG(ERROR) << "Get clusterId failed!";
        ::close(fd);
        return 0;
    }
    LOG(INFO) << "Get clusterId: " << clusterId;
    ::close(fd);
    return clusterId;
}

}  // namespace meta
}  // namespace nebula
