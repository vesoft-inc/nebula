/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "base/Base.h"
#include "network/NetworkUtils.h"
#include "time/TimeUtils.h"
#include "fs/FileUtils.h"
#include "ClusterManager.h"

namespace nebula {
namespace kvstore {

    const char* ClusterManager::fileName_ = "cluster_id.txt";

    ResultCode ClusterManager::init() {
        auto result = loadClusterId();
        if (result == ResultCode::SUCCEEDED) {
            return result;
        }
        createClusterId();
        return dumpClusterId();
    }

    ResultCode ClusterManager::loadClusterId() {
        auto filePath = folly::stringPrintf("%s/%s", dataPath_.c_str(), fileName_);
        int fd = ::open(filePath.c_str(), O_RDONLY);
        if (fd < 0) {
            LOG(WARNING) << "loadClusterId failed!";
            return ResultCode::ERR_UNKNOWN;
        }
        char buff[32] = {};
        int len = ::read(fd, buff, sizeof(buff));
        buff[len] = '\0';

        LOG(INFO) << "loadClusterId succeed! string clusterId: " << buff;
        ClusterID clusterId = std::stol(buff);
        LOG(INFO) << "int clusterId: " << clusterId;
        if (clusterId == 0) {
            return ResultCode::ERR_UNKNOWN;
        } else {
            setClusterId(clusterId);
            return ResultCode::SUCCEEDED;
        }
    }

    ResultCode ClusterManager::createClusterId() {
        // localhost int ip
        uint32_t intIp = storeSvcAddr_.first;
        uint32_t intPort = storeSvcAddr_.second;
        std::string curStrIp = nebula::network::NetworkUtils::ipFromHostAddr(storeSvcAddr_);
        LOG(INFO) << "curStrIp: " << curStrIp << ", intPort: " << intPort;

        int32_t nowTime = nebula::time::TimeUtils::nowInSeconds();
        ClusterID clusterId = nowTime;
        clusterId <<= 32;
        clusterId += intIp;
        clusterId += intPort;
        setClusterId(clusterId);
        return ResultCode::SUCCEEDED;
    }

    ResultCode ClusterManager::dumpClusterId() {
        auto filePath = folly::stringPrintf("%s/%s", dataPath_.c_str(), fileName_);
        int status = ::access(dataPath_.c_str(), O_WRONLY);
        if (0 != status) {  // file not exists or can't be accessed
            if (EACCES == errno) {
                return ResultCode::ERR_IO_ERROR;
            } else if (ENOENT == errno) {
                // Todo ,create DIR AND FILE
                ResultCode ret = mkdirsMultiLevel(dataPath_);
                if (ResultCode::SUCCEEDED != ret) {
                    return ret;
                }
            }
        }
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
        int fd = open(filePath.c_str(), flags, 0644);
        if (fd < 0) {
            return ResultCode::ERR_IO_ERROR;
        }
        std::string strClusterId = std::to_string(clusterId_);
        LOG(INFO) << "dump strClusterId: " << strClusterId;
        ::write(fd, strClusterId.c_str(), strClusterId.size());
        ::close(fd);
        return ResultCode::SUCCEEDED;
    }


    ResultCode ClusterManager::mkdirsMultiLevel(std::string& path) {
        // remove prefix and suffix spaces
        const int kSize = path.size();
        int start = 0;
        while (start < kSize && ::isspace(path[start])) {
            ++start;
        }
        if (start >= kSize) {
            return ResultCode::SUCCEEDED;
        }
        int end = kSize - 1;
        while (start >= 0 && ::isspace(path[end])) {
            --end;
        }
        if (end < 0) {
            return ResultCode::SUCCEEDED;
        }
        path = path.substr(start, end - start + 1);

        bool isAbsPath = path[0] == '/';
        std::vector<std::string> pieces;
        splitPath(path, pieces, '/');
        if (pieces.empty()) {
            return ResultCode::SUCCEEDED;
        }

        std::string curPath;
        if (isAbsPath) {
            curPath += "/";
        }
        for (auto& piece : pieces) {
            curPath += piece;
            int mode = R_OK | W_OK | X_OK;
            bool status = ::access(curPath.c_str(), mode);
            if (0 != status) {
                if (ENOENT == errno) {
                    int dirMode = 0755;
                    int dirStatus = ::mkdir(curPath.c_str(), dirMode);
                    if (0 != dirStatus) {
                        return ResultCode::ERR_IO_ERROR;
                    }
                } else {
                    return ResultCode::ERR_IO_ERROR;
                }
            } else {
                continue;
            }
        }
        return ResultCode::SUCCEEDED;
    }


    void ClusterManager::splitPath(const std::string& path,
                   std::vector<std::string>& pieces,
                   char delim) {
        if (path.empty()) {
            return;
        }

        const int kSize = path.size();
        int i = 0;
        while (i < kSize) {
            int start = i;
            while (i < kSize && path[i] != delim) ++i;
            if (i != start) {
                pieces.emplace_back(path.substr(start, i - start));
            }
            ++i;
        }
    }

}  // namespace kvstore
}  // namespace nebula

