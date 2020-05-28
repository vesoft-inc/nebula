/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempFile.h"

namespace nebula {
namespace fs {

TempFile::TempFile(const char *path, bool autoDelete) {
    autoDelete_ = autoDelete;
    auto len = ::strlen(path);
    path_ = std::make_unique<char[]>(len + 1);
    ::strcpy(path_.get(), path);    // NOLINT

    auto fd = ::mkstemp(path_.get());
    if (fd == -1) {
        throw std::runtime_error(std::string(path) + ": " + strerror(errno));
    }
    ::close(fd);
}

TempFile::~TempFile() {
    if (autoDelete_) {
        ::unlink(path_.get());
    }
}

const char* TempFile::path() const {
    return path_.get();
}

}   // namespace fs
}   // namespace nebula
