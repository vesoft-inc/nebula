/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "fs/TempFile.h"

namespace vesoft {
namespace fs {

TempFile::TempFile(const char *path, bool autoDelete) {
    autoDelete_ = autoDelete;
    auto len = ::strlen(path);
    path_ = std::make_unique<char[]>(len + 1);
    ::strcpy(path_.get(), path);

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
}   // namespace vesoft
