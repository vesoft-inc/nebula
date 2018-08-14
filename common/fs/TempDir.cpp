/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "fs/TempDir.h"

namespace vesoft {
namespace fs {

TempDir::TempDir(const char* pathTemplate, bool deleteOnDestroy)
        : deleteOnDestroy_(deleteOnDestroy) {
    auto len = strlen(pathTemplate);
    std::unique_ptr<char[]> name(new char[len + 1]);
    strncpy(name.get(), pathTemplate, len);
    name.get()[len] = '\0';

    VLOG(2) << "Trying to create the temp directory \""
            << name.get() << "\"";

    if (!mkdtemp(name.get())) {
        // Failed
        LOG(ERROR) << "Failed to created the temp directory with template \""
                   << name.get() << "\" (" << errno << "): "
                   << strerror(errno);
    } else {
        dirPath_ = std::move(name);
    }
}


TempDir::~TempDir() {
    if (deleteOnDestroy_ && !!dirPath_) {
        FileUtils::remove(dirPath_.get(), true);
    }
}

}  // namespace fs
}  // namespace vesoft

