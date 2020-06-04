/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"

namespace nebula {
namespace fs {

TempDir::TempDir(const char* pathTemplate, bool deleteOnDestroy)
        : deleteOnDestroy_(deleteOnDestroy) {
    auto len = strlen(pathTemplate);
    std::unique_ptr<char[]> name(new char[len + 1]);
    strcpy(name.get(), pathTemplate);   // NOLINT

    VLOG(2) << "Trying to create the temp directory with pattern \""
            << name.get() << "\"";

    if (!mkdtemp(name.get())) {
        // Failed
        LOG(ERROR) << "Failed to created the temp directory with template \""
                   << name.get() << "\" (" << errno << "): "
                   << strerror(errno);
    } else {
        dirPath_ = std::move(name);
        VLOG(2) << "Created temporary directory " << dirPath_.get();
    }
}


TempDir::~TempDir() {
    if (deleteOnDestroy_ && !!dirPath_) {
        FileUtils::remove(dirPath_.get(), true);
    }
}

}  // namespace fs
}  // namespace nebula
