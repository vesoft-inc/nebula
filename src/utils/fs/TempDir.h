/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_FS_TEMPDIR_H_
#define COMMON_FS_TEMPDIR_H_

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"

namespace nebula {
namespace fs {

class TempDir final {
public:
    // When the flag deleteOnDestroy == true, the temp directory will be
    // deleted along with all contents when this TempDir object is destroyed.
    // Otherwise, the temp directory will be left behind.
    //
    // Either pathTemplate or nameTemplate must contain 6 capital letter "X", such
    // as "/tmp/temp_dir.XXXXXX" or "temp_dir.XXXXXX". The "XXXXXX" will be
    // replaced by the random string.
    //
    // If the temp directory is created successfully, path() will return the
    // full path name, otherwise, path() will return empty
    explicit TempDir(const char* pathTemplate, bool deleteOnDestroy = true);
    TempDir(const char* parent,
            const char* nameTemplate,
            bool deleteOnDestroy = true)
        : TempDir(FileUtils::joinPath(parent, nameTemplate).c_str(),
                  deleteOnDestroy) {}

    ~TempDir();

    const char* path() const {
        return dirPath_.get();
    }

private:
    std::unique_ptr<char[]> dirPath_;
    bool deleteOnDestroy_;
};

}  // namespace fs
}  // namespace nebula

#endif  // COMMON_FS_TEMPDIR_H_
