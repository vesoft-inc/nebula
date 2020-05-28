/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_FS_TEMPFILE_H_
#define COMMON_FS_TEMPFILE_H_

#include "common/base/Base.h"

/**
 * TempFile is a wrapper on `mkstemp' to create a temporary file.
 * TempFile is only responsible for the creation and optional deletion.
 */

namespace nebula {
namespace fs {

class TempFile final {
public:
    /**
     * @path    a path template like "/tmp/foobar.XXXXXX"(six `X's), please refer to `mkstemp(3)'
     * @autoDelete  whether to unlink the created file automatically on destruction.
     * The file would be created with the mode 0600.
     * May throw std::runtime_error on failure.
     */
    explicit TempFile(const char *path, bool autoDelete = true);
    ~TempFile();
    /**
     * Return the actual path of the created temporary file.
     */
    const char* path() const;

private:
    bool                                        autoDelete_;
    std::unique_ptr<char[]>                     path_;
};

}   // namespace fs
}   // namespace nebula

#endif  // COMMON_FS_TEMPFILE_H_
