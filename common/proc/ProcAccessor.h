/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef COMMON_PROC_PROCACCESSOR_H_
#define COMMON_PROC_PROCACCESSOR_H_

#include "base/Base.h"
#include <dirent.h>

namespace vesoft {
namespace proc {

/**
 * This is a helper class to access statistic files in the /proc filesystem.
 * It supports both directory iteration and ordinary file reads:
 *      For directory accessing, next() returns a sub-directory each time
 *      For file reading, next() returns next line in the file
 */
class ProcAccessor final {
public:
    explicit ProcAccessor(std::string path) : path_(std::move(path)) {}
    ~ProcAccessor();

    // Get next sub-directory or next line
    bool next(std::string& entry);
    // Read next matched entry
    // Return false if there is nothing left
    bool next(const std::regex& regex, std::smatch& sm);

private:
    enum FileType {
        TYPE_DIR,
        TYPE_FILE,
        TYPE_UNKOWN,
    };

    std::string path_;
    std::ifstream file_;
    DIR* dir_{NULL};
    FileType type_{TYPE_UNKOWN};

    bool checkFileType();
    bool fileNext(std::string& line);
    bool dirNext(std::string& entry);
};

}   // namespace proc
}   // namespace vesoft

#endif  // COMMON_PROC_PROCACCESSOR_H_
