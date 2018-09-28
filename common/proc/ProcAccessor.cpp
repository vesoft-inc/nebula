/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "proc/ProcAccessor.h"

namespace vesoft {
namespace proc {

ProcAccessor::~ProcAccessor() {
    if (file_.is_open()) {
        file_.close();
    }
    if (dir_ != NULL) {
        closedir(dir_);
    }
}


bool ProcAccessor::checkFileType() {
    struct stat st;
    memset(&st, 0, sizeof(st));
    auto ok = (stat(path_.c_str(), &st) == 0);
    if (!ok) {
        LOG(ERROR) << "Failed to read " << path_
                   << ": " << strerror(errno);
        return false;
    }

    if (S_ISDIR(st.st_mode)) {
        type_ = TYPE_DIR;
    } else {
        type_ = TYPE_FILE;
    }

    return true;
}


bool ProcAccessor::next(std::string& entry) {
    if (type_ == TYPE_UNKOWN) {
        if (!checkFileType()) {
            return false;
        }
    }

    if (type_ == TYPE_DIR) {
        return dirNext(entry);
    } else {
        return fileNext(entry);
    }
}


bool ProcAccessor::next(const std::regex& regex, std::smatch& sm) {
    static thread_local std::string entry;
    auto ok = false;
    while ((ok = next(entry))) {
        if (!std::regex_search(entry, sm, regex)) {
            continue;
        }
        break;
    }
    return ok;
}


bool ProcAccessor::fileNext(std::string& line) {
    assert(type_ == TYPE_FILE);

    line.clear();
    if (!file_.is_open()) {
        file_.open(path_);
        if (!file_.is_open()) {
            LOG(ERROR) << "Failed to open " << path_
                       << ": " << strerror(errno);
            return false;
        }
    }
    return !!std::getline(file_, line);
}


bool ProcAccessor::dirNext(std::string& entry) {
    assert(type_ == TYPE_DIR);

    if (dir_ == NULL) {
        dir_ = opendir(path_.c_str());
        if (dir_ == NULL) {
            LOG(ERROR) << "Failed to read " << path_
                       << ": " << strerror(errno);
            return false;
        }
    }

    struct dirent* dent;
    while ((dent = readdir(dir_)) != NULL) {
        if (dent->d_name[0] == '.') {   // skip `.' and `..'
            continue;
        }
        break;
    }

    if (dent == NULL) {
        return false;
    }

    entry = dent->d_name;
    return true;
}

}   // namespace proc
}   // namespace vesoft
