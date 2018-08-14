/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "fs/FileUtils.h"
#include <dirent.h>
#include <fnmatch.h>

namespace vesoft {
namespace fs {

static const int32_t kMaxPathLen = 1024;

namespace detail {

bool removeDir(const char* path, bool recursively) {
    if (recursively) {
        // Assuming the path is a directory
        DIR* dh = opendir(path);
        if (!dh) {
            LOG(ERROR) << "Failed to read the directory \"" << path
                       << "\" (" << errno << "): " << strerror(errno);
            return false;
        }
    
        bool succeeded = true;
        struct dirent* dEnt;
        errno = 0;
        while (succeeded && !!(dEnt = readdir(dh))) {
            if (!strcmp(dEnt->d_name, ".") || !strcmp(dEnt->d_name, "..")) {
                // Skip "." and ".."
                continue;
            }
            if (dEnt->d_type == DT_DIR && !recursively) {
                LOG(ERROR) << "Cannot remove the directory \"" << path
                           << "\" because it contains sub-directory \""
                           << dEnt->d_name << "\"";
                succeeded = false;
            } else {
                // Remove the directory entry
                succeeded = FileUtils::remove(
                    FileUtils::joinPath(path, dEnt->d_name).c_str(), recursively);
                if (!succeeded) {
                    LOG(ERROR) << "Failed to remove \"" << dEnt->d_name
                               << "\" in \"" << path
                               << "\"";
                } else {
                    VLOG(2) << "Succeeded removing \"" << dEnt->d_name << "\"";
                }
            }
        }
    
        if (succeeded && errno) {
            // There is an error
            LOG(ERROR) << "Failed to read the directory \"" << path
                       << "\" (" << errno << "): " << strerror(errno);
            succeeded = false;
        }
    
        if (closedir(dh)) {
            // Failed to close the directory stream
            LOG(ERROR) << "Failed to close the directory stream (" << errno
                       << "): " << strerror(errno);
            return false;
        }
    
        if (!succeeded) {
            LOG(ERROR) << "Failed to remove the content of the directory \""
                       << path << "\"";
            return false;
        }
    }

    // All content has been removed, now remove the directory itself
    if (rmdir(path)) {
        LOG(ERROR) << "Failed to remove the directory \"" << path
                   << "\" (" << errno << "): " << strerror(errno);
        return false;
    }
    return true;
}

}  // namespace detail


std::string FileUtils::getExePath() {
    pid_t pid = getpid();
    char symLink[kMaxPathLen];
    snprintf(symLink, kMaxPathLen, "/proc/%d/exe", pid);

    char path[kMaxPathLen];
    ssize_t len = readlink(symLink, path, kMaxPathLen);
    path[len == kMaxPathLen ? len - 1 : len] = '\0';

    return path;
}


std::string FileUtils::getExeCWD() {
    pid_t pid = getpid();
    char symLink[kMaxPathLen];
    snprintf(symLink, kMaxPathLen, "/proc/%d/cwd", pid);

    char path[kMaxPathLen];
    ssize_t len = readlink(symLink, path, kMaxPathLen);
    path[len == kMaxPathLen ? len - 1 : len] = '\0';

    return path;
}


const char* FileUtils::getFileTypeName(FileType type) {
    static const char* kTypeNames[] = {
        "Unknown",
        "NotExist",
        "Regular",
        "Directory",
        "SoftLink",
        "CharDevice",
        "BlockDevice",
        "FIFO",
        "Socket"
    };

    return kTypeNames[static_cast<int>(type)];
}


size_t FileUtils::fileSize(const char* path) {
    struct stat st;
    if (lstat(path, &st)) {
        // Failed o get file stat
        VLOG(3) << "Failed to get infomation about \"" << path
                << "\" (" << errno << "): " << strerror(errno);
        return 0;
    }

    return st.st_size;
}


FileType FileUtils::fileType(const char* path) {
    struct stat st;
    if (lstat(path, &st)) {
        if (errno == ENOENT) {
            VLOG(3) << "The path \"" << path << "\" does not exist";
            return FileType::NOTEXIST;
        } else {
            // Failed o get file stat
            VLOG(3) << "Failed to get infomation about \"" << path
                    << "\" (" << errno << "): " << strerror(errno);
            return FileType::UNKNOWN;
        }
    }

    if (S_ISREG(st.st_mode)) {
        return FileType::REGULAR;
    } else if (S_ISDIR(st.st_mode)) {
        return FileType::DIRECTORY;
    } else if (S_ISLNK(st.st_mode)) {
        return FileType::SYM_LINK;
    } else if (S_ISCHR(st.st_mode)) {
        return FileType::CHAR_DEV;
    } else if (S_ISBLK(st.st_mode)) {
        return FileType::BLOCK_DEV;
    } else if (S_ISFIFO(st.st_mode)) {
        return FileType::FIFO;
    } else if (S_ISSOCK(st.st_mode)) {
        return FileType::SOCKET;
    }

    return FileType::UNKNOWN;
}


int64_t FileUtils::fileLastUpdateTime(const char* path) {
    struct stat st;
    if (lstat(path, &st)) {
        // Failed to get file stat
        LOG(ERROR) << "Failed to get file infomation for \"" << path
                   << "\" (" << errno << "): " << strerror(errno);
        return -1;
    }
    return st.st_mtime;
}


bool FileUtils::isStdinTTY() {
    return isFdTTY(::fileno(stdin));
}


bool FileUtils::isStdoutTTY() {
    return isFdTTY(::fileno(stdout));
}


bool FileUtils::isStderrTTY() {
    return isFdTTY(::fileno(stderr));
}


bool FileUtils::isFdTTY(int fd) {
    return ::isatty(fd) == 1;
}


std::string FileUtils::joinPath(const folly::StringPiece dir,
                                const folly::StringPiece filename) {
    std::string buf;

    std::size_t len = dir.size();
    if (len == 0) {
        buf.resize(filename.size() + 2);
        strncpy(&(buf[0]), "./", 2);
        strncpy(&(buf[2]), filename.begin(), filename.size());
        return std::move(buf);
    }

    if (dir[len-1] == '/') {
        buf.resize(len + filename.size());
        strncpy(&(buf[0]), dir.data(), len);
    } else {
        buf.resize(len + filename.size() + 1);
        strncpy(&(buf[0]), dir.data(), len);
        buf[len++] = '/';
    }

    strncpy(&(buf[len]), filename.data(), filename.size());

    return std::move(buf);
}


void FileUtils::dividePath(const folly::StringPiece path,
                           folly::StringPiece& parent,
                           folly::StringPiece& child) {
    if (path.empty() || path == "/") {
        // The given string is empty or just "/"
        parent = folly::StringPiece();
        child = path;
        return;
    }

    folly::StringPiece pathToLook =
        (path.back() == '/') ? folly::StringPiece(path.begin(), path.size() - 1)
                             : path;
    auto pos = pathToLook.rfind('/');
    if (pos == std::string::npos) {
        // Not found
        parent = folly::StringPiece();
        child = pathToLook;
        return;
    }

    // Found the last "/"
    child = folly::StringPiece(pathToLook.begin() + pos + 1,
                               pathToLook.size() - pos - 1);
    if (pos == 0) {
        // In the root directory
        parent = folly::StringPiece(pathToLook.begin(), 1);
    } else {
        parent = folly::StringPiece(pathToLook.begin(), pos);
    }

    return;
}


bool FileUtils::remove(const char* path, bool recursively) {
    auto type = fileType(path);
    switch (type) {
    case FileType::REGULAR:
    case FileType::SYM_LINK:
        // Regular file or link
        if (unlink(path)) {
            // Failed
            LOG(ERROR) << "Failed to remove the file \"" << path
                       << "\" (" << errno << "): " << strerror(errno);
            return false;
        }
        return true;
    case FileType::DIRECTORY:
        // Directory
        return detail::removeDir(path, recursively);
    case FileType::CHAR_DEV:
    case FileType::BLOCK_DEV:
    case FileType::FIFO:
    case FileType::SOCKET:
        LOG(ERROR) << "Only a directory, a regular file, or a soft link"
                   << " can be removed. But \"" << path << "\" is a "
                   << getFileTypeName(type);
        return false;
    case FileType::NOTEXIST:
        VLOG(2) << "The path \"" << path << "\" does not exist";
        return true;
    default:
        LOG(ERROR) << "We don't know the type of \"" << path << "\"";
        return false;
    }
}


bool FileUtils::makeDir(const std::string& dir) {
    if (dir.empty()) {
        return false;
    }
    FileType type = fileType(dir.c_str());
    if (type == FileType::DIRECTORY) {
        // The directory already exists
        return true;
    } else if (type != FileType::NOTEXIST) {
        // A file has existed, cannot create the directory
        return false;
    }

    folly::StringPiece parent;
    folly::StringPiece child;
    dividePath(dir, parent, child);

    // create parent if it is not empty
    if (!parent.empty()) {
        bool ret = makeDir(parent.toString());
        if (!ret) {
            return false;
        }
    }

    int err = mkdir(dir.c_str(), S_IRWXU);
    if (err != 0) {
        if (fileType(dir.c_str()) == FileType::DIRECTORY) {
            return true;
        }
        return false;
    }
    return true;
}


std::vector<std::string> FileUtils::listAllTypedEntitiesInDir(
        const char* dirpath,
        FileType type,
        bool returnFullPath,
        const char* namePattern) {
    std::vector<std::string> entities;
    struct dirent *dirInfo;
    DIR *dir = opendir(dirpath);
    if (dir == NULL) {
        LOG(ERROR)<< "Failed to read the directory \"" << dirpath
                  << "\" (" << errno << "): " << strerror(errno);
        return entities;
    }

    while ((dirInfo = readdir(dir)) != NULL) {
        if ((type == FileType::REGULAR && dirInfo->d_type == DT_REG) ||
            (type == FileType::DIRECTORY && dirInfo->d_type == DT_DIR) ||
            (type == FileType::SYM_LINK && dirInfo->d_type == DT_LNK) ||
            (type == FileType::CHAR_DEV && dirInfo->d_type == DT_CHR) ||
            (type == FileType::BLOCK_DEV && dirInfo->d_type == DT_BLK) ||
            (type == FileType::FIFO && dirInfo->d_type == DT_FIFO) ||
            (type == FileType::SOCKET && dirInfo->d_type == DT_SOCK)) {
            if (!strcmp(dirInfo->d_name, ".") || !strcmp(dirInfo->d_name, "..")) {
                // Skip the "." and ".."
                continue;
            }
            if (namePattern &&
                fnmatch(namePattern, dirInfo->d_name, FNM_FILE_NAME | FNM_PERIOD)) {
                // Mismatched
                continue;
            }

            // We found one entity
            entities.push_back(
                returnFullPath ? joinPath(dirpath, std::string(dirInfo->d_name))
                               : std::string(dirInfo->d_name));
        }
    }
    closedir(dir);

    return std::move(entities);
}


std::vector<std::string> FileUtils::listAllFilesInDir(
        const char* dirpath,
        bool returnFullPath,
        const char* namePattern) {
    return listAllTypedEntitiesInDir(dirpath,
                                     FileType::REGULAR,
                                     returnFullPath,
                                     namePattern);
}


std::vector<std::string> FileUtils::listAllDirsInDir(
        const char* dirpath,
        bool returnFullPath,
        const char* namePattern) {
    return listAllTypedEntitiesInDir(dirpath,
                                     FileType::DIRECTORY,
                                     returnFullPath,
                                     namePattern);
}

}  // namespace fs
}  // namespace vesoft

