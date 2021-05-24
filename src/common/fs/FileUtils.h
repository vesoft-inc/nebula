/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_FS_FILEUTILS_H_
#define COMMON_FS_FILEUTILS_H_

#include "common/base/Base.h"
#include <dirent.h>
#include "common/base/StatusOr.h"

namespace nebula {
namespace fs {

enum class FileType {
    UNKNOWN = 0,
    NOTEXIST,
    REGULAR,
    DIRECTORY,
    SYM_LINK,
    CHAR_DEV,
    BLOCK_DEV,
    FIFO,
    SOCKET
};


class FileUtils final {
public:
    FileUtils() = delete;

    // Get the directory part of a path
    static std::string dirname(const char *path);
    // Get the base part of a path
    static std::string basename(const char *path);
    // Get the content of a symbol link
    static StatusOr<std::string> readLink(const char *path);
    // Get the canonicalized absolute pathname of a path
    static StatusOr<std::string> realPath(const char *path);

    // return the size of the given file
    static size_t fileSize(const char* path);
    // return the type of the given file
    static FileType fileType(const char* path);
    // Return the file type name
    static const char* getFileTypeName(FileType type);
    // Return the last update time for the given file (UNIX Epoch time)
    static time_t fileLastUpdateTime(const char* path);

    // Tell if stdin attached to a TTY
    static bool isStdinTTY();
    // Tell if stdout atached to a TTY
    static bool isStdoutTTY();
    // Tell if stderr attached to a TTY
    static bool isStderrTTY();
    // Tell if the given fd attached to a TTY
    static bool isFdTTY(int fd);

    // Concatenate directory name and filename together
    //
    // If dir is empty, the result would be representation of the local file,
    // for example, joinPath("", "foo.bar") will return "./foo.bar"
    static std::string joinPath(const folly::StringPiece dir,
                                const folly::StringPiece filename);
    // Divide the given path into two parts: the parent directory path
    // (without the ending "/") and the filename or the child directory name
    //
    // For example, if the given path is
    //   "/a/b/c/d"
    // The result parent path would be "/a/b/c" and the child would be
    // "d"
    //
    // In the given path, the ending "/" will be ignored. For example, when
    // the given path is "/a/b/c/d/", the result parent path would be "/a/b/c",
    // and the child name would be "d"
    //
    // When the given path is in the root directory, such as "/a", the
    // result parent would be "/" (this is the only case that the parent
    // will end with "/"), and the child is "a"
    //
    // When the given path is the root directory itself, the result parent
    // would be empty, and the child is "/"
    //
    // Parameters: path -- the given path
    //             parent -- return the parent directory path. It ends
    //                       with the directory name without "/", unless
    //                       the parent directory is the root directory
    //             child -- return the filename or the child directory
    //                      name
    static void dividePath(const folly::StringPiece path,
                           folly::StringPiece& parent,
                           folly::StringPiece& child);

    // Remove the given file or directory (with its content)
    //
    // When the given path is a directory and recursively == false,
    // removal will fail if the directory is not empty. If recursive == true,
    // all content will be removed, including the sub-directories and their
    // contents
    //
    // The method returns true if removal succeeded or the given file
    // or directory does not exist.
    static bool remove(const char* path, bool recursively = false);
    // Create the directory
    //
    // It will make the parent directories as needed
    // (much like commandline "mkdir -p")
    static bool makeDir(const std::string& dir, uint32_t mode = 0775);
    // Check the path is exist
    static bool exist(const std::string& path);
    // Like the command `mv', apply to file and directory
    // Refer to `man 3 rename'
    // return false when rename failed
    static bool rename(const std::string& src, const std::string& dst);

    /**
     * List all entities in the given directory, whose type matches
     * the given file type
     *
     * The function returns either fullpath or just the filename
     *
     * You can also pass in a wildcard pattern, which makes the function
     * to only return entities whose names match the pattern
     */
    static std::vector<std::string> listAllTypedEntitiesInDir(
        const char* dirpath,
        FileType type,
        bool returnFullPath,
        const char* namePattern);
    /**
     * List all files in the given directory
     *
     * Internally, it calls listAllTypedEntitiesInDir()
     */
    static std::vector<std::string> listAllFilesInDir(
        const char* dirpath,
        bool returnFullPath = false,
        const char* namePattern = nullptr);
    /**
     * List all sub-directories in the given directory
     *
     * Internally, it calls listAllTypedEntitiesInDir()
     */
    static std::vector<std::string> listAllDirsInDir(
        const char* dirpath,
        bool returnFullPath = false,
        const char* namePattern = nullptr);

    static bool isReg(struct dirent* dEnt, const char* path);
    static bool isDir(struct dirent* dEnt, const char* path);
    static bool isLink(struct dirent* dEnt, const char* path);
    static bool isChr(struct dirent* dEnt, const char* path);
    static bool isBlk(struct dirent* dEnt, const char* path);
    static bool isFifo(struct dirent* dEnt, const char* path);
    static bool isSock(struct dirent* dEnt, const char* path);

    // free space in bytes
    static StatusOr<uint64_t> free(const char* path);

    // available space in bytes
    static StatusOr<uint64_t> available(const char* path);

    /**
     * class Iterator works like other iterators,
     * which iterates over lines in a file or entries in a directory.
     * Additionally, if offered a pattern, Iterator filters out lines or entry names
     * that matches the pattern.
     *
     * NOTE Iterator is not designed to be used in the performance critical situations.
     */
    class Iterator;
    using DirEntryIterator = Iterator;
    using FileLineIterator = Iterator;
    class Iterator final {
    public:
        /**
         * @path    path to a regular file or directory
         * @pattern optional regex pattern
         */
        explicit Iterator(std::string path, const std::regex *pattern = nullptr);
        ~Iterator();

        // Whether this iterator is valid
        bool valid() const {
            return status_.ok();
        }

        // Step to the next line or entry
        void next();
        // Step to the next line or entry
        // Overload the prefix-increment operator
        Iterator& operator++() {
            next();
            return *this;
        }

        // Forbid the overload of postfix-increment operator
        Iterator operator++(int) = delete;

        // Line or directory entry
        // REQUIRES:    valid() == true
        std::string& entry() {
            CHECK(valid());
            return entry_;
        }

        // Line or directory entry
        // REQUIRES:    valid() == true
        const std::string& entry() const {
            CHECK(valid());
            return entry_;
        }

        // The matched result of the pattern
        // REQUIRES:    valid() == true && pattern != nullptr
        std::smatch& matched() {
            CHECK(valid());
            CHECK(pattern_ != nullptr);
            return matched_;
        }

        // The matched result of the pattern
        // REQUIRES:    valid() == true && pattern != nullptr
        const std::smatch& matched() const {
            CHECK(valid());
            CHECK(pattern_ != nullptr);
            return matched_;
        }

        // Status to indicates the error
        const Status& status() const {
            return status_;
        }

    private:
        void openFileOrDirectory();
        void dirNext();
        void fileNext();

    private:
        std::string                         path_;
        FileType                            type_{FileType::UNKNOWN};
        std::unique_ptr<std::ifstream>      fstream_;
        DIR                                *dir_{nullptr};
        const std::regex                   *pattern_{nullptr};
        std::string                         entry_;
        std::smatch                         matched_;
        Status                              status_;
    };
};

}  // namespace fs
}  // namespace nebula

#define CHECK_TYPE(NAME, FTYPE, DTYPE) \
    bool FileUtils::is ## NAME(struct dirent *dEnt, const char* path) { \
        if (dEnt->d_type == DT_UNKNOWN) { \
            return FileUtils::fileType(FileUtils::joinPath(path, dEnt->d_name).c_str()) == \
                   FileType::FTYPE; \
        } else { \
            return dEnt->d_type == DT_ ## DTYPE; \
        } \
    }

#endif  // COMMON_FS_FILEUTILS_H_
