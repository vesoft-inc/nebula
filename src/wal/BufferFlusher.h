/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef WAL_BUFFERFLUSHER_H_
#define WAL_BUFFERFLUSHER_H_

#include "base/Base.h"
#include "thread/NamedThread.h"
#include "wal/InMemoryLogBuffer.h"

namespace nebula {
namespace wal {

class FileBasedWal;

class BufferFlusher final {
public:
    BufferFlusher();
    ~BufferFlusher();

    bool flushBuffer(std::shared_ptr<FileBasedWal> wal, BufferPtr buffer);

private:
    void flushLoop();

private:
    bool stopped_{false};

    std::queue<
        std::pair<std::shared_ptr<FileBasedWal>, BufferPtr>
    > buffers_;
    std::mutex buffersLock_;
    std::condition_variable bufferReadyCV_;

    thread::NamedThread flushThread_;
};

}  // namespace wal
}  // namespace nebula

#endif  // WAL_BUFFERFLUSHER_H_

