/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef PARSER_LOADSENTENCES_H_
#define PARSER_LOADSENTENCES_H_

#include "base/Base.h"
#include "parser/Sentence.h"

namespace nebula {

enum class LoadKind : uint32_t {
    kUnknown,
    kLoadVertex,
    kLoadEdge,
};

class LoadSentence : public Sentence {
public:
    explicit LoadSentence(LoadKind lkind) {
        kind_ = Kind::kLoad;
        loadKind_ = std::move(lkind);
    }

    std::string toString() const override;

    LoadKind loadKind() const {
        return loadKind_;
    }

    std::string* path() const {
        return path_.get();
    }

    void setPath(std::string *path) {
       path_.reset(path);
    }

private:
    LoadKind loadKind_{LoadKind::kUnknown};
    std::unique_ptr<std::string> path_;
};

inline std::ostream& operator<<(std::ostream &os, LoadKind kind) {
    return os << static_cast<uint32_t>(kind);
}

}  // namespace nebula
#endif  // PARSER_LOADSENTENCES_H_
