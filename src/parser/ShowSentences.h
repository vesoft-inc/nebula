/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_SHOWSENTENCES_H_
#define PARSER_SHOWSENTENCES_H_

#include "base/Base.h"
#include "parser/Clauses.h"
#include "parser/Sentence.h"

namespace nebula {

enum class ShowKind : uint32_t {
    kUnknown,
    kShowHosts,
};


class ShowSentence final : public Sentence {
public:
    explicit ShowSentence(ShowKind sKind) {
        kind_ = Kind::kShow;
        showKind_ = sKind;
    }

    std::string toString() const override;

    ShowKind showKind() const {
        return showKind_;
    }

private:
    ShowKind                showKind_{ShowKind::kUnknown};
};

inline std::ostream& operator<<(std::ostream &os, ShowKind kind) {
    return os << static_cast<uint32_t>(kind);
}

}   // namespace nebula

#endif  // PARSER_SHOWSENTENCES_H_
