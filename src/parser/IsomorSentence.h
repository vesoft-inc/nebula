/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef PARSER_ISOMORSENTENCE_H_
#define PARSER_ISOMORSENTENCE_H_

#include "common/expression/ContainerExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "parser/MatchPath.h"
#include "parser/Sentence.h"


namespace nebula {

class IsomorSentence final : public Sentence {
  public:
    explicit IsomorSentence(NameLabelList* tags) {
      tags_.reset(tags);
    }
    const NameLabelList* tags() const {
      return tags_->empty() ? nullptr : tags_.get();
    }
    std::string toString() const override;
  private:
    std::unique_ptr<NameLabelList> tags_;
};
}  // namespace nebula

#endif  // PARSER_ISOMORSENTENCE_H_
