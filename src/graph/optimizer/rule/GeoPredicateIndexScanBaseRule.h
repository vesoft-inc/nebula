/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_GEOPREDICATEINDEXSCANBASERULE_H
#define GRAPH_OPTIMIZER_RULE_GEOPREDICATEINDEXSCANBASERULE_H

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

class GeoPredicateIndexScanBaseRule : public OptRule {
 public:
  bool match(OptContext *ctx, const MatchedResult &matched) const override;
  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;
};

}  // namespace opt
}  // namespace nebula
#endif
