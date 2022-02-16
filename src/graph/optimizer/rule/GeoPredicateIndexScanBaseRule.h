/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_GEOPREDICATEINDEXSCANBASERULE_H
#define GRAPH_OPTIMIZER_RULE_GEOPREDICATEINDEXSCANBASERULE_H

#include "common/base/StatusOr.h"     // for StatusOr
#include "graph/optimizer/OptRule.h"  // for MatchedResult (ptr only), OptRule

namespace nebula {
namespace opt {
class OptContext;

class OptContext;

class GeoPredicateIndexScanBaseRule : public OptRule {
 public:
  bool match(OptContext *ctx, const MatchedResult &matched) const override;
  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;
};

}  // namespace opt
}  // namespace nebula
#endif
