/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_PLANNER_PLANNERREGISTER_H_
#define GRAPH_PLANNER_PLANNERREGISTER_H_

namespace nebula {
namespace graph {

class PlannersRegister final {
 public:
  PlannersRegister() = delete;
  ~PlannersRegister() = delete;

  static void registPlanners();

 private:
  static void registDDL();
  static void registSequential();
  static void registMatch();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PLANNER_PLANNERREGISTER_H_
