/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_PLANNERREGISTER_H_
#define GRAPH_PLANNER_PLANNERREGISTER_H_

namespace nebula {
namespace graph {

class PlannersRegister final {
 public:
  PlannersRegister() = delete;
  ~PlannersRegister() = delete;

  static void registerPlanners();

 private:
  static void registerDDL();
  static void registerSequential();
  static void registerMatch();
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PLANNER_PLANNERREGISTER_H_
