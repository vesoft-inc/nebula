nebula_add_library(
    planner_obj OBJECT
    PlanNode.cpp
    ExecutionPlan.cpp
    Query.cpp
    IdGenerator.cpp
)

add_subdirectory(test)
