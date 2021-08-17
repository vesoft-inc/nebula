# Summary

Proposal the `Edge Computation` component to complete the push-down computation. The main principle of it is to push down a part of plan to storage to execute.

# Motivation

1. Support more complex push-down computation
2. Simplify the push-down interface
3. Unify the behavior of graph computation and push-down computation by reuse the computation executor

# Usage explanation

Will expose the interface in storage RPC services.
```
enum ExecutorKind {
     kLimit,
     kFilter,
     kAggregate,
     ...
}

// Or we could reuse the origin Limit Plan Node
// and implement Serialize/Deserialize interface of fbthrift for it
struct LimitExecutor {
     i64 offset,
     i64 limit,
}

// More executor...

struct Executor {
     ExecutorKind kind,
     Optional LimitExecutor,
     Optional FilterExecutor,
     Optional AggregateExecutor,
     ...
}

struct EdgeComputer {
     // The executor depends on previous executor
     // And get output of previous
     list<Executor>
}

struct GetNeighborsRequest {
     // Origin fields ...
     EdgeComputer edge_computer,
}
```

# Design explanation

In storage side:

The `Edge Computation` will append to the origin plan in storage, e.g. Append push-down plan into `GetNeighbors` origin plan. So storage scan data firstly, then complete the push-down computation and response data finally.

And storage should support all expression evaluation except the variable related expression.

In graph side:

The push-down is still handled in optimizer. But now fill the executor to request instead of fill the field of request.

# Rationale and alternatives


# Drawbacks

Will be more flexible and complex for push-down computation.

# Prior art


# Unresolved questions


# Future possibilities

Maybe we could unify the Graph Computation and Edge Computation by reuse the computation executors. But now maybe this need too much refactor, so don't require it in this proposal.
