There are only two ways to compose statements (or subqueries):
* More than one statements can be batched together, separated by semicolon (;). The result of the last statement will be returned as the result of the batch.
* Statements could be piped together using operator (|), much like the pipe in the shell scripts. The result yielded from the previous statement could be redirected to the next statement as input.

> Notice that compose statements are not `Transactional` queries. 
> For example, a statement composed of three subqueries: A | B | C, where A is a read operation, B is a computation, and C is a write operation.
> If any part fails in the execution, the whole result could be undefined -- currently, there is no so call roll back -- what was written dependents on the query optimizer.

### Examples

* semicolon statements

```
SHOW TAGS; SHOW EDGES;          -- only edges are shown

INSERT VERTEX player(name, age) VALUES 100:("Tim Duncan", 42); \ 
INSERT VERTEX player(name, age) VALUES 101:("Tony Parker", 36); \
INSERT VERTEX player(name, age) VALUES 102:("LaMarcus Aldridge", 33);  /* multiple vertices are added in a compose statement. */

```

* PIPE statements

```
GO FROM 201 OVER edge_serve | GO FROM $-.id OVER edge_fans | GO FROM $-.id ...
```

Placeholder `$-.id` takes the result from the first statment `GO FROM 201 OVER edge_serve`