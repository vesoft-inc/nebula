# Variables Syntax

Nebula use `gflags` for run-time configurations.

## Show variables

```sql
SHOW VARIABLE [graph|meta|storage]
```

For example

```sql
nebula> SHOW VARIABLE meta
============================================================================================================================
| module | name                                        | type   | mode      | value                                        |
============================================================================================================================
| META   | v                                           | INT64  | IMMUTABLE | 4                                            |
----------------------------------------------------------------------------------------------------------------------------
| META   | help                                        | BOOL   | IMMUTABLE | False                                        |
----------------------------------------------------------------------------------------------------------------------------
| META   | port                                        | INT64  | IMMUTABLE | 45500                                        |
----------------------------------------------------------------------------------------------------------------------------
```

## Get variables

```sql
GET VARIABLE [graph|meta|storage :] var
```

For example

```sql
nebula> GET VARIABLE storage:load_data_interval_secs
=================================================================
| module  | name                      | type  | mode    | value |
=================================================================
| STORAGE | load_data_interval_secs   | INT64 | MUTABLE | 120   |
-----------------------------------------------------------------
```

```sql
nebula> GET VARIABLE load_data_interval_secs
=================================================================
| module  | name                    | type  | mode      | value |
=================================================================
| GRAPH   | load_data_interval_secs | INT64 | MUTABLE   | 120   |
-----------------------------------------------------------------
| META    | load_data_interval_secs | INT64 | IMMUTABLE | 120   |
-----------------------------------------------------------------
| STORAGE | load_data_interval_secs | INT64 | MUTABLE   | 120   |
-----------------------------------------------------------------
Got 3 rows (Time spent: 1449/2339 us)
```

## Update variables

```sql
UPDATE VARIABLE [graph|meta|storage :] var = value
```

> The updated variables will be stored into meta-service permanently.
> If the variable's mode is `MUTABLE`, the change will take effects immediately. Otherwise, if the mode is `REBOOT`, the change will not work until server restart.

For example

```sql
nebula> UPDATE VARIABLE storage:load_data_interval_secs=1
Execution succeeded (Time spent: 1750/2484 us)
nebula> GET VARIABLE storage:load_data_interval_secs
===============================================================
| module  | name                    | type  | mode    | value |
===============================================================
| STORAGE | load_data_interval_secs | INT64 | MUTABLE | 1     |
---------------------------------------------------------------
Got 1 rows (Time spent: 1678/3420 us)
```
