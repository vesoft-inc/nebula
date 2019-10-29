Nebula使用`gflags`进行运行时配置。

### 显示变量
```
SHOW VARIABLES graph|meta|storage
```

例如

```
nebula> SHOW VARIABLES meta
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


### 获取变量
```
GET VARIABLES [graph|meta|storage :] var
```

例如
```
nebula> GET VARIABLES storage:load_data_interval_secs
=================================================================
| module  | name                      | type  | mode    | value |
=================================================================
| STORAGE | load_data_interval_secs   | INT64 | MUTABLE | 120   |
-----------------------------------------------------------------
```

```
nebula> GET VARIABLES load_data_interval_secs
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

### 更新变量
```
UPDATE VARIABLES [graph|meta|storage :] var = value
```
> 更新的变量将永久存储于meta-service中。
> 如果变量模式为`MUTABLE`，更改会即时生效。如果模式为`REBOOT`，更改在服务器重启后生效。

例如
```
nebula> UPDATE VARIABLES storage:load_data_interval_secs=1
Execution succeeded (Time spent: 1750/2484 us)
nebula> GET VARIABLES storage:load_data_interval_secs
===============================================================
| module  | name                    | type  | mode    | value |
===============================================================
| STORAGE | load_data_interval_secs | INT64 | MUTABLE | 1     |
---------------------------------------------------------------
Got 1 rows (Time spent: 1678/3420 us)
```
