Nebula use `gflags` for run-time configurations.


### Show variables
```
SHOW VARIABLES [graph|meta|storage]
```

For example

```
(user@127.0.0.1) [(none)]> SHOW VARIABLES meta
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


### Get variables
```
GET VARIABLES [graph|meta|storage : var]
```

For example
```
(user@127.0.0.1) [(none)]> GET VARIABLES storage:load_config_interval_secs
=================================================================
| module  | name                      | type  | mode    | value |
=================================================================
| STORAGE | load_config_interval_secs | INT64 | MUTABLE | 120   |
-----------------------------------------------------------------
```

### Update variables
```
UPDATE VARIABLES [graph|meta|storage : var = value]
```
> The updated variables will be stored into meta-service permanently.
>  If the variable's mode is `MUTABLE`, the change will take affects immediately. Otherwise, if the mode is `REBOOT`, the change will not work until a reboot. 

For example
```
(user@127.0.0.1) [(none)]> UPDATE VARIABLES storage:load_config_interval_secs=1
Execution succeeded (Time spent: 1750/2484 us)
(user@127.0.0.1) [(none)]> GET VARIABLES storage:load_config_interval_secs
=================================================================
| module  | name                      | type  | mode    | value |
=================================================================
| STORAGE | load_config_interval_secs | INT64 | MUTABLE | 1     |
-----------------------------------------------------------------
Got 1 rows (Time spent: 1714/2663 us)    
```