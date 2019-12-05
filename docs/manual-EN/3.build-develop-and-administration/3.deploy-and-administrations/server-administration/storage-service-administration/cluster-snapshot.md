# Cluster Snapshot

## Create Snapshot

The `CREATE SNAPSHOT` command creates a snapshot at the current point in time for the whole cluster. The snapshot name is composed of the timestamp of the meta server. If snapshot creation fails in the current version, you must use the `DROP SNAPSHOT` to clear the invalid snapshots.

The current version does not support creating snapshot for the specified graph spaces, and executing `CREATE SNAPSHOT` creates a snapshot for all graph spaces in the cluster. For example:

```ngql
nebula> CREATE SNAPSHOT;
Execution succeeded (Time spent: 22892/23923 us)
```

## Show Snapshots

The command `SHOW SNAPSHOT` looks at the states (VALID or INVALID), names and the IP addresses of all storage servers when the snapshots are created in the cluster. For example:

```ngql
nebula> SHOW SNAPSHOTS;
===========================================================
| Name                         | Status | Hosts           |
===========================================================
| SNAPSHOT_2019_12_04_10_54_36 | VALID  | 127.0.0.1:77833 |
-----------------------------------------------------------
| SNAPSHOT_2019_12_04_10_54_42 | VALID  | 127.0.0.1:77833 |
-----------------------------------------------------------
| SNAPSHOT_2019_12_04_10_54_44 | VALID  | 127.0.0.1:77833 |
-----------------------------------------------------------
```

## Delete Snapshot

The `DROP SNAPSHOT` command deletes a snapshot with the specified name, the syntax is:

```ngql
DROP SNAPSHOT <snapshot-name>
```

You can get the snapshot names with the command `SHOW SNAPSHOTS`. `DROP SNAPSHOT` can delete both valid snapshots and invalid snapshots that failed during creation. For example:

```ngql
nebula> DROP SNAPSHOT SNAPSHOT_2019_12_04_10_54_36;
nebula> SHOW SNAPSHOTS;
===========================================================
| Name                         | Status | Hosts           |
===========================================================
| SNAPSHOT_2019_12_04_10_54_42 | VALID  | 127.0.0.1:77833 |
-----------------------------------------------------------
| SNAPSHOT_2019_12_04_10_54_44 | VALID  | 127.0.0.1:77833 |
-----------------------------------------------------------
```

Now the deletes snapshot is not in the show snapshots list.

## Tips

- When the system structure changes, it is better to create a snapshot immediately. For example, when you add host, drop host, create space, drop space or balance.
- The current version does not support automatic garbage collection for the failed snapshots in creation. We will develop cluster checker in meta server to check the cluster state via asynchronous threads and automatically collect the garbage files in failure snapshot creation.
- The current version does not support customized snapshot directory. The snapshots are created in the `data_path/nebula` directory by default.
- The current version does not support snapshot restore. Users need to write a shell script based on their actual productions to restore snapshots. The implementation logic is rather simple, you copy the snapshots of the engine servers to the specified folder, set this folder to `data_path/`, then start the cluster.
