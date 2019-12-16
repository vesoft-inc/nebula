# Create Space Syntax

```ngql
CREATE SPACE [IF NOT EXISTS] <space_name>
   [(partition_num = <part_num>, replica_factor = <raft_copy>)]
```

This statement creates a new space with the given name. SPACE is a region that provides physically isolated graphs in **Nebula Graph**. An error occurs if the database exists.

## IF NOT EXISTS

You can use the `If NOT EXISTS` keywords when creating spaces. This keyword automatically detects if the corresponding space exists. If it does not exist, a new one is created. Otherwise, no space is created.

**Note:** The space existence detection here only compares the space name (excluding properties).

## Space Name

* **space_name**

    The name uniquely identifies the space in a cluster. The rules for the naming are given in [Schema Object Names](../../3.language-structure/schema-object-names.md)

## Customized Space Options

When creating a space, the following two customized options can be given:

* _partition_num_

    _partition_num_ specifies the number of partitions in one replica. The default value is 100.

* _replica_factor_

    _replica_factor_ specifies the number of replicas in the cluster. The default replica factor is 1. The suggested number is 3 in cluster.

However, if no option is given, **Nebula Graph** will create the space with the default partition number and replica factor.

## Example

```ngql
nebula> CREATE SPACE my_space_1; -- create space with default partition number and replica factor
nebula> CREATE SPACE my_space_2(partition_num=10); -- create space with default replica factor
nebula> CREATE SPACE my_space_3(replica_factor=1); -- create space with default partition number
nebula> CREATE SPACE my_space_4(partition_num=10, replica_factor=1);
```
