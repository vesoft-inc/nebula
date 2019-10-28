# Create Space Syntax

```
CREATE SPACE space_name
    (space_option,...)

space_option:
    option_name = value
```

This statement creates a new space. SPACE is a region that provides physically isolation of graphs in Nebula.

### Space Name

* **space_name**

    The name uniquely identifies the space in a cluster. The rules for the naming are given in [Schema Object Names](../../3.language-structure/schema-object-names.md)

### Space Options

When creating a space, options can be given. An _option_name_ can be any of the following:
* _partition_num_

    _partition_num_ specifies the number of partitions in one replica. The default value is 1024. 

* _replica_factor_

    _replica_factor_ specifies the number of replicas in the cluster. The default replica factor is 1.

However, if no option is given, Nebula Graph will create the space with the default partition number and replica factor.

### Example

```
CREATE SPACE my_space_1; -- create space with default partion number and replica factor
CREATE SPACE my_space_2(partition_num=10); -- create space with default replica factor
CREATE SPACE my_space_3(replica_factor=1); -- create space with default partion number
CREATE SPACE my_space_4(partition_num=10, replica_factor=1);
```

