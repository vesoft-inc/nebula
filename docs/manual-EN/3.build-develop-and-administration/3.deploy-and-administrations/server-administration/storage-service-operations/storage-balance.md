# Storage Balance Usage

Nebula's services are composed of  three parts: graphd, storaged and metad. The **balance** in this document focuses on the operation of storage. 

Currently, storage can be scaled horizontally by the command `balance`. There are two kinds of balance command, one is to move data, which is `balance data`; the other one only changes the distribution of leader partition to balance load without moving data, which is `balance leader`.

## Balance data

Let's use an example to expand the cluster from 3 instances to 8 to show how to balance data.

### Step 1 Prerequisites

Deploy a cluster with three replicas, one graphd, one metad and three storaged. Details on deployment please refer [Cluster Deploy](https://github.com/Amber1990Zhang/nebula/blob/master/docs/get-started/deploy-cluster-on-docker.md). Check cluster status using command `SHOW HOSTS`.

#### Step 1.1

```
(user@127.0.0.1) [(none)]> SHOW HOSTS

================================================================================================
| Ip            | Port  | Status | Leader count | Leader distribution | Partition distribution |
================================================================================================
| 192.168.8.210 | 34600 | online | 0            | No valid partition  | No valid partition     |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34700 | online | 0            | No valid partition  | No valid partition     |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34500 | online | 0            | No valid partition  | No valid partition     |
------------------------------------------------------------------------------------------------
Got 3 rows (Time spent: 5886/6835 us)
```

**_Explanations on the returned results:_**

- **IP** and **Port** is the present storage instance, the cluster starts with three storaged instances (192.168.8.210:34600, 192.168.8.210:34700, 192.168.8.210:34500) without any data. 
- **Status** shows the state of the present instance, currently there are two kind of states, i.e. online/offline. When a machine is out of service, metad will turn it to offline if no heart beat received for certain time threshold. The default threshold is 10 minutes and can be changed in parameter `expired_threshold_sec` when starting metad service.
- **Leader count** shows RAFT leader number of the present process.
- **Leader distribution** shows how the present leader is distributed in each graph space. No space is created for now.
- **Partition distribution** shows how many partitions are served by each host.

#### Step 1.2

Create a graph space named **test** with 100 partition and 3 replicas.

```
(user@127.0.0.1) [(none)]> CREATE SPACE test(PARTITION_NUM=100, REPLICA_FACTOR=3)

Execution succeeded (Time spent: 2770/3777 us)
```

Get the new partition distribution by the command `SHOW HOSTS`.

```
(user@127.0.0.1) [(none)]> SHOW HOSTS

================================================================================================
| Ip            | Port  | Status | Leader count | Leader distribution | Partition distribution |
================================================================================================
| 192.168.8.210 | 34600 | online | 0            | test: 0             | test: 100              |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34700 | online | 52           | test: 52            | test: 100              |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34500 | online | 48           | test: 48            | test: 100              |
------------------------------------------------------------------------------------------------
Got 3 rows (Time spent: 6029/7175 us)
```

### Step 2 Add new hosts into the cluster

Now, add some new hosts (storaged instances) into the cluster.

Again, show the new status using command `SHOW HOSTS`. You can see there are already eight hosts in serving. But no partition is running on the new hosts.

```
(user@127.0.0.1) [(none)]> SHOW HOSTS

================================================================================================
| Ip            | Port  | Status | Leader count | Leader distribution | Partition distribution |
================================================================================================
| 192.168.8.210 | 34600 | online | 0            | test: 0             | test: 100              |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34900 | online | 0            | No valid partition  | No valid partition     |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 35940 | online | 0            | No valid partition  | No valid partition     |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34920 | online | 0            | No valid partition  | No valid partition     |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 44920 | online | 0            | No valid partition  | No valid partition     |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34700 | online | 52           | test: 52            | test: 100              |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34500 | online | 48           | test: 48            | test: 100              |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34800 | online | 0            | No valid partition  | No valid partition     |
------------------------------------------------------------------------------------------------
```

### Step 3 Data migration

Check the current balance plan id using command `balance data` if the cluster is balanced. Otherwise, a new plan id will be generated by the command.

```
(user@127.0.0.1) [(none)]> balance data

==============
| ID         |
==============
| 1570761786 |
--------------
Got 1 rows (Time spent: 14378/15410 us)
```

Check the progress of balance using command `balance data $id`.

```
(user@127.0.0.1) [(none)]> balance data 1570761786

===============================================================================
| balanceId, spaceId:partId, src->dst                           | status      |
===============================================================================
| [1570761786, 1:1, 192.168.8.210:34600->192.168.8.210:44920]   | succeeded   |
-------------------------------------------------------------------------------
| [1570761786, 1:1, 192.168.8.210:34700->192.168.8.210:34920]   | succeeded   |
-------------------------------------------------------------------------------
| [1570761786, 1:1, 192.168.8.210:34500->192.168.8.210:34800]   | succeeded   |
-------------------------------------------------------------------------------
| [1570761786, 1:2, 192.168.8.210:34600->192.168.8.210:44920]   | in progress |
-------------------------------------------------------------------------------
| [1570761786, 1:2, 192.168.8.210:34700->192.168.8.210:34920]   | in progress |
-------------------------------------------------------------------------------
| [1570761786, 1:2, 192.168.8.210:34500->192.168.8.210:34800]   | in progress |
-------------------------------------------------------------------------------
| [1570761786, 1:3, 192.168.8.210:34600->192.168.8.210:44920]   | succeeded   |
-------------------------------------------------------------------------------
...
| Total:189, Succeeded:170, Failed:0, In Progress:19, Invalid:0 | 89.947090%  |
-------------------------------------------------------------------------------

Got 190 rows (Time spent: 5454/11095 us)
```

**_Explanations on the returned results:_**

- The first column is a specific balance task. Take 1570761786, 1:88, 192.168.8.210:34700->192.168.8.210:35940 for example

  - **1570761786** is the balance id
  - **1:88**, 1 is the spaceId, 88 is the moved partId
  - **192.168.8.210:34700->192.168.8.210:3594**, moving data from 192.168.8.210:34700 to 192.168.8.210:35940

- The second column shows the state (result) of the task, there are four states:
  - Succeeded
  - Failed
  - In progress
  - Invalid

The last row is the summary of the tasks. Some partitions are yet to be migrated.

### Step 4 Migration confirmation

In most cases, data migration will take hours or even days. During the migration, Nebula service are not affected. Once migration is done, the progress will show 100%. You can retry `balance data` to fix a failed task. If it can't be fixed after several attempts, please contact us at [GitHub](https://github.com/vesoft-inc/nebula/issues).

Now, you can check partition distribution using command `SHOW HOSTS` when balance completed.

```
(user@127.0.0.1) [(none)]> SHOW HOSTS

================================================================================================
| Ip            | Port  | Status | Leader count | Leader distribution | Partition distribution |
================================================================================================
| 192.168.8.210 | 34600 | online | 3            | test: 3             | test: 37               |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34900 | online | 0            | test: 0             | test: 38               |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 35940 | online | 0            | test: 0             | test: 37               |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34920 | online | 0            | test: 0             | test: 38               |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 44920 | online | 0            | test: 0             | test: 38               |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34700 | online | 35           | test: 35            | test: 37               |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34500 | online | 24           | test: 24            | test: 37               |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34800 | online | 38           | test: 38            | test: 38               |
------------------------------------------------------------------------------------------------
Got 8 rows (Time spent: 5074/6488 us)
```

Now partitions and data are evenly distributed on the machines.

## Balance leader

Command `balance data` only migrates partitions. But the leader distribution remains unbalanced, which means old hosts are overloaded, while the new ones are not fully used. Redistribute RAFT leader using the command `balance leader`.

```
(user@127.0.0.1) [(none)]> balance leader

Execution succeeded (Time spent: 89672/90581 us)
```

Seconds later, check the results using the command `SHOW HOSTS`. The RAFT leaders are distributed evenly over all the hosts in the cluster.

```
(user@127.0.0.1) [(none)]> SHOW HOSTS

================================================================================================
| Ip            | Port  | Status | Leader count | Leader distribution | Partition distribution |
================================================================================================
| 192.168.8.210 | 34600 | online | 13           | test: 13            | test: 37               |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34900 | online | 12           | test: 12            | test: 38               |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 35940 | online | 12           | test: 12            | test: 37               |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34920 | online | 12           | test: 12            | test: 38               |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 44920 | online | 13           | test: 13            | test: 38               |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34700 | online | 12           | test: 12            | test: 37               |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34500 | online | 13           | test: 13            | test: 37               |
------------------------------------------------------------------------------------------------
| 192.168.8.210 | 34800 | online | 13           | test: 13            | test: 38               |
------------------------------------------------------------------------------------------------
Got 8 rows (Time spent: 5039/6346 us)
```
