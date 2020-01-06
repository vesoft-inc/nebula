# Job Manager

The job here refers to the two commands `compact` and `flush` running at the storage layer. The manager means to manage the jobs. For example, you can run, show, stop and recover jobs.

## Statements List

### admin compact / flush

If the `admin flush` statement runs successfully, the job ID is returned. The example is as follows:

```ngql
==============
| New Job Id |
==============
| 40         |
--------------
```

### SHOW JOB

#### List Single Job Information

The `SHOW JOB <job_id>` statement lists the information of the given job.

```ngql
nebula> SHOW JOB 40
=====================================================================================
| Job Id(TaskId) | Command(Dest) | Status   | Start Time        | Stop Time         |
=====================================================================================
| 40             | flush nba     | finished | 12/17/19 17:21:30 | 12/17/19 17:21:30 |
-------------------------------------------------------------------------------------
| 40-0           | 192.168.8.5   | finished | 12/17/19 17:21:30 | 12/17/19 17:21:30 |
-------------------------------------------------------------------------------------
```

The above statement returns one to multiple rows, which is determined by the storage number where the space is located. The `flush nba` is the job's logical information. The `192.168.8.5` is the node IP that the job is running at.

#### List All Jobs

The `SHOW JOBS` statement lists all the jobs (all the jobs that are not backuped, please refer to the next section on how to backup job).

```ngql
nebula> SHOW JOBS
=============================================================================
| Job Id | Command       | Status   | Start Time        | Stop Time         |
=============================================================================
| 22     | flush test2   | failed   | 12/06/19 14:46:22 | 12/06/19 14:46:22 |
-----------------------------------------------------------------------------
| 23     | compact test2 | stopped  | 12/06/19 15:07:09 | 12/06/19 15:07:33 |
-----------------------------------------------------------------------------
| 24     | compact test2 | stopped  | 12/06/19 15:07:11 | 12/06/19 15:07:20 |
-----------------------------------------------------------------------------
| 25     | compact test2 | stopped  | 12/06/19 15:07:13 | 12/06/19 15:07:24 |
-----------------------------------------------------------------------------
```

### BACKUP JOB

The `BACKUP JOB <from_id> <to_id>`  statement archives a job when there are too many jobs. The archived jobs are no longer visible in the console. If you show an archived job, an error is thrown.

```ngql
nebula> BACKUP JOB 0 22
=========================
| BACKUP Result         |
=========================
| backup 1 jobs 2 tasks |
-------------------------
```

### RECOVER JOB

The `RECOVER JOB <job_id>` statement reruns the given job and returns a new job ID.

```ngql
nebula> RECOVER JOB 40
==============
| New Job Id |
==============
| 41         |
--------------
```
