# Job Manager

The job here refers to the long tasks running at the storage layer. For example, `compact` and `flush`. The manager means to manage the jobs. For example, you can run, show, stop and recover jobs.

## Statements List

### submit job compact / flush

The `submit job compact/flush` statement creates a new job and returns the job ID in the job manager, and executes the `compact/flush` command in the storage. The example is as follows:

```ngql
nebula> SUBMIT JOB COMPACT;
==============
| New Job Id |
==============
| 40         |
--------------

nebula> SUBMIT JOB FLUSH;
==============
| New Job Id |
==============
| 2          |
--------------
```

### SHOW JOB

#### List Single Job Information

The `SHOW JOB <job_id>` statement shows a job with certain ID and all its tasks. After a job arrives to Meta, Meta will split the job to tasks, and send them to storage.

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

The above statement returns one to multiple rows, which is determined by the storage number where the space is located.

What's in the returned results:

- `40` is the job ID
- `flush nba` indicates that a flush operation is performed on space nba
- `finished` is the job status, which indicates that the job execution is finished and successful. Other job status are Queue, running, failed and stopped
- `12/17/19 17:21:30` is the start time, which is initially empty(Queue). The value is set if and only if the job status is running.
- `12/17/19 17:21:30` is the stop time, which is empty when the job status is Queue or running. The value is set when the job status is finished, failed and stopped
- `40-0` indicated that the job ID is 40, the task ID is 0
- `192.168.8.5` shows which machine the job is running on
- `finished` is the job status, which indicates that the job execution is finished and successful. Other job status are Queue, running, failed and stopped
- `12/17/19 17:21:30` is the start time, which can never be empty because the initial status is running
- `12/17/19 17:21:30` is the end time, which is empty when the job status is running. The value is set when the job status is finished, failed and stopped

**Note:** There are five job status, i.e. QUEUE, RUNNING, FINISHED, FAILED, STOPPED. Status switching is described below:

```ngql
Queue -- running -- finished -- removed
     \          \                /
      \          \ -- failed -- /
       \          \            /
        \ ---------- stopped -/
```

#### List All Jobs

The `SHOW JOBS` statement lists all the jobs that are not expired. The default job expiration time is one week. You can change it with meta flag `job_expired_secs`.

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

For details on the returned results, please refer to the previous section [List Single Job Information](#list-single-job-information).

### STOP JOB

The `STOP JOB` statement stops jobs that are not finished.

```ngql
nebula> STOP JOB 22
=========================
| STOP Result         |
=========================
| stop 1 jobs 2 tasks |
-------------------------
```

### RECOVER JOB

The `RECOVER JOB` statement re-executes the failed jobs and returns the number of the recovered jobs.

```ngql
nebula> RECOVER JOB
=====================
| Recovered job num |
=====================
| 5 job recovered   |
---------------------
```
