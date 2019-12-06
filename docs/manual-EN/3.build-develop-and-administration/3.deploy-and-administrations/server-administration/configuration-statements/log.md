# Logs

**Nebula Graph** uses [glog](https://github.com/google/glog) to print logs, gflag to control the severity level of the log, and provides an HTTP interface to dynamically change the log level at runtime to facilitate tracking.

## Parameter Description

### Two most commonly used flags in glog

- minloglevel 0-3: the numbers of severity levels INFO(DEBUG), WARNING, ERROR, and FATAL are 0, 1, 2, and 3, respectively. Usually specified as 0 in debug, 1 in production. If you set the minloglevel to 4, no logs are printed.
- v 0-4: when minloglevel is set to 0,  you can further set the severity level of the debug log. The larger the value, the more detailed the log.

### Configuration Files

The default severity level for the metad, graphd, storaged logs can be found in the configuration files (usually under `/usr/local/nebula/etc/`).

## Check and Change Severity Levels Dynamically

Check all the flag values (log values included) of the current glags with the following command.

```bash
> curl ${ws_ip}:${ws_port}/get_flags
```

Parameters:

- `ws_ip` is the HTTP service IP, which can be found in the above configuration files (default IP is 127.0.0.1)
- `ws_port` is the HTTP port, the default value for `metad` is 11000, `storaged` is 12000 and `graphd` is 13000

For example, check the severity minloglevel of storaged:

```bash
> curl 127.0.0.1:12000/get_flags | grep minloglevel
```

Change the logs severity level to **most detailed** with the following command.

```bash
> curl "http://127.0.0.1:12000/set_flags?flag=v&value=4"
> curl "http://127.0.0.1:12000/set_flags?flag=minloglevel&value=0"
```

In **Nebula Graph** console, check the severity minloglevel of graphd and set it to **most detailed** with the following commands.

```ngql
nebula> GET CONFIGS graph:minloglevel
nebula> UPDATE CONFIGS graph:minloglevel=0
```

To change the severity of the storage log, replace `graph` in the above command with `storage`. Note that Nebula Graph only supports modifying the graph and storage log severity via console, meta log cannot be changed.

Or **close** all logs print (FATAL only).

```bash
> curl "http://127.0.0.1:12000/set_flags?flag=minloglevel&value=3"
```
