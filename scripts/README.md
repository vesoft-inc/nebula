# Overview

This document handles **Nebula Graph** installation, and all the operations are done on single hosts.

Firstly, you need to create the config files for the three daemons in the `etc` directory based on their `*.default` counterparts.

You could operate on all of the three daemons with `scripts/nebula.service`:

```shell
$ scripts/nebula.service start all
...
$ scripts/nebula.service status all
...
$ scripts/nebula.service stop all
...
$ scripts/nebula.service restart all
...
```

Or, you could do some of the same action on a specific daemon:

```shell
$ scripts/nebula.service start metad # Or, scripts/nebula-metad.service start
...
$ scripts/nebula.service status graphd # Or, scripts/nebula-graphd.service status
...
$ scripts/nebula.service restart storaged # Or, scripts/nebula-storaged.service restart
...
```
