# 日志

Nebula 使用 [glog](https://github.com/google/glog) 来打印日志，使用 gflag 来控制日志的级别，并提供 HTTP 接口来运行时动态的改变日志级别，以方便追踪问题。

## 参数说明

### glog 中的两个主要参数

- minloglevel 0-4： 对应的日志级别分别为 INFO(DEBUG), WARING, ERROR, FATAL。通常在调试环境设置为 0，生产环境设置为1。
- v 0-4: 当 minloglevel 设置为 0 时，可以进一步设置调试日志的详细程度，值越大越详细。

### 配置文件

配置文件中(通常在 `/usr/local/nebula/etc/` 下)可以找到 metad, graphd, storaged 的日志默认配置级别。

## 动态查看和修改日志级别

可以通过如下命令来查看当前的所有的 gflags 参数（包括日志参数）

```sh
> curl ${ws_ip}:${ws_port}/get_flags
```

其中，
- `ws_ip` 为 HTTP 服务的 ip，可以在上述配置文件中找到（默认为127.0.0.1）。
- `ws_port` 为 HTTP 服务的 port。`metad` 默认为 11000， `storaged` 默认为 12000，`graphd` 默认为 13000。

例如，查看 storaged 的 minloglevel 级别：

```sh
> curl 127.0.0.1:12000/get_flags | grep minloglevel
```

也可以通过如下命令将日志级别更改为**最详细**

```sh
> curl "http://127.0.0.1:12000/set_flags?flag=v&value=4"
> curl "http://127.0.0.1:12000/set_flags?flag=minloglevel&value=0"
```

或者**关闭** 所有的日志打印(仅保留 FATAL)

```sh
> curl "http://127.0.0.1:12000/set_flags?flag=minloglevel&value=4"
```
