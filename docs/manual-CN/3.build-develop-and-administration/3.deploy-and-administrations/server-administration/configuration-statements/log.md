# 日志

**Nebula Graph** 使用 [glog](https://github.com/google/glog) 打印日志，使用 gflag 控制日志级别，并提供 HTTP 接口在运行时动态改变日志级别，以方便追踪问题。

## 参数说明

### glog 中的两个主要参数

- minloglevel 0-3：对应的日志级别分别为 INFO(DEBUG)，WARNING，ERROR，FATAL。通常在调试环境设置为 0，生产环境设置为 1，设置为 4 不打印任何日志。
- v 0-4: 当 minloglevel 设置为 0 时，可以进一步设置调试日志的详细程度，值越大越详细。

### 配置文件

在配置文件中（通常在 `/usr/local/nebula/etc/` 下）可以找到 metad，graphd，storaged 的日志默认配置级别。

## 动态查看和修改日志级别

可以通过如下命令来查看当前的所有的 gflags 参数（包括日志参数）

```bash
> curl ${ws_ip}:${ws_port}/get_flags
```

其中，

- `ws_ip` 为 HTTP 服务的 IP，可以在上述配置文件中找到（默认为 127.0.0.1）。
- `ws_port` 为 HTTP 服务的端口号。`metad` 默认为 11000， `storaged` 默认为 12000，`graphd` 默认为 13000。

例如，查看 storaged 的 minloglevel 级别：

```bash
> curl 127.0.0.1:12000/get_flags | grep minloglevel
```

也可以通过如下命令将日志级别更改为**最详细**。

```bash
> curl "http://127.0.0.1:12000/set_flags?flag=v&value=4"
> curl "http://127.0.0.1:12000/set_flags?flag=minloglevel&value=0"
```

在 console 中，使用如下命令获取当前日志级别并将日志级别设置为**最详细**。

```ngql
nebula> GET CONFIGS graph:minloglevel
nebula> UPDATE CONFIGS graph:minloglevel=0
```

如需更改 storage 日志级别，将上述命令中的 `graph` 更换为 `storage` 即可，注意，**Nebula Graph** 仅支持通过 console 修改 graph 和 storage 日志级别，meta 日志不能更改。

或者**关闭**所有的日志打印(仅保留 FATAL)。

```bash
> curl "http://127.0.0.1:12000/set_flags?flag=minloglevel&value=3"
```
