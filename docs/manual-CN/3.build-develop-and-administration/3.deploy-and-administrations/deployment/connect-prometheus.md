# 接入 Prometheus

## 接入方式

Prometheus 支持 push/pull 两种 metrics 获取方式，**Nebula Graph** 支持pull 方式，在 pull 方式下，需要 Prometheus 周期性地通过 HTTP 请求到特定的端口拉取 metrics 数据。

## 启动 Nebula Graph

可以参考[快速入门](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-CN/1.overview/2.quick-start/1.get-started.md)快速启动 **Nebula Graph** 服务。

## 配置启动 Prometheus

本节介绍 Prometheus 配置，让 Prometheus 到配置好的端口拉取 metrics 数据。Prometheus 安装配置详情请参考 Prometheus [官方文档](https://prometheus.io/docs/prometheus/latest/getting_started/)。本节只修改拉取 metrics 数据的端口。示例配置文件 `prometheus.yml` 如下。

```yaml
# my global config
global:
  scrape_interval:     15s # Set the scrape interval to every 15 seconds. Default is every 1 minute.
  evaluation_interval: 15s # Evaluate rules every 15 seconds. The default is every 1 minute.
  # scrape_timeout is set to the global default (10s).

# Alertmanager configuration
alerting:
  alertmanagers:
  - static_configs:
    - targets:
      # - alertmanager:9093

# Load rules once and periodically evaluate them according to the global 'evaluation_interval'.
rule_files:
  # - "first_rules.yml"
  # - "second_rules.yml"

# A scrape configuration containing exactly one endpoint to scrape:
# Here it's Prometheus itself.
scrape_configs:
  # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
  - job_name: 'prometheus'

    # metrics_path defaults to '/metrics'
    # scheme defaults to 'http'.

    static_configs:
    - targets: ['localhost:11000', 'localhost:12000', 'localhost:13000']

```

如上所示，对于默认配置单机启动的 **Nebula Graph** 来说，只需要拉取 11000、12000、13000 三个端口的数据即可。如果是集群或者非默认配置启动，需要将所有服务的 HTTP 端口暴露给 Prometheus。

## 通过 Prometheus 查看 metrics

成功执行以上三个步骤后，**Nebula Graph** 和 Prometheus 已启动配置连接完成，此时可以通过浏览器访问 Prometheus 提供的图形化操作界面，在浏览器中输入 `http://localhost:9090`，并在 Prometheus 的查询框内输入 `add_edges_latency_bucket`，并点击 `execute` 按钮，就可以查询到对应 metrics 值，如下图所示：
![image](https://user-images.githubusercontent.com/42762957/69702038-465c3200-1129-11ea-8641-2ece295390a1.png)
