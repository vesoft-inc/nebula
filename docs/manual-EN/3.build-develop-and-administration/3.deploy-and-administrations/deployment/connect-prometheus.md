# Connect to Prometheus

## Connection Method

Prometheus supports fetching metrics with `push/pull`. **Nebula Graph** only supports `pull`. When you pull, Prometheus pulls metrics data periodically from certain endpoints via HTTP requests.

## Start Nebula Graph

Refer to [Get Started](https://github.com/vesoft-inc/nebula/blob/master/docs/manual-CN/1.overview/2.quick-start/1.get-started.md) to start **Nebula Graph** services.

## Configure Prometheus

This section introduces Prometheus configurations. Prometheus fetches metrics data from the configured endpoints. For details on Prometheus installation and configuration, please refer to Prometheus [Official Documentation](https://prometheus.io/docs/prometheus/latest/getting_started/). In this section, we only modify the endpoints that pull the metrics data. An example configuration file `prometheus.yml` is as follows.

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

As shown above, for default configuration **Nebula Graph**, i.e. the single node, we only need to pull data of endpoints 11000, 12000, and 13000. For non-default configuration or clusters, you need to expose all the HTTP endpoints of all services to Prometheus.

## Check Metrics via Prometheus

After executing the above three steps successfully, all the configurations are completed and **Nebula Graph** and Prometheus are connected. Now you can access the graphical operation interface provided by Prometheus through a browser, enter `http: // localhost: 9090` in the browser, then input `add_edges_latency_bucket` in the query box of Prometheus, click the `execute` button to check the corresponding metrics value. Consider the following example:
![image](https://user-images.githubusercontent.com/42762957/69702038-465c3200-1129-11ea-8641-2ece295390a1.png)