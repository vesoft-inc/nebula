<p align="center">
  <img src="docs/logo.png"/>
  <br> English | <a href="README-CN.md">中文</a>
  <br>A distributed, scalable, lightning-fast graph database<br>
</p>
<p align="center">
  <a href="https://hub.docker.com/u/vesoft">
    <img src="https://github.com/vesoft-inc/nebula/workflows/docker/badge.svg" alt="build docker image workflow"/>
  </a>
  <a href="https://github.com/vesoft-inc/nebula/actions?workflow=package">
    <img src="https://github.com/vesoft-inc/nebula/workflows/package/badge.svg" alt="make nebula packages workflow"/>
  </a>
  <a href="http://githubbadges.com/star.svg?user=vesoft-inc&repo=nebula&style=default">
    <img src="http://githubbadges.com/star.svg?user=vesoft-inc&repo=nebula&style=default" alt="nebula star"/>
  </a>
  <a href="http://githubbadges.com/fork.svg?user=vesoft-inc&repo=nebula&style=default">
    <img src="http://githubbadges.com/fork.svg?user=vesoft-inc&repo=nebula&style=default" alt="nebula fork"/>
  </a>
  <a href="https://codecov.io/gh/vesoft-inc/nebula">
    <img src="https://codecov.io/gh/vesoft-inc/nebula/branch/master/graph/badge.svg" alt="codecov"/>
  </a>
  <br>
</p>

## About this Repo

Please note that this repo is only for Nebula Graph `v1.*.*`.

From `v2.*.*`, which was already GA in 2021 April, Nebula Graph's core codebases are [nebula-graph](https://github.com/vesoft-inc/nebula-graph), [nebula-common](https://github.com/vesoft-inc/nebula-common) and [nebula-storage](https://github.com/vesoft-inc/nebula-storage), it's recommended to start from the repo: [nebula-graph](https://github.com/vesoft-inc/nebula-graph).


# What is Nebula Graph

**Nebula Graph** is an open-source graph database capable of hosting super large-scale graphs with billions of vertices (nodes) and trillions of edges, with milliseconds of latency. It delivers enterprise-grade high performance to simplify the most complex data sets imaginable into meaningful and useful information.

Below is the architecture of **Nebula Graph**:

![image](https://github.com/vesoft-inc/nebula-docs/raw/master/images/Nebula%20Arch.png)

Compared with other graph database solutions, **Nebula Graph** has the following advantages:

* Symmetrically distributed
* Storage and computing separation
* Horizontal scalability
* Strong data consistency by RAFT protocol
* SQL-like query language
* Role-based access control for higher level security

## Quick start

Read the `Getting started` from [docs](https://docs.nebula-graph.io/) to quickly get going with **Nebula Graph**.

For Nebula Graph Installation, in addition to referring to the docs to deploy from one of the three options: source code, RPM/DEB package, or Docker Compose, if you prefer a video tutorial, please check out our [YouTube channel](https://www.youtube.com/channel/UC73V8q795eSEMxDX4Pvdwmw).

In case you encounter any problem, be sure to ask us on our [official forum](https://discuss.nebula-graph.io).

>**Note**
>
>For the users from the legacy `v1.*.*`, please reach out to the following links:
>- [Getting started](https://docs.nebula-graph.io/1.2.0/manual-EN/1.overview/2.quick-start/1.get-started/)
>- [installing from source code](https://docs.nebula-graph.io/1.2.0/manual-EN/3.build-develop-and-administration/1.build/1.build-source-code/)
>- [rpm/deb packages](https://docs.nebula-graph.io/1.2.0/manual-EN/3.build-develop-and-administration/2.install/1.install-with-rpm-deb/)


## Documentation

* [English](https://docs.nebula-graph.io/)
* [Simplified Chinese (简体中文)](https://docs.nebula-graph.com.cn/)

<!--
## Roadmap

See our [Roadmap](https://github.com/vesoft-inc/nebula/wiki/Nebula-Graph-Roadmap) for what's coming soon in **Nebula Graph**.
-->

## Visualization Tool: Nebula Graph Studio

Visit [Nebula Graph Studio](https://github.com/vesoft-inc/nebula-web-docker) for visual exploration of graph data on a web UI.

## Supported Clients

* [Go](https://github.com/vesoft-inc/nebula-go)
* [Python](https://github.com/vesoft-inc/nebula-python)
* [Java](https://github.com/vesoft-inc/nebula-java)

## Licensing

**Nebula Graph** is under [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license. So you can freely download, modify, and deploy the source code to meet your needs. You can also freely deploy **Nebula Graph** as a back-end service to support your SaaS deployment.

In order to prevent cloud providers from monetizing the project without contributing back, we added [Commons Clause 1.0](https://commonsclause.com/) to the project. As mentioned, we are fully committed to the open source community. We would love to hear your thoughts on the licensing model and are willing to make it more suitable for the community.

## Contributing

Contributions are warmly welcomed and greatly appreciated. Here are a few ways you can contribute:

* Start by some [good first issues](https://github.com/vesoft-inc/nebula/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
* Submit Pull Requests to us. See [how-to-contribute](https://github.com/vesoft-inc/nebula-community/blob/master/Contributors/how-to-contribute.md).

## Getting help & Contact

In case you encounter any problems playing around **Nebula Graph**, please reach out for help:

* [Official Forum](https://discuss.nebula-graph.io)
* Twitter: [@NebulaGraph](https://twitter.com/NebulaGraph)
* [Facebook page](https://www.facebook.com/NebulaGraph/)
* [LinkedIn page](https://www.linkedin.com/company/vesoft-nebula-graph/)

If you like **Nebula Graph**, please leave us a star.<a href="http://githubbadges.com/star.svg?user=vesoft-inc&repo=nebula&style=default">
    <img src="http://githubbadges.com/star.svg?user=vesoft-inc&repo=nebula&style=default" alt="nebula star"/>
  </a>