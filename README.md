<p align="center">
  <img src="https://docs-cdn.nebula-graph.com.cn/figures/nebularepo-logo-new.png"/>
  <br> English | <a href="README-CN.md">中文</a>
  <br>A distributed, scalable, lightning-fast graph database<br>
</p>
<p align="center">
  <a href="https://stackoverflow.com/questions/tagged/nebula-graph">
      <img src="https://img.shields.io/badge/Stack%20Overflow-nebula--graph-orange" alt="Stack Overflow" />
  </a>
  <a href="https://app.codecov.io/gh/vesoft-inc/nebula">
    <img src="https://codecov.io/github/vesoft-inc/nebula/coverage.svg?branch=master" alt="code coverage"/>
  </a>
  <a href="https://github.com/vesoft-inc/nebula/actions?workflow=nightly">
    <img src="https://github.com/vesoft-inc/nebula/workflows/nightly/badge.svg" alt="nightly build"/>
  </a>
  <a href="https://github.com/vesoft-inc/nebula/stargazers">
      <img src="https://img.shields.io/github/stars/vesoft-inc/nebula" alt="GitHub stars" />
  </a>
  <a href="https://github.com/vesoft-inc/nebula/network/members">
      <img src="https://img.shields.io/github/forks/vesoft-inc/nebula" alt="GitHub forks" />
  </a>
  <a href="https://todos.tickgit.com/browse?repo=github.com/vesoft-inc/nebula">
      <img src="https://img.shields.io/endpoint?url=https://todos.tickgit.com/badge?repo=github.com/vesoft-inc/nebula" alt="GitHub TODOs" />
  </a>
  <br>
</p>


# NebulaGraph

## Introduction

**NebulaGraph** is a popular open-source graph database that can handle large volumes of data with milliseconds of latency, scale up quickly, and have the ability to perform fast graph analytics. NebulaGraph has been widely used for social media, recommendation systems, knowledge graphs, security, capital flows, AI, etc. See [our users](https://nebula-graph.io/cases).

The following lists some of **NebulaGraph** features:

* Symmetrically distributed
* Storage and computing separation
* Horizontal scalability
* Strong data consistency by RAFT protocol
* OpenCypher-compatible query language
* Role-based access control for higher-level security
* Different types of graph analytics algorithms

The following figure shows the architecture of the **NebulaGraph** core.
![NebulaGraph Architecture](https://docs-cdn.nebula-graph.com.cn/figures/nebula-graph-architecture_3.png)

Learn more on [NebulaGraph website](https://nebula-graph.io/).
<!--
## Notice of Release

NebulaGraph used to be split into three repositories: [Nebula-Graph](https://github.com/vesoft-inc/nebula-graph), [Nebula-Storage,](https://github.com/vesoft-inc/nebula-storage) and [Nebula-Common](https://github.com/vesoft-inc/nebula-common) for versions between v2.0.0 and v2.5.x, which will be archived.

The one and only codebase of NebulaGraph is now [github.com/vesoft-inc/nebula](https://github.com/vesoft-inc/nebula), as it's back to mono-repo since v2.6.0.

Please check the latest release via the documentation: https://docs.nebula-graph.io/.


NebulaGraph 1.x is not actively maintained. Please move to NebulaGraph 2.x.  <br/>
The data format, rpc protocols, clients, etc. are not compatible between NebulaGraph v1.x and v2.x,  but we do offer [upgrade guide](https://docs.nebula-graph.io/2.5.0/4.deployment-and-installation/3.upgrade-nebula-graph/upgrade-nebula-graph-to-250/).

To use the stable release, see [NebulaGraph 1.0](https://github.com/vesoft-inc/nebula).


## Roadmap

See our [Roadmap](https://github.com/vesoft-inc/nebula/wiki/Nebula-Graph-Roadmap-2020) for what's coming soon in **NebulaGraph**.
-->

## Quick start

Read the [getting started](https://docs.nebula-graph.io/3.2.0/2.quick-start/1.quick-start-workflow/) article for a quick start.

## Using NebulaGraph

NebulaGraph is a distributed graph database with multiple components. You can [download](https://www.nebula-graph.io/download) or try in following ways:

- [Build from source](https://docs.nebula-graph.io/3.3.0/4.deployment-and-installation/2.compile-and-install-nebula-graph/1.install-nebula-graph-by-compiling-the-source-code/)

- In the cloud: [AWS](https://docs.nebula-graph.io/3.1.3/nebula-cloud/nebula-cloud-on-aws/1.aws-overview/) and [Azure](https://docs.nebula-graph.io/3.1.3/nebula-cloud/nebula-cloud-on-azure/azure-self-managed/1.azure-overview/)
  
## Getting help

In case you encounter any problems playing around **NebulaGraph**, please reach out for help:
* [FAQ](https://docs.nebula-graph.io/3.3.0/20.appendix/0.FAQ/)
* [Discussions](https://github.com/vesoft-inc/nebula/discussions)
* [Documentation](https://docs.nebula-graph.io/)

## DevTools

NebulaGraph comes with a set of tools to help you manage and monitor your graph database. See [Ecosystem](https://docs.nebula-graph.io/3.3.0/20.appendix/6.eco-tool-version/).

## Contributing

Contributions are warmly welcomed and greatly appreciated. And here are a few ways you can contribute:

* Start by some [issues](https://github.com/vesoft-inc/nebula/issues)
* Submit Pull Requests to us. See [how-to-contribute](https://docs.nebula-graph.io/master/15.contribution/how-to-contribute/).

## Landscape

<p align="left">
<img src="https://landscape.cncf.io/images/left-logo.svg" width="150">&nbsp;&nbsp;<img src="https://landscape.cncf.io/images/right-logo.svg" width="200" />
<br />

[NebulaGraph](https://landscape.cncf.io/?selected=nebula-graph) enriches the [CNCF Database Landscape](https://landscape.cncf.io/card-mode?category=database&grouping=category).
</p>

## Licensing

**NebulaGraph** is under [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license, so you can freely download, modify, and deploy the source code to meet your needs.  <br/>
You can also freely deploy **NebulaGraph** as a back-end service to support your SaaS deployment.

## Contact

* [Community Chat](https://community-chat.nebula-graph.io/)
* [Slack Channel](https://join.slack.com/t/nebulagraph/shared_invite/zt-7ybejuqa-NCZBroh~PCh66d9kOQj45g)
* [Stack Overflow](https://stackoverflow.com/questions/tagged/nebula-graph)
* Twitter: [@NebulaGraph](https://twitter.com/NebulaGraph)
* [LinkedIn Page](https://www.linkedin.com/company/nebula-graph/)
* Email: info@vesoft.com

## Community

| Join NebulaGraph Community          | Where to Find us                                             |
| ----------------------------------- | ------------------------------------------------------------ |
| Asking Questions                    | [![Stack Overflow](https://img.shields.io/badge/Stack%20Overflow-nebula--graph-orange?style=for-the-badge&logo=stack-overflow&logoColor=white)](https://stackoverflow.com/questions/tagged/nebula-graph) [![Discussions](https://img.shields.io/badge/GitHub_Discussion-000000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/vesoft-inc/nebula/discussions) |
| Chat with Community Members         | [![Chat History](https://img.shields.io/badge/Community%20Chat-000000?style=for-the-badge&logo=discord&logoColor=white)](https://community-chat.nebula-graph.io/) [![Slack](https://img.shields.io/badge/Slack-9F2B68?style=for-the-badge&logo=slack&logoColor=white)](https://join.slack.com/t/nebulagraph/shared_invite/zt-7ybejuqa-NCZBroh~PCh66d9kOQj45g) |
| NebulaGraph Meetup                  | [![Google Calendar](https://img.shields.io/badge/Calander-4285F4?style=for-the-badge&logo=google&logoColor=white)](https://calendar.google.com/calendar/u/0?cid=Z29mbGttamM3ZTVlZ2hpazI2cmNlNXVnZThAZ3JvdXAuY2FsZW5kYXIuZ29vZ2xlLmNvbQ)  [![Zoom](https://img.shields.io/badge/Zoom-2D8CFF?style=for-the-badge&logo=zoom&logoColor=white)](https://us02web.zoom.us/meeting/register/tZ0rcuypqDMvGdLuIm4VprTlx96wrEf062SH) [![Meetup](https://img.shields.io/badge/Meetup-FF0000?style=for-the-badge&logo=meetup&logoColor=white)](https://www.meetup.com/nebulagraph/events/) [![Meeting Archive](https://img.shields.io/badge/Meeting_Archive-808080?style=for-the-badge&logo=readthedocs&logoColor=white)](https://github.com/vesoft-inc/nebula-community/wiki) |
| Chat, Asking, or Meeting in Chinese | [![WeChat Group](https://img.shields.io/badge/WeChat_Group-000000?style=for-the-badge&logo=wechat)](https://wj.qq.com/s2/8321168/8e2f/) [![Tencent_Meeting](https://img.shields.io/badge/腾讯会议-2D8CFF?style=for-the-badge&logo=googlemeet&logoColor=white)](https://meeting.tencent.com/dm/F8NX1aRZ8PQv) [![Discourse](https://img.shields.io/badge/中文论坛-4285F4?style=for-the-badge&logo=discourse&logoColor=white)](https://discuss.nebula-graph.com.cn/) |

<br />

#### If you find NebulaGraph interesting, please ⭐️ [Star](https://github.com/vesoft-inc/nebula) it at the top of the GitHub page.

