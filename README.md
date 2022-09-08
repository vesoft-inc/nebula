<p align="center">
  <img src="https://nebula-website-cn.oss-cn-hangzhou.aliyuncs.com/nebula-website/images/nebulagraph-logo.png"/>
  <br> English | <a href="README-CN.md">中文</a>
  <br>A distributed, scalable, lightning-fast graph database<br>
</p>
<p align="center">
  <a href="https://app.codecov.io/gh/vesoft-inc/nebula">
    <img src="https://codecov.io/github/vesoft-inc/nebula/coverage.svg?branch=master" alt="code coverage"/>
  </a>
  <a href="https://github.com/vesoft-inc/nebula/actions?workflow=nightly">
    <img src="https://github.com/vesoft-inc/nebula/workflows/nightly/badge.svg" alt="nightly build"/>
  </a>
  <a href="https://github.com/vesoft-inc/nebula/stargazers">
    <img src="http://githubbadges.com/star.svg?user=vesoft-inc&repo=nebula&style=default" alt="nebula star"/>
  </a>
  <a href="https://github.com/vesoft-inc/nebula/network/members">
    <img src="http://githubbadges.com/fork.svg?user=vesoft-inc&repo=nebula&style=default" alt="nebula fork"/>
  </a>
  <br>
</p>

# What is NebulaGraph?

**NebulaGraph** is an open-source graph database capable of hosting super large scale graphs <br/>
with dozens of billions of vertices (nodes) and trillions of edges, with milliseconds of latency. 


It has a lot of users, including some famous and big companies of **BAT**, **TMD**,  <br/>
and has been widely used for social media, recommendations, security, capital flows, AI, etc.<br/>
https://nebula-graph.io/cases


Compared with other graph database solutions, **NebulaGraph** has the following advantages:

* Symmetrically distributed
* Storage and computing separation
* Horizontal scalability
* Strong data consistency by RAFT protocol
* OpenCypher-compatible query language
* Role-based access control for higher level security

## Notice of Release

This repository hosts the source code of NebulaGraph versions before 2.0.0-alpha and after v2.5.x.  <br/>
If you are looking to use the versions between v2.0.0 and v2.5.x, please head to [NebulaGraph repo](https://github.com/vesoft-inc/nebula-graph).

NebulaGraph 1.x is not actively maintained. Please move to NebulaGraph 2.x.  <br/>
The data format, rpc protocols, clients, etc. are not compatible between NebulaGraph v1.x and v2.x,  but we do offer [upgrade guide](https://docs.nebula-graph.io/2.5.0/4.deployment-and-installation/3.upgrade-nebula-graph/upgrade-nebula-graph-to-250/).

<!--
To use the stable release, see [NebulaGraph 1.0](https://github.com/vesoft-inc/nebula).


## Roadmap

See our [Roadmap](https://github.com/vesoft-inc/nebula/wiki/Nebula-Graph-Roadmap-2020) for what's coming soon in **NebulaGraph**.
-->

## Quick start

Read the [Getting started](https://docs.nebula-graph.io/3.2.0/2.quick-start/1.quick-start-workflow/) article for a quick start.

<!--
Please note that you need to install **NebulaGraph**, either by [installing source code](https://docs.nebula-graph.io/manual-EN/3.build-develop-and-administration/1.build/1.build-source-code/) or by [docker compose](https://docs.nebula-graph.io/manual-EN/3.build-develop-and-administration/1.build/2.build-by-docker/), before you can actually start using it. If you prefer a video tutorial, visit our [YouTube channel](https://www.youtube.com/channel/UC73V8q795eSEMxDX4Pvdwmw/videos).
-->

## Getting help
In case you encounter any problems playing around **NebulaGraph**, please reach out for help:
* [FAQ](https://docs.nebula-graph.io/2.0/2.quick-start/0.FAQ/)
* [Discussions](https://github.com/vesoft-inc/nebula/discussions)

## Documentation

* [English](https://docs.nebula-graph.io/)

## Architecture
![NebulaGraph Architecture](https://docs-cdn.nebula-graph.com.cn/figures/nebula-graph-architecture_3.png)

## Contributing

Contributions are warmly welcomed and greatly appreciated. And here are a few ways you can contribute:

* Start by some [issues](https://github.com/vesoft-inc/nebula/issues)
* Submit Pull Requests to us. See [how-to-contribute](https://docs.nebula-graph.io/master/15.contribution/how-to-contribute/).

## Landscape

<p align="left">
<img src="https://landscape.cncf.io/images/left-logo.svg" width="150">&nbsp;&nbsp;<img src="https://landscape.cncf.io/images/right-logo.svg" width="200" />
<br />

[NebulaGraph](https://landscape.cncf.io/?selected=nebula-graph) enriches the <a href="https://landscape.cncf.io/card-mode?category=database&grouping=category">
CNCF Database Landscape.</a>
</p>

## Licensing

**NebulaGraph** is under [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license, so you can freely download, modify, and deploy the source code to meet your needs.  <br/>
You can also freely deploy **NebulaGraph** as a back-end service to support your SaaS deployment.

## Contact

* [Slack Channel](https://join.slack.com/t/nebulagraph/shared_invite/zt-7ybejuqa-NCZBroh~PCh66d9kOQj45g)
* [Stack Overflow](https://stackoverflow.com/questions/tagged/nebulagraph)
* Twitter: [@NebulaGraph](https://twitter.com/NebulaGraph)
* [LinkedIn Page](https://www.linkedin.com/company/vesoft-nebula-graph)
* Email: info@vesoft.com

## Community

[![Discussions](https://img.shields.io/badge/GitHub_Discussion-000000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/vesoft-inc/nebula/discussions) [![Slack](https://img.shields.io/badge/Slack-9F2B68?style=for-the-badge&logo=slack&logoColor=white)](https://join.slack.com/t/nebulagraph/shared_invite/zt-7ybejuqa-NCZBroh~PCh66d9kOQj45g) [![Zoom](https://img.shields.io/badge/Zoom-2D8CFF?style=for-the-badge&logo=zoom&logoColor=white)](https://us02web.zoom.us/meeting/register/tZ0rcuypqDMvGdLuIm4VprTlx96wrEf062SH) [![Google Calendar](https://img.shields.io/badge/Calander-4285F4?style=for-the-badge&logo=google&logoColor=white)](https://calendar.google.com/calendar/u/0?cid=Z29mbGttamM3ZTVlZ2hpazI2cmNlNXVnZThAZ3JvdXAuY2FsZW5kYXIuZ29vZ2xlLmNvbQ) [![Meetup](https://img.shields.io/badge/Meetup-FF0000?style=for-the-badge&logo=meetup&logoColor=white)](https://www.meetup.com/nebulagraph/events/287180186?utm_medium=referral&utm_campaign=share-btn_savedevents_share_modal&utm_source=link) [![Meeting Archive](https://img.shields.io/badge/Community_wiki-808080?style=for-the-badge&logo=readthedocs&logoColor=white)](https://github.com/vesoft-inc/nebula-community/wiki) [![Discourse](https://img.shields.io/badge/中文论坛-4285F4?style=for-the-badge&logo=discourse&logoColor=white)](https://discuss.nebula-graph.com.cn/) [![Tecent_Meeting](https://img.shields.io/badge/腾讯会议-2D8CFF?style=for-the-badge&logo=googlemeet&logoColor=white)](https://meeting.tencent.com/dm/F8NX1aRZ8PQv)

<br />

#### If you find NebulaGraph interesting, please ⭐️ Star it on GitHub at the top of the page.
https://github.com/vesoft-inc/nebula
