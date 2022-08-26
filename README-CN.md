<p align="center">
  <img src="https://nebula-website-cn.oss-cn-hangzhou.aliyuncs.com/nebula-website/images/nebulagraph-logo.png"/>
  <br>中文 | <a href="README.md">English</a>
  <br>世界上唯一能够容纳千亿个顶点和万亿条边，并提供毫秒级查询延时的图数据库解决方案<br>
</p>

<p align="center">
  <a href="https://user-images.githubusercontent.com/38887077/67449282-4362b300-f64c-11e9-878f-7efc373e5e55.jpg">
    <img src="https://img.shields.io/badge/WeChat-%E5%BE%AE%E4%BF%A1-brightgreen" alt="WeiXin">
  </a>
  <a href="https://www.zhihu.com/org/nebulagraph/activities">
    <img src="https://img.shields.io/badge/Zhihu-%E7%9F%A5%E4%B9%8E-blue" alt="Zhihu">
  </a>
  <a href="https://segmentfault.com/t/nebula">
    <img src="https://img.shields.io/badge/SegmentFault-%E6%80%9D%E5%90%A6-green" alt="SegmentFault">
  </a>
  <a href="https://weibo.com/p/1006067122684542/home?from=page_100606&mod=TAB#place">
    <img src="https://img.shields.io/badge/Weibo-%E5%BE%AE%E5%8D%9A-red" alt="Sina Weibo">
  </a>
  <a href="https://github.com/vesoft-inc/nebula/stargazers">
    <img src="http://githubbadges.com/star.svg?user=vesoft-inc&repo=nebula&style=default" alt="nebula star"/>
  </a>
  <a href="https://github.com/vesoft-inc/nebula/network/members">
    <img src="http://githubbadges.com/fork.svg?user=vesoft-inc&repo=nebula&style=default" alt="nebula fork"/>
  </a>
</p>

# NebulaGraph 是什么？


**NebulaGraph** 是一款开源的图数据库，擅长处理千亿个顶点和万亿条边的超大规模数据集。

已陆续被包括在 **BAT**, **TMD** 中的众多知名大厂和大量国内外中小企业使用，<br />
广泛应用在社交媒体、实时推荐、网络安全、金融风控、知识图谱、人工智能等。<br />
https://nebula-graph.com.cn/cases

与其他图数据库产品相比，**NebulaGraph** 具有如下优势：

* 全对称分布式架构
* 存储与计算分离
* 水平可扩展性
* RAFT 协议下的数据强一致
* 支持 openCypher
* 用户鉴权

## 发布通告

v1.x 和 v2.5.x 之后的版本，NebulaGraph 在这个 repo 管理。<br />
如需获取 v2.0.0 到 v2.5.x 之间的版本，请访问 [NebulaGraph repo](https://github.com/vesoft-inc/nebula-graph)。

NebulaGraph 1.x 后续不再进行功能的更新，请升级到 2.0+ 版本。<br />
NebulaGraph内核 1.x 与 2.x 数据格式、通信协议、客户端等均双向不兼容，可参照[升级指导](https://docs.nebula-graph.com.cn/2.5.0/4.deployment-and-installation/3.upgrade-nebula-graph/upgrade-nebula-graph-to-250/)进行升级。

<!--
如需使用稳定版本，请参见[NebulaGraph 1.0](https://github.com/vesoft-inc/nebula)。


## 产品路线图

**NebulaGraph** 产品规划路线图请参见 [roadmap](https://github.com/vesoft-inc/nebula/wiki/Nebula-Graph-Roadmap-2020)。
-->

## 快速使用

请查看[快速使用手册](https://docs.nebula-graph.com.cn/3.2.0/2.quick-start/1.quick-start-workflow/)，开始使用 **NebulaGraph**。

<!--
在开始使用 **NebulaGraph** 之前，必须通过[编译源码](https://docs.nebula-graph.com.cn/manual-CN/3.build-develop-and-administration/1.build/1.build-source-code/)或者 [docker compose](https://docs.nebula-graph.com.cn/manual-CN/3.build-develop-and-administration/1.build/2.build-by-docker/) 方式安装 **NebulaGraph**。您也可以观看[视频](https://space.bilibili.com/472621355)学习如何安装 **NebulaGraph**。
-->

## 获取帮助

在使用 **NebulaGraph** 过程中遇到任何问题，都可以通过下面的方式寻求帮助：

* [FAQ](https://docs.nebula-graph.io/2.0/2.quick-start/0.FAQ/)
* 访问[论坛](https://discuss.nebula-graph.com.cn/)

## 文档

* [简体中文](https://docs.nebula-graph.com.cn/)
* [English](https://docs.nebula-graph.io/)

## NebulaGraph 产品架构图

![image](https://docs-cdn.nebula-graph.com.cn/figures/nebula-graph-architecture_3.png)


## 如何贡献

**NebulaGraph** 是一个完全开源的项目，欢迎开源爱好者通过以下方式参与到 **NebulaGraph** 社区：

* 直接在GitHub上提 [Issue](https://github.com/vesoft-inc/nebula/issues)
* 贡献代码，详情请参见 [如何贡献](https://docs.nebula-graph.com.cn/master/15.contribution/how-to-contribute/)

## 许可证

**NebulaGraph** 使用 [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) 许可证，您可以免费下载，修改以及部署源代码。<br />
您还可以将 **NebulaGraph** 作为后端服务部署以支持您的 SaaS 部署。

## 联系方式

* 访问[官网](http://nebula-graph.com.cn/)
* [![WeiXin](https://img.shields.io/badge/WeChat-%E5%BE%AE%E4%BF%A1-brightgreen)](https://user-images.githubusercontent.com/38887077/67449282-4362b300-f64c-11e9-878f-7efc373e5e55.jpg)
* [![Sina Weibo](https://img.shields.io/badge/Weibo-%E5%BE%AE%E5%8D%9A-red)](https://weibo.com/p/1006067122684542/home?from=page_100606&mod=TAB#place)
* [知乎](https://www.zhihu.com/org/nebulagraph/activities)
* [SegmentFault](https://segmentfault.com/t/nebula)
* Email: info@vesoft.com

## 加入 NebulaGraph 社区

[![Discussions](https://img.shields.io/badge/GitHub_Discussion-000000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/vesoft-inc/nebula/discussions) [![Discourse](https://img.shields.io/badge/中文论坛-4285F4?style=for-the-badge&logo=discourse&logoColor=white)](https://discuss.nebula-graph.com.cn/) [![Slack](https://img.shields.io/badge/Slack-9F2B68?style=for-the-badge&logo=slack&logoColor=white)](https://join.slack.com/t/nebulagraph/shared_invite/zt-7ybejuqa-NCZBroh~PCh66d9kOQj45g) [![Tecent_Meeting](https://img.shields.io/badge/腾讯会议-2D8CFF?style=for-the-badge&logo=googlemeet&logoColor=white)](https://meeting.tencent.com/dm/F8NX1aRZ8PQv) [![Google Calendar](https://img.shields.io/badge/Calander-4285F4?style=for-the-badge&logo=google&logoColor=white)](https://calendar.google.com/calendar/u/0?cid=Z29mbGttamM3ZTVlZ2hpazI2cmNlNXVnZThAZ3JvdXAuY2FsZW5kYXIuZ29vZ2xlLmNvbQ) [![Meetup](https://img.shields.io/badge/Meetup-FF0000?style=for-the-badge&logo=meetup&logoColor=white)](https://www.meetup.com/nebulagraph/events/287180186?utm_medium=referral&utm_campaign=share-btn_savedevents_share_modal&utm_source=link) [![Meeting Archive](https://img.shields.io/badge/Community_wiki-808080?style=for-the-badge&logo=readthedocs&logoColor=white)](https://github.com/vesoft-inc/nebula-community/wiki)

<br />

#### 如果你喜欢这个项目，或者对你有用，可以点右上角 ⭐️ Star 来支持/收藏下~
https://github.com/vesoft-inc/nebula
