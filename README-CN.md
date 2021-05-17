<p align="center">
  <img src="docs/logo.png"/>
  <br>中文 | <a href="README.md">English</a>
  <br>世界上唯一能够容纳千亿个顶点和万亿条边，并提供毫秒级查询延时的图数据库解决方案<br>
</p>

<p align="center">
  <a href="https://user-images.githubusercontent.com/38887077/67449282-4362b300-f64c-11e9-878f-7efc373e5e55.jpg"><img src="https://img.shields.io/badge/WeChat-%E5%BE%AE%E4%BF%A1-brightgreen" alt="WeiXin"></a>
  <a href="https://www.zhihu.com/org/nebulagraph/activities"><img src="https://img.shields.io/badge/Zhihu-%E7%9F%A5%E4%B9%8E-blue" alt="Zhihu"></a>
  <a href="https://segmentfault.com/t/nebula"><img src="https://img.shields.io/badge/SegmentFault-%E6%80%9D%E5%90%A6-green" alt="SegmentFault"></a>
  <a href="https://weibo.com/p/1006067122684542/home?from=page_100606&mod=TAB#place"><img src="https://img.shields.io/badge/Weibo-%E5%BE%AE%E5%8D%9A-red" alt="Sina Weibo"></a>
  <a href="http://githubbadges.com/star.svg?user=vesoft-inc&repo=nebula&style=default">
    <img src="http://githubbadges.com/star.svg?user=vesoft-inc&repo=nebula&style=default" alt="nebula star"/>
  </a>
  <a href="http://githubbadges.com/fork.svg?user=vesoft-inc&repo=nebula&style=default">
    <img src="http://githubbadges.com/fork.svg?user=vesoft-inc&repo=nebula&style=default" alt="nebula fork"/>
  </a>
  <a href="https://codecov.io/gh/vesoft-inc/nebula">
    <img src="https://codecov.io/gh/vesoft-inc/nebula/branch/master/graph/badge.svg" alt="codecov"/>
  </a>
</p>

# Nebula Graph 是什么

**Nebula Graph** 是一款开源的图数据库，擅长处理千亿个顶点和万亿条边的超大规模数据集。

下图为 **Nebula Graph** 产品架构图：

![image](https://github.com/vesoft-inc/nebula-docs/raw/master/images/Nebula%20Arch.png)

与其他图数据库产品相比，**Nebula Graph** 具有如下优势：

* 全对称分布式架构
* 存储与计算分离
* 水平可扩展性
* RAFT 协议下的数据强一致
* 类 SQL 查询语言
* 用户鉴权

## Nebula Graph Cloud Service 火热公测中

[Nebula Graph Cloud Service](https://cloud.nebula-graph.com.cn/) 是 Nebula Graph 的图数据库云服务平台（Database-as-a-Service，DBaaS），支持一键部署 Nebula Graph。Nebula Graph Cloud Service 服务目前处于公测阶段。公测期间不会就云服务收取任何费用，欢迎 **免费试用**。

## 关于这个 Repo

Please note that this repo is only for Nebula Graph `v1.*.*`.

请注意当前 Repo(https://github.com/vesoft-inc/nebula) 是 Nebula Graph 的旧版本（ `v1.*.*`）仓库。

从 `v2.*.*`开始（2021年四月发布了 GA），Nebula Graph 的代码被分到了三个仓库中： [nebula-graph](https://github.com/vesoft-inc/nebula-graph)，[nebula-common](https://github.com/vesoft-inc/nebula-common) 和 [nebula-storage](https://github.com/vesoft-inc/nebula-storage)。

我们推荐您从访问 [nebula-graph](https://github.com/vesoft-inc/nebula-graph) 开始。

## 快速使用

请查看[快速使用手册](https://docs.nebula-graph.com.cn/2.0.1/2.quick-start/1.quick-start-workflow/)，开始使用 **Nebula Graph**。

在开始使用 **Nebula Graph** 之前，必须通过[编译源码](https://docs.nebula-graph.com.cn/2.0.1/4.deployment-and-installation/2.compile-and-install-nebula-graph/1.install-nebula-graph-by-compiling-the-source-code/)，[rpm/deb 包](https://docs.nebula-graph.com.cn/2.0.1/4.deployment-and-installation/2.compile-and-install-nebula-graph/2.install-nebula-graph-by-rpm-or-deb/) 或者 [docker compose](https://github.com/vesoft-inc/nebula-docker-compose/blob/master/README_zh-CN.md) 方式安装 **Nebula Graph**。您也可以观看[视频](https://space.bilibili.com/472621355)学习如何安装 **Nebula Graph**。

**注意**

如果您想使用旧的  `v1.*.*` 版本请参考接下来的链接 [Getting started](https://docs.nebula-graph.com.cn/1.2.0/manual-CN/1.overview/2.quick-start/1.get-started/), [installing source code](https://docs.nebula-graph.com.cn/1.2.0/manual-CN/3.build-develop-and-administration/1.build/1.build-source-code/), [rpm/deb packages](https://docs.nebula-graph.com.cn/1.2.0/manual-CN/3.build-develop-and-administration/2.install/1.install-with-rpm-deb/)

如果您遇到任何问题，请前往 Nebula Graph [官方论坛](https://discuss.nebula-graph.com.cn) 提问。

## 文档

* [简体中文](https://docs.nebula-graph.com.cn/)
* [English](https://docs.nebula-graph.io/)

<!--
## 产品路线图

**Nebula Graph** 产品规划路线图请参见 [roadmap](https://github.com/vesoft-inc/nebula/wiki/Nebula-Graph-Roadmap)。
-->

## 可视化工具：Nebula Graph Studio

查看图可视化工具[Nebula Graph Studio](https://github.com/vesoft-inc/nebula-web-docker)，开启图数据可视化探索之旅。

## 支持的客户端

* [Go](https://github.com/vesoft-inc/nebula-go)
* [Python](https://github.com/vesoft-inc/nebula-python)
* [Java](https://github.com/vesoft-inc/nebula-java)

## 许可证

**Nebula Graph** 使用 [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) 许可证，您可以免费下载，修改以及部署源代码。您还可以将 **Nebula Graph** 作为后端服务部署以支持您的 SaaS 部署。

为防止云供应商从项目赢利而不回馈，**Nebula Graph** 在项目中添加了 [Commons Clause 1.0](https://commonsclause.com/) 条款。如上所述，**Nebula Graph** 是一个完全开源的项目，欢迎您就许可模式提出建议，帮助 **Nebula Graph** 社区更好地发展。

## 如何贡献

**Nebula Graph** 是一个完全开源的项目，欢迎开源爱好者通过以下方式参与到 **Nebula Graph** 社区：

* 从标记为 [good first issues](https://github.com/vesoft-inc/nebula/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) 的问题入手
* 贡献代码，详情请参见 [如何贡献](https://docs.nebula-graph.com.cn/2.0.1/15.contribution/how-to-contribute/)
* 直接在GitHub上提 [Issue](https://github.com/vesoft-inc/nebula/issues)

## 获取帮助 & 联系方式

在使用 **Nebula Graph** 过程中遇到任何问题，都可以通过下面的方式寻求帮助：

* [官方论坛](https://discuss.nebula-graph.com.cn/)
* 访问官网 [Home Page](http://nebula-graph.io/)。
* [![WeiXin](https://img.shields.io/badge/WeChat-%E5%BE%AE%E4%BF%A1-brightgreen)](https://user-images.githubusercontent.com/38887077/67449282-4362b300-f64c-11e9-878f-7efc373e5e55.jpg)
* [![Sina Weibo](https://img.shields.io/badge/Weibo-%E5%BE%AE%E5%8D%9A-red)](https://weibo.com/p/1006067122684542/home?from=page_100606&mod=TAB#place)
* email: info@vesoft.com

如果喜欢 **Nebula Graph**，请为我们点 star。<a href="http://githubbadges.com/star.svg?user=vesoft-inc&repo=nebula&style=default">
    <img src="http://githubbadges.com/star.svg?user=vesoft-inc&repo=nebula&style=default" alt="nebula star"/>
  </a>