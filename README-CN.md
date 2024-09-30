<p align="center">
  <img src="https://docs-cdn.nebula-graph.com.cn/figures/nebularepo-logo-new-cn.png"/>
  <br>中文 | <a href="README.md">English</a>
  <br>世界上唯一能够容纳千亿个顶点和万亿条边，并提供毫秒级查询延时的图数据库解决方案<br>
</p>

<p align="center">
  <a href="https://user-images.githubusercontent.com/38887077/67449282-4362b300-f64c-11e9-878f-7efc373e5e55.jpg">
    <img src="https://img.shields.io/badge/WeChat-%E5%BE%AE%E4%BF%A1-brightgreen" alt="WeiXin">
  </a>
  <a href="https://weibo.com/p/1006067122684542/home?from=page_100606&mod=TAB#place">
    <img src="https://img.shields.io/badge/Weibo-%E5%BE%AE%E5%8D%9A-red" alt="Sina Weibo">
  </a>
  <a href="https://github.com/vesoft-inc/nebula/stargazers">
      <img src="https://img.shields.io/github/stars/vesoft-inc/nebula" alt="GitHub stars" />
  </a>
  <a href="https://github.com/vesoft-inc/nebula/network/members">
      <img src="https://img.shields.io/github/forks/vesoft-inc/nebula" alt="GitHub forks" />
  </a>

</p>

# NebulaGraph 是什么？


**NebulaGraph** 是一款开源的图数据库，擅长处理千亿个顶点和万亿条边的超大规模数据集。

NebulaGraph 社区已成长为一个荟聚了众多用户、融合了各类图技术场景实践知识的活跃开源社区。你可以在其中与大家共同交流 NebulaGraph [周边生态项目](https://docs.nebula-graph.com.cn/master/20.appendix/6.eco-tool-version/)的应用心得，或者社交媒体、实时推荐、网络安全、金融风控、知识图谱、人工智能等[大规模生产场景](https://nebula-graph.com.cn/cases)的实践经验。

**NebulaGraph** 特点如下：

* 全对称分布式架构
* 存储与计算分离
* 水平可扩展性
* RAFT 协议下的数据强一致
* 支持 openCypher
* 用户鉴权
* 支持多种类型的图计算算法

**NebulaGraph** 内核架构图如下：

![image](https://docs-cdn.nebula-graph.com.cn/figures/nebula-graph-architecture_3.png)

点击 [NebulaGraph 官网](https://www.nebula-graph.com.cn/) 了解更多信息。

<!-- ## 发布通告 deprecated

**NebulaGraph** 的 GitHub 仓库经历过拆分和合并的过程。

- 从 v2.6.0 开始，**NebulaGraph** 内核代码集中在 [nebula](https://github.com/vesoft-inc/nebula) 仓库下。

- 从 v2.0.0 到 v2.5.x 的代码分布在 [nebula-graph](https://github.com/vesoft-inc/nebula-graph)、[nebula-storage](https://github.com/vesoft-inc/nebula-storage)、[nebula-common](https://github.com/vesoft-inc/nebula-common) 这几个仓库中，这几个仓库将被归档。

请访问 [NebulaGraph 文档](https://docs.nebula-graph.com.cn/)了解、获取 **NebulaGraph** 的最新的正式版本。

<!--

NebulaGraph 1.x 后续不再进行功能的更新，请升级到 2.0+ 版本。<br />
NebulaGraph内核 1.x 与 2.x 数据格式、通信协议、客户端等均双向不兼容，可参照[升级指导](https://docs.nebula-graph.com.cn/2.5.0/4.deployment-and-installation/3.upgrade-nebula-graph/upgrade-nebula-graph-to-250/)进行升级。

如需使用稳定版本，请参见[NebulaGraph 1.0](https://github.com/vesoft-inc/nebula)。


## 产品路线图

**NebulaGraph** 产品规划路线图请参见 [roadmap](https://github.com/vesoft-inc/nebula/wiki/Nebula-Graph-Roadmap-2020)。
--> 

## 快速使用

您可以在[云上](https://cloud.nebula-graph.io/login)或[本地](https://docs.nebula-graph.com.cn/3.8.0/2.quick-start/3.quick-start-on-premise/2.install-nebula-graph/)快速体验 **NebulaGraph**。

<!--
在开始使用 **NebulaGraph** 之前，必须通过[编译源码](https://docs.nebula-graph.com.cn/manual-CN/3.build-develop-and-administration/1.build/1.build-source-code/)或者 [docker compose](https://docs.nebula-graph.com.cn/manual-CN/3.build-develop-and-administration/1.build/2.build-by-docker/) 方式安装 **NebulaGraph**。您也可以观看[视频](https://space.bilibili.com/472621355)学习如何安装 **NebulaGraph**。
-->

## 安装方式

您可以通过[下载](https://www.nebula-graph.com.cn/download)安装包或者[源码编译](https://docs.nebula-graph.com.cn/3.8.0/2.quick-start/3.quick-start-on-premise/2.install-nebula-graph/)安装 **NebulaGraph**：

## 获取帮助

在使用 **NebulaGraph** 过程中遇到任何问题，都可以通过下面的方式寻求帮助：

* [FAQ](https://docs.nebula-graph.com.cn/3.8.0/20.appendix/0.FAQ/)
* [访问论坛](https://discuss.nebula-graph.com.cn/)
* [查看文档](https://docs.nebula-graph.com.cn/)
  
## 如何贡献

**NebulaGraph** 是一个完全开源的项目，欢迎开源爱好者通过以下方式参与到 **NebulaGraph** 社区：

* 在 GitHub 上提 [Issue](https://github.com/vesoft-inc/nebula/issues)。
* 贡献代码，详情请参见[如何贡献](https://docs.nebula-graph.com.cn/master/15.contribution/how-to-contribute/)。

## 许可证

**NebulaGraph** 使用 [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) 许可证，您可以免费下载，修改以及部署源代码。<br />
您还可以将 **NebulaGraph** 作为后端服务部署以支持您的 SaaS 部署。

## 联系方式

* 访问[官网](http://nebula-graph.com.cn/)
* [![WeiXin](https://img.shields.io/badge/WeChat-%E5%BE%AE%E4%BF%A1-brightgreen)](https://user-images.githubusercontent.com/38887077/67449282-4362b300-f64c-11e9-878f-7efc373e5e55.jpg)
* [![Sina Weibo](https://img.shields.io/badge/Weibo-%E5%BE%AE%E5%8D%9A-red)](https://weibo.com/p/1006067122684542/home?from=page_100606&mod=TAB#place)
* Email: info@vesoft.com

## 加入 NebulaGraph 社区



| 加入 NebulaGraph 社区   | 加入方式                                                     |
| ----------------------- | ------------------------------------------------------------ |
| 微信群                  | [![WeChat Group](https://img.shields.io/badge/微信群-000000?style=for-the-badge&logo=wechat)](https://wj.qq.com/s2/8321168/8e2f/) |
| 提问                    | [![Discourse](https://img.shields.io/badge/中文论坛-4285F4?style=for-the-badge&logo=discourse&logoColor=white)](https://discuss.nebula-graph.com.cn/) [![Stack Overflow](https://img.shields.io/badge/Stack%20Overflow-nebula--graph-orange?style=for-the-badge&logo=stack-overflow&logoColor=white)](https://stackoverflow.com/questions/tagged/nebula-graph) [![Discussions](https://img.shields.io/badge/GitHub_Discussion-000000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/vesoft-inc/nebula/discussions) |
| 聊天                    | [![Chat History](https://img.shields.io/badge/Community%20Chat-000000?style=for-the-badge&logo=discord&logoColor=white)](https://community-chat.nebula-graph.io/) [![Slack](https://img.shields.io/badge/Slack-9F2B68?style=for-the-badge&logo=slack&logoColor=white)](https://join.slack.com/t/nebulagraph/shared_invite/zt-7ybejuqa-NCZBroh~PCh66d9kOQj45g) |
| NebulaGraph Meetup 活动 | [![Tencent_Meeting](https://img.shields.io/badge/腾讯会议-2D8CFF?style=for-the-badge&logo=googlemeet&logoColor=white)](https://meeting.tencent.com/dm/F8NX1aRZ8PQv) [![Google Calendar](https://img.shields.io/badge/Calander-4285F4?style=for-the-badge&logo=google&logoColor=white)](https://calendar.google.com/calendar/u/0?cid=Z29mbGttamM3ZTVlZ2hpazI2cmNlNXVnZThAZ3JvdXAuY2FsZW5kYXIuZ29vZ2xlLmNvbQ)  [![Zoom](https://img.shields.io/badge/Zoom-2D8CFF?style=for-the-badge&logo=zoom&logoColor=white)](https://us02web.zoom.us/meeting/register/tZ0rcuypqDMvGdLuIm4VprTlx96wrEf062SH) [![Meetup](https://img.shields.io/badge/Meetup-FF0000?style=for-the-badge&logo=meetup&logoColor=white)](https://www.meetup.com/nebulagraph/events/) [![Meeting Archive](https://img.shields.io/badge/Meeting_Archive-808080?style=for-the-badge&logo=readthedocs&logoColor=white)](https://github.com/vesoft-inc/nebula-community/wiki) |

<br />

#### 如果你喜欢这个项目，或者对你有用，请[点击](https://github.com/vesoft-inc/nebula)右上角 ⭐️ Star 收藏吧~

