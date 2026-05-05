环境信息：3节点环境，k8s容器化部署nebula graph，其中graph/meta/storage均为3实例部署 图空间创建参数为partition_num=20,replica_factor=3
背景：当前代码仓库是nebula graph开源图数据库master分支版本。我是一位开源贡献者，计划将之前修复的开源问题提交社区仓库。修复的开源问题是偶先出现leader均衡任务和创建图空间任务同时下发，storage服务出现offline（通过执行show hosts命令），工作线程全部死锁，storage服务无法对外提供工作。当前问题我已修复PR见链接https://github.com/vesoft-inc/nebula/pull/6145，git提交ID：60df94c7d3ed2500086a763487c91b79da9ae22a。
补充：模糊记得1个关键因素，leader均衡任务和创建图空间任务在操作分片时都会操作分片级锁（全局锁），有一个线程一直占用锁不释放，导致其他线程阻塞

诉求：1、当前我记不清代码bug的详细根因，因此提交PR受阻。因此我需要你辅助我复现问题，分析bug根因，并分析我当前修复的方式是否合理，并协助我根据nebula graph的官方要求写出符合要求的PR。我当前有环境复现，且有现成的脚本可以复现，并且也有gdb或者生成coredump的能力。
2、我是在nebula graph的3.6版本（release-3.6分支）发现这个问题的，不确定master分支是否存在，请你帮我确认;3、按照nebula graph的官方要求，请阅读我当前的PR，分析需要补充哪些信息，并协助我补充
