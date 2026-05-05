# 环境信息
3节点环境，k8s容器化部署nebula graph，其中graph/meta/storage均为3实例部署 图空间创建参数为partition_num=20,replica_factor=3
# 背景
当前代码仓库是nebula graph开源图数据库master分支版本。我是一位开源贡献者，计划将之前修复的开源问题提交社区仓库。修复的开源问题是偶先出现leader均衡任务和创建图空间任务同时下发，storage服务出现offline（通过执行show hosts命令），工作线程全部死锁，storage服务无法对外提供工作。当前问题我已修复PR见链接https://github.com/vesoft-inc/nebula/pull/6145，git提交ID：60df94c7d3ed2500086a763487c91b79da9ae22a。

# 诉求
1. 当前我记不清代码bug的详细根因，因此提交PR受阻。因此我需要你辅助我复现问题，分析bug根因，并分析我当前修复的方式是否合理，并协助我根据nebula graph的官方要求写出符合要求的PR。我当前有环境复现，且有现成的脚本可以复现，并且也有gdb或者生成coredump的能力。
2. 我是在nebula graph的3.6版本（release-3.6分支）发现这个问题的，不确定master分支是否存在，请你帮我确认;3、按照nebula graph的官方要求，请阅读我当前的PR，分析需要补充哪些信息，并协助我补充

#压测脚本
我通过脚本压测已复现问题，此时offline storage实例CPU一直占用路90%+，如下是我脚本的实现逻辑
脚本的实现逻辑：我开发了2个脚本，分别并行执行。脚本1为create_space.sh和脚本2位，伪代码如下
```
# create_space.sh
names=("basketballplayer" "basketballplayer1" "basketballplayer2" "basketballplayer3" "basketballplayer4" "basketballplayer5" "basketballplayer6" "basketballplayer7" "basketballplayer8" "basketballplayer9")
function main() {
  while true; do
    for name in "${names[@]}"; do
      # 检查是否有storage实例offline，如果有则脚本退出
      check_node_status
      if [ $? != 0 ]; then
        echo "have some storaged offline"
        exit 0
      fi
      # 创建图空间
      create_space $name
      sleep 5
    done
    sleep 5
    for name in "${names[@]}"; do
      drop_space $name
      sleep 5
    done
    sleep 5
  done
}
```
```
names=("basketballplayer" "basketballplayer1" "basketballplayer2" "basketballplayer3" "basketballplayer4" "basketballplayer5" "basketballplayer6" "basketballplayer7" "basketballplayer8" "basketballplayer9")
function main() {
  while true; do
  # 检查是否有storage实例offline，如果有则脚本退出
  check_node_status
  if [ $? != 0 ]; then
    echo "have some storaged offline"
    exit 0
  fi
  # 创建图空间
  leader_balance
  sleep 10
done
}
```
# 复现定位日志
借助如上脚本我复现了问题，执行`show hosts`发现infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local实例是offline，如下是日志文件末尾10秒的日志
I20260505 17:57:11.530529 31279 RocksEngineConfig.cpp:371] Emplace rocksdb option max_background_jobs=8
I20260505 17:57:11.530623 31279 RocksEngineConfig.cpp:371] Emplace rocksdb option max_subcompactions=8
I20260505 17:57:11.530864 31279 RocksEngineConfig.cpp:371] Emplace rocksdb option max_bytes_for_level_base=268435456
I20260505 17:57:11.530936 31279 RocksEngineConfig.cpp:371] Emplace rocksdb option max_write_buffer_number=8
I20260505 17:57:11.530987 31279 RocksEngineConfig.cpp:371] Emplace rocksdb option write_buffer_size=268435456
I20260505 17:57:11.531322 31279 RocksEngineConfig.cpp:371] Emplace rocksdb option block_size=8192
I20260505 17:57:11.563696 31279 RocksEngine.cpp:107] open rocksdb on /opt/datacube/infinitygraph/data/storage/nebula/42/data
I20260505 17:57:11.564468 30733 NebulaStore.cpp:480] Space 42, part 1 has been added, asLearner 0
I20260505 17:57:11.564867 30733 NebulaStore.cpp:480] Space 42, part 2 has been added, asLearner 0
I20260505 17:57:11.565284 30733 NebulaStore.cpp:480] Space 42, part 3 has been added, asLearner 0
I20260505 17:57:11.565696 30733 NebulaStore.cpp:480] Space 42, part 4 has been added, asLearner 0
I20260505 17:57:11.566102 30733 NebulaStore.cpp:480] Space 42, part 5 has been added, asLearner 0
I20260505 17:57:11.566514 30733 NebulaStore.cpp:480] Space 42, part 6 has been added, asLearner 0
I20260505 17:57:11.566890 30733 NebulaStore.cpp:480] Space 42, part 7 has been added, asLearner 0
I20260505 17:57:11.567301 30733 NebulaStore.cpp:480] Space 42, part 20 has been added, asLearner 0
I20260505 17:57:11.567682 30733 NebulaStore.cpp:480] Space 42, part 19 has been added, asLearner 0
I20260505 17:57:11.568060 30733 NebulaStore.cpp:480] Space 42, part 18 has been added, asLearner 0
I20260505 17:57:11.568465 30733 NebulaStore.cpp:480] Space 42, part 17 has been added, asLearner 0
I20260505 17:57:11.568841 30733 NebulaStore.cpp:480] Space 42, part 16 has been added, asLearner 0
I20260505 17:57:11.569252 30733 NebulaStore.cpp:480] Space 42, part 15 has been added, asLearner 0
I20260505 17:57:11.569643 30733 NebulaStore.cpp:480] Space 42, part 14 has been added, asLearner 0
I20260505 17:57:11.570019 30733 NebulaStore.cpp:480] Space 42, part 13 has been added, asLearner 0
I20260505 17:57:11.570461 30733 NebulaStore.cpp:480] Space 42, part 12 has been added, asLearner 0
I20260505 17:57:11.570919 30733 NebulaStore.cpp:480] Space 42, part 11 has been added, asLearner 0
I20260505 17:57:11.571388 30733 NebulaStore.cpp:480] Space 42, part 10 has been added, asLearner 0
I20260505 17:57:11.571830 30733 NebulaStore.cpp:480] Space 42, part 9 has been added, asLearner 0
I20260505 17:57:11.572314 30733 NebulaStore.cpp:480] Space 42, part 8 has been added, asLearner 0
I20260505 17:57:16.112118 31694 AdminProcessor.h:104] Found new leader of space 32 part 5: "infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:16.113294 31605 AdminProcessor.h:104] Found new leader of space 32 part 7: "infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:16.113402 31518 AdminProcessor.h:104] Found new leader of space 32 part 6: "infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:16.113550 31693 AdminProcessor.h:104] Found new leader of space 32 part 4: "infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.490540 30702 AdminProcessor.h:44] Receive transfer leader for space 35, part 9, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.490653 30711 AdminProcessor.h:44] Receive transfer leader for space 35, part 10, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.490650 30713 AdminProcessor.h:44] Receive transfer leader for space 35, part 6, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.490656 30696 AdminProcessor.h:44] Receive transfer leader for space 35, part 2, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.490715 30704 AdminProcessor.h:44] Receive transfer leader for space 35, part 5, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.490736 30706 AdminProcessor.h:44] Receive transfer leader for space 35, part 1, to [infinitygraph-storaged-0.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.490696 30712 AdminProcessor.h:44] Receive transfer leader for space 35, part 3, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.492533 31328 AdminProcessor.h:115] Can't find leader for space 35 part 9 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.492597 31366 AdminProcessor.h:115] Can't find leader for space 35 part 10 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.492797 31415 AdminProcessor.h:115] Can't find leader for space 35 part 6 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.492906 31453 AdminProcessor.h:115] Can't find leader for space 35 part 2 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.492885 31469 AdminProcessor.h:115] Can't find leader for space 35 part 5 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.492865 31518 AdminProcessor.h:115] Can't find leader for space 35 part 1 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.493072 31556 AdminProcessor.h:115] Can't find leader for space 35 part 3 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.520269 30704 AdminProcessor.h:44] Receive transfer leader for space 38, part 6, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.520291 30706 AdminProcessor.h:44] Receive transfer leader for space 38, part 5, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.520306 30696 AdminProcessor.h:44] Receive transfer leader for space 38, part 7, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.520311 30712 AdminProcessor.h:44] Receive transfer leader for space 38, part 2, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.520365 30711 AdminProcessor.h:44] Receive transfer leader for space 38, part 1, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.520380 30713 AdminProcessor.h:44] Receive transfer leader for space 38, part 4, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.521703 31605 AdminProcessor.h:115] Can't find leader for space 38 part 6 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.521785 31694 AdminProcessor.h:115] Can't find leader for space 38 part 7 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.521766 31693 AdminProcessor.h:115] Can't find leader for space 38 part 5 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.522117 31695 AdminProcessor.h:115] Can't find leader for space 38 part 1 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.522259 31697 AdminProcessor.h:115] Can't find leader for space 38 part 4 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.522255 31696 AdminProcessor.h:115] Can't find leader for space 38 part 2 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.543037 30713 AdminProcessor.h:44] Receive transfer leader for space 40, part 6, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.543151 30711 AdminProcessor.h:44] Receive transfer leader for space 40, part 4, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.543108 30706 AdminProcessor.h:44] Receive transfer leader for space 40, part 10, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.543077 30712 AdminProcessor.h:44] Receive transfer leader for space 40, part 11, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.543177 30696 AdminProcessor.h:44] Receive transfer leader for space 40, part 2, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.544513 31187 AdminProcessor.h:115] Can't find leader for space 40 part 6 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.544713 31236 AdminProcessor.h:115] Can't find leader for space 40 part 10 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.544723 31279 AdminProcessor.h:115] Can't find leader for space 40 part 11 on "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746
I20260505 17:57:21.575894 30704 AdminProcessor.h:44] Receive transfer leader for space 42, part 12, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.575956 30713 AdminProcessor.h:44] Receive transfer leader for space 42, part 5, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.575968 30711 AdminProcessor.h:44] Receive transfer leader for space 42, part 11, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.576045 30696 AdminProcessor.h:44] Receive transfer leader for space 42, part 4, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.576185 30706 AdminProcessor.h:44] Receive transfer leader for space 42, part 1, to [infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local, 26746]
I20260505 17:57:21.676832 30733 MetaClient.cpp:3263] Load leader of "infinitygraph-storaged-0.infinitygraph-storaged-headless.gde.svc.cluster.local":26746 in 10 space
I20260505 17:57:21.677085 30733 MetaClient.cpp:3263] Load leader of "infinitygraph-storaged-1.infinitygraph-storaged-headless.gde.svc.cluster.local":26746 in 6 space
I20260505 17:57:21.677481 30733 MetaClient.cpp:3263] Load leader of "infinitygraph-storaged-2.infinitygraph-storaged-headless.gde.svc.cluster.local":26746 in 10 space
I20260505 17:57:21.677556 30733 MetaClient.cpp:3269] Load leader ok
I20260505 17:57:21.681453 30733 NebulaStore.cpp:417] Create data space 43
# 复现的coredump文件
offline storage实例coredump文件内容在当前工程的bug-fix/core_dump.txt路径下
