# Enable AutoFDO for NebulaGraph

This document will help you to enable the AutoFDO for NebulaGraph.
More background about the AutoFDO, please refer [AutoFDO Wiki](https://gcc.gnu.org/wiki/AutoFDO)

## Resource Preparations
### Install Dependencies
#### Install perf
```bash
sudo apt-get update
sudo apt-get install -y linux-tools-common \
linux-tools-generic \
linux-tools-`uname -r`
```
#### Install autofdo tool

```bash
sudo apt-get update
sudo apt-get install -y autofdo
```

Or you can compile the ***autofdo tool*** from [source](https://github.com/google/autofdo.git).

### NebulaGraph Binary with Debug Version

For how to build NebulaGraph from source, please refer to the official document: [Install NebulaGraph by compiling the source code](https://docs.nebula-graph.io/3.3.0/4.deployment-and-installation/2.compile-and-install-nebula-graph/1.install-nebula-graph-by-compiling-the-source-code/).
In the configure step, replace `CMAKE_BUILD_TYPE=Release` with `CMAKE_BUILD_TYPE=Debug` as below:
```bash
$ cmake -DCMAKE_INSTALL_PREFIX=/usr/local/nebula -DENABLE_TESTING=OFF -DCMAKE_BUILD_TYPE=Debug ..
```
Or manually add the dbeug symbol by add compile option `-g` like below.
```diff
diff --git a/cmake/nebula/GeneralCompilerConfig.cmake b/cmake/nebula/GeneralCompilerConfig.cmake
@@ -20,6 +20,8 @@ add_compile_options(-Wshadow)
 add_compile_options(-Wnon-virtual-dtor)
 add_compile_options(-Woverloaded-virtual)
 add_compile_options(-Wignored-qualifiers)
+add_compile_options(-g)
```

## Prepare Test Data

In our test environment, we use [NebulaGraph Bench](https://github.com/nebula-contrib/NebulaGraph-Bench) to prepare the test data and collect the profile data by running the ***FindShortestPath***, ***Go1Step***, ***Go2Step***, ***Go3Step***, ***InsertPersonScenario*** 5 scenarios. 
>**Note:** You can use your ***TopN*** queries in your production environment to collect the profile data, the performance can gain more in your environment.
## Prepare Profile Data
### Collect Perf Data For AutoFdo Tool
After the test data preparation work done. Collect the perf data for different scenarios.
Get the pid of `storaged`, `graphd`, `metad`.
```bash
$ nebula.service status all
[INFO] nebula-metad: Running as 305422, Listening on 9559
[INFO] nebula-graphd: Running as 305516, Listening on 9669
[INFO] nebula-storaged: Running as 305707, Listening on 9779
```
Start the ***perf record*** for *nebula-graphd* and *nebula-storaged*. 
```bash
perf record -p 305516,305707 -b -e br_inst_retired.near_taken:pp -o ~/FindShortestPath.data
```

***Note:*** Because the `nebula-metad` service contribution percent is small compared with `nebula-graphd` and `nebula-storaged` services. To reduce effort, we didn't collect the perf data for `nebula-metad` service.
Start the benchmark test for ***FindShortestPath*** scenario.

```bash
cd NebulaGraph-Bench 
python3 run.py stress run -s benchmark -scenario find_path.FindShortestPath -a localhost:9669 --args='-u 100 -i 100000'
```

After the benchmark finished, end the ***perf record*** by ***Ctrl + c***.
Repeat above steps to collect corresponding profile data for the rest  ***Go1Step***, ***Go2Step***, ***Go3Step*** and ***InsertPersonScenario*** scenarios.

### Create Gcov File

```bash
create_gcov --binary=$NEBULA_HOME/bin/nebula-storaged \
--profile=~/FindShortestPath.data \
--gcov=~/FindShortestPath-storaged.gcov \
-gcov_version=1

create_gcov --binary=$NEBULA_HOME/bin/nebula-graphd \
--profile=~/FindShortestPath.data \
--gcov=~/FindShortestPath-graphd.gcov \
-gcov_version=1
```

Repeat for ***Go1Step***, ***Go2Step***, ***Go3Step*** and ***InsertPersonScenario*** scenarios.

### Merge the Profile Data

```bash
profile_merger ~/FindShortestPath-graphd.gcov \
~/FindShortestPath-storaged.gcov \
~/go1step-storaged.gcov \
~/go1step-graphd.gcov \
~/go2step-storaged.gcov \
~/go2step-graphd.gcov \
~/go3step-storaged.gcov \
~/go3step-master-graphd.gcov \
~/InsertPersonScenario-storaged.gcov \
~/InsertPersonScenario-graphd.gcov
```
You will get a merged profile which is named `fbdata.afdo` after that.

## Recompile GraphNebula Binary with the Merged Profile

Recompile the GraphNebula Binary by passing the profile with compile option `-fauto-profile`
```diff
diff --git a/cmake/nebula/GeneralCompilerConfig.cmake b/cmake/nebula/GeneralCompilerConfig.cmake
@@ -20,6 +20,8 @@ add_compile_options(-Wshadow)
 add_compile_options(-Wnon-virtual-dtor)
 add_compile_options(-Woverloaded-virtual)
 add_compile_options(-Wignored-qualifiers)
+add_compile_options(-fauto-profile=~/fbdata.afdo)
```

## Performance Test Result

### Hardware & Software Environment

|Key|Value|
|:---|---:|
|CPU Processor#|2|
|Sockets|2|
|NUMA|2|
|CPU Type|Intel(R) Xeon(R) Platinum 8380 CPU @ 2.30GHz|
|Cores per Processor|40C80T|
|Cache|L1 data: 48KB L1 i: 32KB  L2: 1.25MB per physical core  L3: shared 60MB per processor|
|Memory|Micron DDR4 3200MT/s 16GB*16Micron DDR4 3200MT/s 16GB*16|
|SSD Disk|INTEL SSDPE2KE016T8|
|SSD R/W Sequential|3200 MB/s (read) / 2100 MB/s(write)|
|Nebula Version|master with commit id 51d84a4ed7d2a032a337e3b996c927e3bc5d1415|
|Kernel|4.18.0-408.el8.x86_64|


### Test Results

|Scenario|Average Latency(LiB)|Default Binary|Optimized Binary with AutoFDO|P95 Latency (LiB)|Default Binary|Optimized Binary with AutoFDO|
|:---|:---:|:---:|:---:|:---:|:---:|---:|
|***FindShortestPath***|**1**|8072.52|7260.10|**1**|22102.00|19108.00|
||**2**|8034.32|7218.59|**2**|22060.85|19006.00|
||**3**|8079.27|7257.24|**3**|22147.00|19053.00|
||**4**|8087.66|7221.39|**4**|22143.00|19050.00|
||**5**|8044.77|7239.85|**5**|22181.00|19055.00|
||**STDDEVP**|20.57|17.34|**STDDEVP**|41.41|32.36|
||**Mean**|8063.71|7239.43|**Mean**|22126.77|19054.40|
||**STDDEVP/Mean**|0.26%|0.24%|**STDDEVP/Mean**|0.19%|0.17%|
||**Opt/Default**|100.00%|***10.22%***|**Opt/Default**|100.00%|***13.89%***|
|***Go1Step***|**1**|422.53|418.37|**1**|838.00|850.00|
||**2**|432.37|402.44|**2**|866.00|815.00|
||**3**|437.45|407.98|**3**|874.00|836.00|
||**4**|429.16|408.38|**4**|858.00|838.00|
||**5**|446.38|411.32|**5**|901.00|837.00|
||**STDDEVP**|8.02|5.20|**STDDEVP**|20.63|11.30|
||**Mean**|433.58|409.70|**Mean**|867.40|835.20|
||**STDDEVP/Mean**|1.85%|1.27%|**STDDEVP/Mean**|2.38%|1.35%|
||**Opt/Default**|100.00%|***5.51%***|**Opt/Default**|100.00%|***3.71%***|
|***Go2Step***|**1**|2989.93|2824.29|**1**|10202.00|9656.95|
||**2**|2957.22|2834.55|**2**|10129.00|9632.40|
||**3**|2962.74|2818.62|**3**|10168.40|9624.70|
||**4**|2992.39|2817.27|**4**|10285.10|9647.50|
||**5**|2934.85|2834.91|**5**|10025.00|9699.65|
||**STDDEVP**|21.53|7.57|**STDDEVP**|85.62|26.25|
||**Mean**|2967.43|2825.93|**Mean**|10161.90|9652.24|
||**STDDEVP/Mean**|0.73%|0.27%|**STDDEVP/Mean**|0.84%|0.27%|
||**Opt/Default**|100.00%|***4.77%***|**Opt/Default**|100.00%|***5.02%***|
|***Go3Step***|**1**|93551.97|89406.96|**1**|371359.55|345433.50|
||**2**|92418.43|89977.25|**2**|368868.00|352375.20|
||**3**|92587.67|90339.25|**3**|365390.15|356198.55|
||**4**|93371.64|92458.95|**4**|373578.15|365177.75|
||**5**|94046.05|89943.44|**5**|373392.25|352576.00|
||**STDDEVP**|609.07|1059.54|**STDDEVP**|3077.38|6437.52|
||**Mean**|93195.15|90425.17|**Mean**|370517.62|354352.20|
||**STDDEVP/Mean**|0.65%|1.17%|**STDDEVP/Mean**|0.83%|1.82%|
||**Opt/Default**|100.00%|***2.97%***|**Opt/Default**|100.00%|***4.36%***|
|***InsertPerson***|**1**|2022.86|1937.36|**1**|2689.00|2633.45|
||**2**|1966.05|1935.41|**2**|2620.45|2555.00|
||**3**|1985.25|1953.58|**3**|2546.00|2593.00|
||**4**|2026.73|1887.28|**4**|2564.00|2394.00|
||**5**|2007.55|1964.41|**5**|2676.00|2581.00|
||**STDDEVP**|23.02|26.42|**STDDEVP**|57.45|82.62|
||**Mean**|2001.69|1935.61|**Mean**|2619.09|2551.29|
||**STDDEVP/Mean**|1.15%|1.37%|**STDDEVP/Mean**|2.19%|3.24%|
||**Opt/Default**|100.00%|***3.30%***|**Opt/Default**|100.00%|***2.59%***|
