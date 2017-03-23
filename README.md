# wizone_houtai
wibupt后台的数据处理代码

### 详细介绍

#### 1. 每隔一天运行wibupt_everyday脚本，该脚本的内容如下，运行了TotalInfo和BrandStat两个进程。

```
#!/bin/sh
#
#description: run gettotal everyday
#processname: gettotal

java -jar /home/wibupt/TotalInfo.jar root root /home/data/scandata yesterday &
java -jar /home/wibupt/BrandStat.jar root root /home/data/scandata yesterday &

exit 0
```

**作用**

* TotalInfo.jar

1. 与表totalinfo有关，统计每一个监测群有多少个手机mac，表totalinfo统计的是每天各个groupid监测到的手机数量；
2. 与表activity有关，将星期几、时间和计算出的活跃度插入到表activity；
3. 与表activityinday有关，将每个小时所有监测点的活跃度都插入到表activityinday；
4. 与表goandcome有关，统计每天每个门的出入人流量;

* BrandStat.jar

统计各品牌手机的数量分布，它关联的是数据库中的branddis表，branddis表中统计的是每天监测到的各品牌手机数量，在网站上体现为Consumption.jsp。

#### 2. 每隔30分钟运行一次gephi脚本，该脚本内容如下，运行了TraceMap进程。

```
#!/bin/bash
RUN_HOME=/home/wibupt/
CPATH=$CPATH:$RUN_HOME/gephi-toolkit.jar
CPATH=$CPATH:$RUN_HOME/log4j-api-2.0.2.jar
CPATH=$CPATH:$RUN_HOME/log4j-core-2.0.2.jar
CPATH=$CPATH:$RUN_HOME/mysql-connector-java-5.1.23.jar

date1=$(date +%Y%m%d_%H%M%S)
filename=/home/wibupt/tomcat6/webapps/wizone/wibupt/img/
time=$(date +%H:%M:%S)
today=$(date +%Y%m%d)

export CPATH=$CPATH

java -jar /home/wibupt/TraceMap.jar root root /home/data/scandata/ $filename $today $time 30
```

**作用**

* TraceMap.jar

与表edges有关，生成svg图，与之关联的是gephi.jsp页面

#### 3. 有两个常驻进程，分别是RealTime和VisitRecord。

**作用**

* RealTime.jar



* VisitRecord.jar

4. 还有一个进程， RealGate

**作用**

### 后台进程 --- 数据库 --- 网页 的对应关系
