# wizone_houtai

### 详细介绍

****

#### 运行

使用Linux的cron机制定时循环运行这些jar包，具体如下：

```
$crontab -l 

00 01 * * * /home/wibupt/wibupt_everyday
1,6,11,16,21,26,31,36,41,46,51,56 * * * * /home/wibupt/wirealtime
2,7,12,17,22,27,32,37,42,47,52,57 * * * * /home/wibupt/visitrecord
3,8,13,18,23,28,33,38,43,48,53,58 * * * * /home/wibupt/realgate
*/30 * * * * /home/wibupt/gephi

```

****

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

1. 与表**totalinfo**有关，统计每一个监测组有多少个手机mac，表totalinfo统计的是每天各个groupid监测到的手机数量；
2. 与表**activity**有关，将星期几、时间和计算出的活跃度插入到表activity；
3. 与表**activityinday**有关，将每个小时所有监测点的活跃度都插入到表activityinday；
4. 与表**goandcome**有关，统计每天每个门的出入人流量;

* BrandStat.jar

- 统计各品牌手机的数量分布，它关联的是数据库中的**branddis**表，branddis表中统计的是每天监测到的各品牌手机数量。

****

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

1. 与表**edges**有关，生成svg图

****

#### 3. 每5分钟运行一次realtime, visitrecord和realgate脚本，分别运行RealTime、VisitRecord和RealGate三个进程

```
wirealtime

#!/bin/sh
#
# description: Auto-starts dataimport
# processname: dataimport

java -jar /home/wibupt/RealTime.jar root root /home/data/scandata 300 today &
exit 0

------------------------------------------------------------------------
visitrecord

#!/bin/sh
#
# description: Auto-starts dataimport
# processname: dwelltime


java -jar /home/wibupt/VisitRecord.jar root root /home/data/scandata 1800 today &
exit 0

------------------------------------------------------------------------
realgate

#!/bin/bash
RUN_HOME=/home/wibupt/
CPATH1=$CPATH1:$RUN_HOME/mysql-connector-java-5.1.23.jar

date1=$(date +%Y%m%d)
time=$(date +%H:%M:%S)

export CPATH1=$CPATH1

java -jar /home/wibupt/RealGate.jar root root $date1 $time

```

**作用**

* RealTime.jar

1. 与表realtimedata_in有关，统计每个分组5分钟内的流量；
2. 与表heatmap有关，统计每个分组5分钟内的流量

* VisitRecord.jar
1. 每隔5分钟统计一次数据
2. 与表visitrecord有关，统计的数据最全面，统计了每个手机接入监测点的时间，离开时间，逗留时间及手机的Mac地址，接入的监测点的Mac地址

* RealGate.jar

- 与表realgate有关，统计的是5分钟内各个门的人流量；

****

### 后台进程 --- 数据库 --- 网页 的对应关系

|后台进程      |      数据库表   |    网页   |
|---------     | -----------     | -----------|
| TotalInfo    | totalinfo, activity, activityinday, goandcome | activity.jsp, goandcome.jsp |
| BrandStat    | branddis                                      | consumption.jsp             |
| TraceMap     | edges                                         | gephi.jsp                   |
| Realtime     | realtimedata_in, heatmap                      | heatmap.jsp                 |
| VisitRecord  | visitrecord                                   | none                        |
| RealGate     | realgate                                      | goandcome.jsp               |