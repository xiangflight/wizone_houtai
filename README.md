# wizone_houtai
wibupt后台的数据处理代码

### 注意点

1. 每隔一天运行wibupt_everyday脚本，该脚本的内容如下，运行了TotalInfo和BrandStat两个进程。

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
* BrandStat.jar

2. 每隔30分钟运行一次gephi脚本，该脚本内容如下，运行了TraceMap进程。

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

3. 有两个常驻进程，分别是RealTime和VisitRecord。

**作用**

* RealTime.jar
* VisitRecord.jar

4. 还有一个进程， RealGate

**作用**

### 后台进程 --- 数据库 --- 网页 的对应关系
