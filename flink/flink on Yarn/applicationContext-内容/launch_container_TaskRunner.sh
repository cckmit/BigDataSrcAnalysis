#!/bin/bash


	// lib ($FLINK_CLASSPATH) + $HADOOP_CONF_DIR + $HADOOP_XXX
export CLASSPATH="
:lib/flink-connector-hive_2.11-1.12.1.jar:lib/flink-connector-jdbc_2.11-1.12.2.jar:lib/flink-connector-kafka_2.11-1.12.2.jar
	:lib/flink-connector-mysql-cdc-1.3.0.jar:lib/flink-csv-1.12.2.jar:lib/flink-json-1.12.2.jar:lib/flink-shaded-zookeeper-3.4.14.jar
	:lib/flink-sql-connector-hive-1.2.2_2.11-1.12.2.jar:lib/flink-table-blink_2.11-1.12.2.jar:lib/flink-table_2.11-1.12.2.jar
	:lib/hadoop-common-2.7.2.jar:lib/hadoop-mapreduce-client-app-2.7.2.jar:lib/hadoop-mapreduce-client-common-2.7.2.jar
	:lib/hadoop-mapreduce-client-core-2.7.2.jar:lib/hadoop-mapreduce-client-hs-2.7.2.jar:lib/hadoop-mapreduce-client-hs-plugins-2.7.2.jar
	:lib/hadoop-mapreduce-client-jobclient-2.7.2.jar:lib/hadoop-mapreduce-client-shuffle-2.7.2.jar:lib/hive-exec-1.2.1.jar
	:lib/hudi-flink-bundle_2.11-0.9.0.jar:lib/hudi-hadoop-mr-bundle-0.9.0.jar:lib/kafka-clients-2.4.1.jar:lib/log4j-1.2-api-2.12.1.jar
	:lib/log4j-api-2.12.1.jar:lib/log4j-core-2.12.1.jar:lib/log4j-slf4j-impl-2.12.1.jar

:flink-dist_2.11-1.12.2.jar
:flink-conf.yaml
::$HADOOP_CONF_DIR
:$HADOOP_COMMON_HOME/share/hadoop/common/*:$HADOOP_COMMON_HOME/share/hadoop/common/lib/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/*
	:$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*:$HADOOP_YARN_HOME/share/hadoop/yarn/*
	:$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*
"



exec /bin/bash -c "

$JAVA_HOME/bin/java \
-Xmx697932173 -Xms697932173 -XX:MaxDirectMemorySize=300647712 -XX:MaxMetaspaceSize=268435456 \
	-Dlog.file=/opt/hadoop/hadoop-2.7.2/logs/userlogs/application_1641774141739_0008/container_1641774141739_0008_01_000003/taskmanager.log \
	-Dlog4j.configuration=file:./log4j.properties -Dlog4j.configurationFile=file:./log4j.properties \

org.apache.flink.yarn.YarnTaskExecutorRunner -D taskmanager.memory.framework.off-heap.size=134217728b 

	-D taskmanager.memory.network.max=166429984b -D taskmanager.memory.network.min=166429984b -D taskmanager.memory.framework.heap.size=134217728b \
	-D taskmanager.memory.managed.size=665719939b -D taskmanager.cpu.cores=1.0 -D taskmanager.memory.task.heap.size=563714445b \
	-D taskmanager.memory.task.off-heap.size=0b -D taskmanager.memory.jvm-metaspace.size=268435456b -D taskmanager.memory.jvm-overhead.max=214748368b \
	-D taskmanager.memory.jvm-overhead.min=214748368b --configDir . -Djobmanager.rpc.address='bdnode102.hjq.com' -Djobmanager.memory.jvm-overhead.min='201326592b' \
	-Dtaskmanager.resource-id='container_1641774141739_0008_01_000003' -Dweb.port='0' -Djobmanager.memory.off-heap.size='134217728b' \
	-Dweb.tmpdir='/tmp/flink-web-9bc0a6e5-abb8-425c-9270-8260f4b10307' -Dinternal.taskmanager.resource-id.metadata='bdnode102.hjq.com:46237' \
	-Djobmanager.rpc.port='35263' -Drest.address='bdnode102.hjq.com' -Djobmanager.memory.jvm-metaspace.size='268435456b' -Djobmanager.memory.heap.size='469762048b' \
	-Djobmanager.memory.jvm-overhead.max='201326592b' 

1> /opt/hadoop/hadoop-2.7.2/logs/userlogs/application_1641774141739_0008/container_1641774141739_0008_01_000003/taskmanager.out 
2> /opt/hadoop/hadoop-2.7.2/logs/userlogs/application_1641774141739_0008/container_1641774141739_0008_01_000003/taskmanager.err

"


hadoop_shell_errorcode=$?


if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi


export LOCAL_DIRS="/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008"
export HADOOP_CONF_DIR="/opt/hadoop/etc/hadoop"
export NM_HTTP_PORT="8042"
export JAVA_HOME="/usr/java/jdk-release"
export LOG_DIRS="/opt/hadoop/hadoop-2.7.2/logs/userlogs/application_1641774141739_0008/container_1641774141739_0008_01_000003"
export NM_AUX_SERVICE_mapreduce_shuffle="AAA0+gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=
"
export NM_PORT="46237"
export USER="bigdata"
export HADOOP_YARN_HOME="/opt/hadoop/hadoop-2.7.2"
export CLASSPATH=":lib/flink-connector-hive_2.11-1.12.1.jar:lib/flink-connector-jdbc_2.11-1.12.2.jar:lib/flink-connector-kafka_2.11-1.12.2.jar:lib/flink-connector-mysql-cdc-1.3.0.jar:lib/flink-csv-1.12.2.jar:lib/flink-json-1.12.2.jar:lib/flink-shaded-zookeeper-3.4.14.jar:lib/flink-sql-connector-hive-1.2.2_2.11-1.12.2.jar:lib/flink-table-blink_2.11-1.12.2.jar:lib/flink-table_2.11-1.12.2.jar:lib/hadoop-common-2.7.2.jar:lib/hadoop-mapreduce-client-app-2.7.2.jar:lib/hadoop-mapreduce-client-common-2.7.2.jar:lib/hadoop-mapreduce-client-core-2.7.2.jar:lib/hadoop-mapreduce-client-hs-2.7.2.jar:lib/hadoop-mapreduce-client-hs-plugins-2.7.2.jar:lib/hadoop-mapreduce-client-jobclient-2.7.2.jar:lib/hadoop-mapreduce-client-shuffle-2.7.2.jar:lib/hive-exec-1.2.1.jar:lib/hudi-flink-bundle_2.11-0.9.0.jar:lib/hudi-hadoop-mr-bundle-0.9.0.jar:lib/kafka-clients-2.4.1.jar:lib/log4j-1.2-api-2.12.1.jar:lib/log4j-api-2.12.1.jar:lib/log4j-core-2.12.1.jar:lib/log4j-slf4j-impl-2.12.1.jar:flink-dist_2.11-1.12.2.jar:flink-conf.yaml::$HADOOP_CONF_DIR:$HADOOP_COMMON_HOME/share/hadoop/common/*:$HADOOP_COMMON_HOME/share/hadoop/common/lib/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*:$HADOOP_YARN_HOME/share/hadoop/yarn/*:$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*"
export NM_HOST="bdnode102.hjq.com"
export HADOOP_USER_NAME="bigdata"
export HADOOP_TOKEN_FILE_LOCATION="/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/container_1641774141739_0008_01_000003/container_tokens"
export _FLINK_CLASSPATH=":lib/flink-connector-hive_2.11-1.12.1.jar:lib/flink-connector-jdbc_2.11-1.12.2.jar:lib/flink-connector-kafka_2.11-1.12.2.jar:lib/flink-connector-mysql-cdc-1.3.0.jar:lib/flink-csv-1.12.2.jar:lib/flink-json-1.12.2.jar:lib/flink-shaded-zookeeper-3.4.14.jar:lib/flink-sql-connector-hive-1.2.2_2.11-1.12.2.jar:lib/flink-table-blink_2.11-1.12.2.jar:lib/flink-table_2.11-1.12.2.jar:lib/hadoop-common-2.7.2.jar:lib/hadoop-mapreduce-client-app-2.7.2.jar:lib/hadoop-mapreduce-client-common-2.7.2.jar:lib/hadoop-mapreduce-client-core-2.7.2.jar:lib/hadoop-mapreduce-client-hs-2.7.2.jar:lib/hadoop-mapreduce-client-hs-plugins-2.7.2.jar:lib/hadoop-mapreduce-client-jobclient-2.7.2.jar:lib/hadoop-mapreduce-client-shuffle-2.7.2.jar:lib/hive-exec-1.2.1.jar:lib/hudi-flink-bundle_2.11-0.9.0.jar:lib/hudi-hadoop-mr-bundle-0.9.0.jar:lib/kafka-clients-2.4.1.jar:lib/log4j-1.2-api-2.12.1.jar:lib/log4j-api-2.12.1.jar:lib/log4j-core-2.12.1.jar:lib/log4j-slf4j-impl-2.12.1.jar:flink-dist_2.11-1.12.2.jar:flink-conf.yaml:"
export HADOOP_HDFS_HOME="/opt/hadoop/hadoop-2.7.2"
export LOGNAME="bigdata"
export JVM_PID="$$"
export _FLINK_NODE_ID="bdnode102.hjq.com"
export PWD="/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/container_1641774141739_0008_01_000003"
export HADOOP_COMMON_HOME="/opt/hadoop/hadoop-2.7.2"
export HOME="/home/"
export CONTAINER_ID="container_1641774141739_0008_01_000003"
export MALLOC_ARENA_MAX="4"
mkdir -p plugins/external-resource-gpu
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/42/gpu-discovery-common.sh" "plugins/external-resource-gpu/gpu-discovery-common.sh"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/26/flink-connector-mysql-cdc-1.3.0.jar" "lib/flink-connector-mysql-cdc-1.3.0.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/31/log4j.properties" "log4j.properties"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/49/flink-shaded-zookeeper-3.4.14.jar" "lib/flink-shaded-zookeeper-3.4.14.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/17/hudi-hadoop-mr-bundle-0.9.0.jar" "lib/hudi-hadoop-mr-bundle-0.9.0.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p plugins/external-resource-gpu
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/37/nvidia-gpu-discovery.sh" "plugins/external-resource-gpu/nvidia-gpu-discovery.sh"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/27/flink-dist_2.11-1.12.2.jar" "flink-dist_2.11-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/45/hadoop-mapreduce-client-jobclient-2.7.2.jar" "lib/hadoop-mapreduce-client-jobclient-2.7.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/15/log4j-slf4j-impl-2.12.1.jar" "lib/log4j-slf4j-impl-2.12.1.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p plugins/metrics-influx
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/34/flink-metrics-influxdb-1.12.2.jar" "plugins/metrics-influx/flink-metrics-influxdb-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/28/flink-table-blink_2.11-1.12.2.jar" "lib/flink-table-blink_2.11-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/12/flink-connector-kafka_2.11-1.12.2.jar" "lib/flink-connector-kafka_2.11-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/30/log4j-api-2.12.1.jar" "lib/log4j-api-2.12.1.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/21/hadoop-mapreduce-client-hs-plugins-2.7.2.jar" "lib/hadoop-mapreduce-client-hs-plugins-2.7.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/32/application_1641774141739_0008-flink-conf.yaml6183664449989849342.tmp" "flink-conf.yaml"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p plugins/metrics-prometheus
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/48/flink-metrics-prometheus-1.12.2.jar" "plugins/metrics-prometheus/flink-metrics-prometheus-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/19/hadoop-mapreduce-client-hs-2.7.2.jar" "lib/hadoop-mapreduce-client-hs-2.7.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/39/hadoop-common-2.7.2.jar" "lib/hadoop-common-2.7.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p plugins/metrics-slf4j
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/11/flink-metrics-slf4j-1.12.2.jar" "plugins/metrics-slf4j/flink-metrics-slf4j-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p plugins
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/43/README.txt" "plugins/README.txt"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p plugins/metrics-datadog
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/13/flink-metrics-datadog-1.12.2.jar" "plugins/metrics-datadog/flink-metrics-datadog-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/29/log4j-1.2-api-2.12.1.jar" "lib/log4j-1.2-api-2.12.1.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/47/hadoop-mapreduce-client-core-2.7.2.jar" "lib/hadoop-mapreduce-client-core-2.7.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/35/hadoop-mapreduce-client-app-2.7.2.jar" "lib/hadoop-mapreduce-client-app-2.7.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/14/flink-connector-hive_2.11-1.12.1.jar" "lib/flink-connector-hive_2.11-1.12.1.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/36/hadoop-mapreduce-client-common-2.7.2.jar" "lib/hadoop-mapreduce-client-common-2.7.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/44/hive-exec-1.2.1.jar" "lib/hive-exec-1.2.1.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/33/flink-table_2.11-1.12.2.jar" "lib/flink-table_2.11-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/20/log4j-core-2.12.1.jar" "lib/log4j-core-2.12.1.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/23/flink-csv-1.12.2.jar" "lib/flink-csv-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/24/flink-connector-jdbc_2.11-1.12.2.jar" "lib/flink-connector-jdbc_2.11-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/25/hadoop-mapreduce-client-shuffle-2.7.2.jar" "lib/hadoop-mapreduce-client-shuffle-2.7.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p plugins/metrics-statsd
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/16/flink-metrics-statsd-1.12.2.jar" "plugins/metrics-statsd/flink-metrics-statsd-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/10/flink-sql-connector-hive-1.2.2_2.11-1.12.2.jar" "lib/flink-sql-connector-hive-1.2.2_2.11-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p plugins/external-resource-gpu
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/46/flink-external-resource-gpu-1.12.2.jar" "plugins/external-resource-gpu/flink-external-resource-gpu-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/38/kafka-clients-2.4.1.jar" "lib/kafka-clients-2.4.1.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/22/hudi-flink-bundle_2.11-0.9.0.jar" "lib/hudi-flink-bundle_2.11-0.9.0.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p plugins/metrics-graphite
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/40/flink-metrics-graphite-1.12.2.jar" "plugins/metrics-graphite/flink-metrics-graphite-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p plugins/metrics-jmx
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/18/flink-metrics-jmx-1.12.2.jar" "plugins/metrics-jmx/flink-metrics-jmx-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0008/filecache/41/flink-json-1.12.2.jar" "lib/flink-json-1.12.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
