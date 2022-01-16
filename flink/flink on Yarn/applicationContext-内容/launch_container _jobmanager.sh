#!/bin/bash

export HADOOP_CONF_DIR="/opt/hadoop/etc/hadoop"
export MAX_APP_ATTEMPTS="1"
export JAVA_HOME="/usr/java/jdk-release"
export _CLIENT_HOME_DIR="hdfs://bdnode102:9000/user/bigdata"
export APP_SUBMIT_TIME_ENV="1641873125742"
export NM_HOST="bdnode102.hjq.com"
export _APP_ID="application_1641774141739_0006"
export HADOOP_USER_NAME="bigdata"
export HADOOP_HDFS_HOME="/opt/hadoop/hadoop-2.7.2"
export LOGNAME="bigdata"
export JVM_PID="$$"
export PWD="/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/container_1641774141739_0006_01_000001"
export HADOOP_COMMON_HOME="/opt/hadoop/hadoop-2.7.2"
export LOCAL_DIRS="/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006"
export APPLICATION_WEB_PROXY_BASE="/proxy/application_1641774141739_0006"
export NM_HTTP_PORT="8042"
export LOG_DIRS="/opt/hadoop/hadoop-2.7.2/logs/userlogs/application_1641774141739_0006/container_1641774141739_0006_01_000001"
export NM_AUX_SERVICE_mapreduce_shuffle="AAA0+gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=
"
export _CLIENT_SHIP_FILES="YarnLocalResourceDescriptor{key=lib/flink-table-blink_2.11-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/flink-table-blink_2.11-1.12.2.jar, size=40316352, modificationTime=1641873123720, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-table_2.11-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/flink-table_2.11-1.12.2.jar, size=36149872, modificationTime=1641873123812, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/log4j-1.2-api-2.12.1.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/log4j-1.2-api-2.12.1.jar, size=67114, modificationTime=1641873123820, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/log4j-slf4j-impl-2.12.1.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/log4j-slf4j-impl-2.12.1.jar, size=23518, modificationTime=1641873123827, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-json-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/flink-json-1.12.2.jar, size=137023, modificationTime=1641873123836, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-shaded-zookeeper-3.4.14.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/flink-shaded-zookeeper-3.4.14.jar, size=7709741, modificationTime=1641873123852, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-csv-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/flink-csv-1.12.2.jar, size=91745, modificationTime=1641873123859, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/log4j-api-2.12.1.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/log4j-api-2.12.1.jar, size=276771, modificationTime=1641873123872, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/log4j-core-2.12.1.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/log4j-core-2.12.1.jar, size=1674433, modificationTime=1641873123880, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/hive-exec-1.2.1.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/hive-exec-1.2.1.jar, size=20599030, modificationTime=1641873123915, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-connector-hive_2.11-1.12.1.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/flink-connector-hive_2.11-1.12.1.jar, size=6321871, modificationTime=1641873124343, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-sql-connector-hive-1.2.2_2.11-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/flink-sql-connector-hive-1.2.2_2.11-1.12.2.jar, size=34412712, modificationTime=1641873124404, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/hadoop-common-2.7.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/hadoop-common-2.7.2.jar, size=3440968, modificationTime=1641873124417, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/hadoop-mapreduce-client-app-2.7.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/hadoop-mapreduce-client-app-2.7.2.jar, size=513088, modificationTime=1641873124425, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/hadoop-mapreduce-client-common-2.7.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/hadoop-mapreduce-client-common-2.7.2.jar, size=751718, modificationTime=1641873124835, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/hadoop-mapreduce-client-core-2.7.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/hadoop-mapreduce-client-core-2.7.2.jar, size=1531757, modificationTime=1641873124847, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/hadoop-mapreduce-client-hs-2.7.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/hadoop-mapreduce-client-hs-2.7.2.jar, size=163774, modificationTime=1641873124858, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/hadoop-mapreduce-client-hs-plugins-2.7.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/hadoop-mapreduce-client-hs-plugins-2.7.2.jar, size=4136, modificationTime=1641873124864, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/hadoop-mapreduce-client-jobclient-2.7.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/hadoop-mapreduce-client-jobclient-2.7.2.jar, size=37638, modificationTime=1641873124874, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/hadoop-mapreduce-client-shuffle-2.7.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/hadoop-mapreduce-client-shuffle-2.7.2.jar, size=47854, modificationTime=1641873124883, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-connector-kafka_2.11-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/flink-connector-kafka_2.11-1.12.2.jar, size=335045, modificationTime=1641873124893, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/kafka-clients-2.4.1.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/kafka-clients-2.4.1.jar, size=3269712, modificationTime=1641873124902, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-connector-mysql-cdc-1.3.0.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/flink-connector-mysql-cdc-1.3.0.jar, size=27136849, modificationTime=1641873124947, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-connector-jdbc_2.11-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/flink-connector-jdbc_2.11-1.12.2.jar, size=197346, modificationTime=1641873125360, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/hudi-flink-bundle_2.11-0.9.0.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/hudi-flink-bundle_2.11-0.9.0.jar, size=52107314, modificationTime=1641873125442, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/hudi-hadoop-mr-bundle-0.9.0.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/lib/hudi-hadoop-mr-bundle-0.9.0.jar, size=17381995, modificationTime=1641873125473, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=log4j.properties, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/log4j.properties, size=2620, modificationTime=1641873125480, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/metrics-statsd/flink-metrics-statsd-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/plugins/metrics-statsd/flink-metrics-statsd-1.12.2.jar, size=13799, modificationTime=1641873125492, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/metrics-datadog/flink-metrics-datadog-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/plugins/metrics-datadog/flink-metrics-datadog-1.12.2.jar, size=504800, modificationTime=1641873125499, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/external-resource-gpu/gpu-discovery-common.sh, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/plugins/external-resource-gpu/gpu-discovery-common.sh, size=3189, modificationTime=1641873125504, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/external-resource-gpu/nvidia-gpu-discovery.sh, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/plugins/external-resource-gpu/nvidia-gpu-discovery.sh, size=1794, modificationTime=1641873125510, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/external-resource-gpu/flink-external-resource-gpu-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/plugins/external-resource-gpu/flink-external-resource-gpu-1.12.2.jar, size=17586, modificationTime=1641873125523, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/metrics-influx/flink-metrics-influxdb-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/plugins/metrics-influx/flink-metrics-influxdb-1.12.2.jar, size=992689, modificationTime=1641873125531, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/metrics-prometheus/flink-metrics-prometheus-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/plugins/metrics-prometheus/flink-metrics-prometheus-1.12.2.jar, size=108251, modificationTime=1641873125537, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/metrics-graphite/flink-metrics-graphite-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/plugins/metrics-graphite/flink-metrics-graphite-1.12.2.jar, size=181513, modificationTime=1641873125542, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/README.txt, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/plugins/README.txt, size=654, modificationTime=1641873125548, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/metrics-slf4j/flink-metrics-slf4j-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/plugins/metrics-slf4j/flink-metrics-slf4j-1.12.2.jar, size=11922, modificationTime=1641873125553, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=plugins/metrics-jmx/flink-metrics-jmx-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/plugins/metrics-jmx/flink-metrics-jmx-1.12.2.jar, size=19898, modificationTime=1641873125558, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=flink-conf.yaml, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/application_1641774141739_0006-flink-conf.yaml8729533049942737551.tmp, size=910, modificationTime=1641873125725, visibility=APPLICATION, type=FILE}"
export NM_PORT="46237"
export USER="bigdata"
export HADOOP_YARN_HOME="/opt/hadoop/hadoop-2.7.2"
export CLASSPATH=":lib/flink-connector-hive_2.11-1.12.1.jar:lib/flink-connector-jdbc_2.11-1.12.2.jar:lib/flink-connector-kafka_2.11-1.12.2.jar:lib/flink-connector-mysql-cdc-1.3.0.jar:lib/flink-csv-1.12.2.jar:lib/flink-json-1.12.2.jar:lib/flink-shaded-zookeeper-3.4.14.jar:lib/flink-sql-connector-hive-1.2.2_2.11-1.12.2.jar:lib/flink-table-blink_2.11-1.12.2.jar:lib/flink-table_2.11-1.12.2.jar:lib/hadoop-common-2.7.2.jar:lib/hadoop-mapreduce-client-app-2.7.2.jar:lib/hadoop-mapreduce-client-common-2.7.2.jar:lib/hadoop-mapreduce-client-core-2.7.2.jar:lib/hadoop-mapreduce-client-hs-2.7.2.jar:lib/hadoop-mapreduce-client-hs-plugins-2.7.2.jar:lib/hadoop-mapreduce-client-jobclient-2.7.2.jar:lib/hadoop-mapreduce-client-shuffle-2.7.2.jar:lib/hive-exec-1.2.1.jar:lib/hudi-flink-bundle_2.11-0.9.0.jar:lib/hudi-hadoop-mr-bundle-0.9.0.jar:lib/kafka-clients-2.4.1.jar:lib/log4j-1.2-api-2.12.1.jar:lib/log4j-api-2.12.1.jar:lib/log4j-core-2.12.1.jar:lib/log4j-slf4j-impl-2.12.1.jar:flink-dist_2.11-1.12.2.jar:flink-conf.yaml::$HADOOP_CONF_DIR:$HADOOP_COMMON_HOME/share/hadoop/common/*:$HADOOP_COMMON_HOME/share/hadoop/common/lib/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*:$HADOOP_YARN_HOME/share/hadoop/yarn/*:$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*"
export _FLINK_YARN_FILES="hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006"
export HADOOP_TOKEN_FILE_LOCATION="/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/container_1641774141739_0006_01_000001/container_tokens"
export _FLINK_CLASSPATH=":lib/flink-connector-hive_2.11-1.12.1.jar:lib/flink-connector-jdbc_2.11-1.12.2.jar:lib/flink-connector-kafka_2.11-1.12.2.jar:lib/flink-connector-mysql-cdc-1.3.0.jar:lib/flink-csv-1.12.2.jar:lib/flink-json-1.12.2.jar:lib/flink-shaded-zookeeper-3.4.14.jar:lib/flink-sql-connector-hive-1.2.2_2.11-1.12.2.jar:lib/flink-table-blink_2.11-1.12.2.jar:lib/flink-table_2.11-1.12.2.jar:lib/hadoop-common-2.7.2.jar:lib/hadoop-mapreduce-client-app-2.7.2.jar:lib/hadoop-mapreduce-client-common-2.7.2.jar:lib/hadoop-mapreduce-client-core-2.7.2.jar:lib/hadoop-mapreduce-client-hs-2.7.2.jar:lib/hadoop-mapreduce-client-hs-plugins-2.7.2.jar:lib/hadoop-mapreduce-client-jobclient-2.7.2.jar:lib/hadoop-mapreduce-client-shuffle-2.7.2.jar:lib/hive-exec-1.2.1.jar:lib/hudi-flink-bundle_2.11-0.9.0.jar:lib/hudi-hadoop-mr-bundle-0.9.0.jar:lib/kafka-clients-2.4.1.jar:lib/log4j-1.2-api-2.12.1.jar:lib/log4j-api-2.12.1.jar:lib/log4j-core-2.12.1.jar:lib/log4j-slf4j-impl-2.12.1.jar:flink-dist_2.11-1.12.2.jar:flink-conf.yaml:"
export _FLINK_DIST_JAR="YarnLocalResourceDescriptor{key=flink-dist_2.11-1.12.2.jar, path=hdfs://bdnode102:9000/user/bigdata/.flink/application_1641774141739_0006/flink-dist_2.11-1.12.2.jar, size=114224188, modificationTime=1641873125712, visibility=APPLICATION, type=FILE}"
export HOME="/home/"
export CONTAINER_ID="container_1641774141739_0006_01_000001"
export MALLOC_ARENA_MAX="4"
mkdir -p lib
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/24/flink-connector-jdbc_2.11-1.12.2.jar" "lib/flink-connector-jdbc_2.11-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/41/flink-json-1.12.2.jar" "lib/flink-json-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/21/hadoop-mapreduce-client-hs-plugins-2.7.2.jar" "lib/hadoop-mapreduce-client-hs-plugins-2.7.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/32/application_1641774141739_0006-flink-conf.yaml8729533049942737551.tmp" "flink-conf.yaml"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/27/flink-dist_2.11-1.12.2.jar" "flink-dist_2.11-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/34/flink-metrics-influxdb-1.12.2.jar" "plugins/metrics-influx/flink-metrics-influxdb-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/36/hadoop-mapreduce-client-common-2.7.2.jar" "lib/hadoop-mapreduce-client-common-2.7.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/11/flink-metrics-slf4j-1.12.2.jar" "plugins/metrics-slf4j/flink-metrics-slf4j-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/45/hadoop-mapreduce-client-jobclient-2.7.2.jar" "lib/hadoop-mapreduce-client-jobclient-2.7.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/15/log4j-slf4j-impl-2.12.1.jar" "lib/log4j-slf4j-impl-2.12.1.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/28/flink-table-blink_2.11-1.12.2.jar" "lib/flink-table-blink_2.11-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/38/kafka-clients-2.4.1.jar" "lib/kafka-clients-2.4.1.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/37/nvidia-gpu-discovery.sh" "plugins/external-resource-gpu/nvidia-gpu-discovery.sh"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/40/flink-metrics-graphite-1.12.2.jar" "plugins/metrics-graphite/flink-metrics-graphite-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/49/flink-shaded-zookeeper-3.4.14.jar" "lib/flink-shaded-zookeeper-3.4.14.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/48/flink-metrics-prometheus-1.12.2.jar" "plugins/metrics-prometheus/flink-metrics-prometheus-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/35/hadoop-mapreduce-client-app-2.7.2.jar" "lib/hadoop-mapreduce-client-app-2.7.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/44/hive-exec-1.2.1.jar" "lib/hive-exec-1.2.1.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/20/log4j-core-2.12.1.jar" "lib/log4j-core-2.12.1.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/46/flink-external-resource-gpu-1.12.2.jar" "plugins/external-resource-gpu/flink-external-resource-gpu-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/29/log4j-1.2-api-2.12.1.jar" "lib/log4j-1.2-api-2.12.1.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/13/flink-metrics-datadog-1.12.2.jar" "plugins/metrics-datadog/flink-metrics-datadog-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/17/hudi-hadoop-mr-bundle-0.9.0.jar" "lib/hudi-hadoop-mr-bundle-0.9.0.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/26/flink-connector-mysql-cdc-1.3.0.jar" "lib/flink-connector-mysql-cdc-1.3.0.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/33/flink-table_2.11-1.12.2.jar" "lib/flink-table_2.11-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/12/flink-connector-kafka_2.11-1.12.2.jar" "lib/flink-connector-kafka_2.11-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/47/hadoop-mapreduce-client-core-2.7.2.jar" "lib/hadoop-mapreduce-client-core-2.7.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/25/hadoop-mapreduce-client-shuffle-2.7.2.jar" "lib/hadoop-mapreduce-client-shuffle-2.7.2.jar"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/31/log4j.properties" "log4j.properties"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/39/hadoop-common-2.7.2.jar" "lib/hadoop-common-2.7.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/10/flink-sql-connector-hive-1.2.2_2.11-1.12.2.jar" "lib/flink-sql-connector-hive-1.2.2_2.11-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/42/gpu-discovery-common.sh" "plugins/external-resource-gpu/gpu-discovery-common.sh"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/14/flink-connector-hive_2.11-1.12.1.jar" "lib/flink-connector-hive_2.11-1.12.1.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/18/flink-metrics-jmx-1.12.2.jar" "plugins/metrics-jmx/flink-metrics-jmx-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/19/hadoop-mapreduce-client-hs-2.7.2.jar" "lib/hadoop-mapreduce-client-hs-2.7.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/30/log4j-api-2.12.1.jar" "lib/log4j-api-2.12.1.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/23/flink-csv-1.12.2.jar" "lib/flink-csv-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/16/flink-metrics-statsd-1.12.2.jar" "plugins/metrics-statsd/flink-metrics-statsd-1.12.2.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/22/hudi-flink-bundle_2.11-0.9.0.jar" "lib/hudi-flink-bundle_2.11-0.9.0.jar"
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
ln -sf "/opt/hadoop/tmpDir/nm-local-dir/usercache/bigdata/appcache/application_1641774141739_0006/filecache/43/README.txt" "plugins/README.txt"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
exec /bin/bash -c "$JAVA_HOME/bin/java -Xmx469762048 -Xms469762048 -XX:MaxMetaspaceSize=268435456 "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=42040" -Dlog.file="/opt/hadoop/hadoop-2.7.2/logs/userlogs/application_1641774141739_0006/container_1641774141739_0006_01_000001/jobmanager.log" -Dlog4j.configuration=file:log4j.properties -Dlog4j.configurationFile=file:log4j.properties org.apache.flink.yarn.entrypoint.YarnSessionClusterEntrypoint -D jobmanager.memory.off-heap.size=134217728b -D jobmanager.memory.jvm-overhead.min=201326592b -D jobmanager.memory.jvm-metaspace.size=268435456b -D jobmanager.memory.heap.size=469762048b -D jobmanager.memory.jvm-overhead.max=201326592b 1> /opt/hadoop/hadoop-2.7.2/logs/userlogs/application_1641774141739_0006/container_1641774141739_0006_01_000001/jobmanager.out 2> /opt/hadoop/hadoop-2.7.2/logs/userlogs/application_1641774141739_0006/container_1641774141739_0006_01_000001/jobmanager.err"
hadoop_shell_errorcode=$?
if [ $hadoop_shell_errorcode -ne 0 ]
then
  exit $hadoop_shell_errorcode
fi
