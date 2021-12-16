#!/bin/bash
export HADOOP_CONF_DIR="/opt/hadoop/etc/hadoop"
export HIVE_CONF_DIR="/opt/hive/conf"
export FLINK_HOME="/opt/flink/flink-release"
export FLINK_CONF_DIR="/opt/flink/conf"
export CLASSPATH="${HADOOP_CONF_DIR}:${HIVE_CONF_DIR}:${PWD}/conf:${PWD}/lib/*:/opt/dsslinkisall/dsslinkisall-release/linkis/lib/linkis-commons/public-module/*:${PWD}"

// 追加9个 属性配置; 
export HADOOP_HOME="/opt/hadoop/hadoop-release"
export LOG_DIRS="/tmp/dsslinkisall/engConn/bigdata/workDir/a689125c-dc7e-4f0f-8343-6fa6e93c4f48/logs"
export ECM_PORT="9102"
export USER="bigdata"
export ECM_HOST="bdnode111.hjq.com"
export TEMP_DIRS="/tmp/dsslinkisall/engConn/bigdata/workDir/a689125c-dc7e-4f0f-8343-6fa6e93c4f48/tmp"
export ECM_HOME="/opt/dsslinkisall/dsslinkisall-1.0.0_1.0.2/linkis/conf/"
export PWD="/tmp/dsslinkisall/engConn/bigdata/workDir/a689125c-dc7e-4f0f-8343-6fa6e93c4f48"
export RANDOM_PORT="38767"
ln -sf "/tmp/dsslinkisall/engConn/engineConnPublickDir/949295fd-73c8-467f-8e87-b10be1067d53/v000002/conf" "/tmp/dsslinkisall/engConn/bigdata/workDir/a689125c-dc7e-4f0f-8343-6fa6e93c4f48/conf"

linkis_engineconn_errorcode=$?

if [ $linkis_engineconn_errorcode -ne 0 ]
then
  cat ${LOG_DIRS}/stderr
  exit $linkis_engineconn_errorcode
fi
ln -sf "/tmp/dsslinkisall/engConn/engineConnPublickDir/3ec04b65-950c-440e-8f9e-54eb658ab029/v000002/lib" "/tmp/dsslinkisall/engConn/bigdata/workDir/a689125c-dc7e-4f0f-8343-6fa6e93c4f48/lib"
linkis_engineconn_errorcode=$?
if [ $linkis_engineconn_errorcode -ne 0 ]
then
  cat ${LOG_DIRS}/stderr
  exit $linkis_engineconn_errorcode
fi

${JAVA_HOME}/bin/java -server -Xmx1g -Xms1g -XX:+UseG1GC -XX:MaxPermSize=250m -XX:PermSize=128m -Xloggc:${LOG_DIRS}/gc.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Dwds.linkis.configuration=linkis-engineconn.properties -Dwds.linkis.gateway.url=http://192.168.51.111:9001 -Dlogging.file=log4j2-engineconn.xml  -DTICKET_ID=a689125c-dc7e-4f0f-8343-6fa6e93c4f48 -Djava.io.tmpdir=${TEMP_DIRS} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=${RANDOM_PORT} -cp ${CLASSPATH} com.webank.wedatasphere.linkis.engineconn.launch.EngineConnServer 1> ${LOG_DIRS}/stdout 2>> ${LOG_DIRS}/stderr --engineconn-conf wds.linkis.rm.instance=1 --engineconn-conf label.userCreator=bigdata-LINKISCLI --engineconn-conf ticketId=a689125c-dc7e-4f0f-8343-6fa6e93c4f48 --engineconn-conf wds.linkis.rm.yarnqueue.memory.max=300G --engineconn-conf label.engineType=flink-1.12.2 --engineconn-conf wds.linkis.rm.yarnqueue.instance.max=30 --engineconn-conf flink.container.num=1 --engineconn-conf flink.taskmanager.memory=1 --engineconn-conf wds.linkis.rm.client.memory.max=20G --engineconn-conf wds.linkis.rm.client.core.max=10 --engineconn-conf wds.linkis.engineConn.memory=4G --engineconn-conf wds.linkis.rm.yarnqueue.cores.max=150 --engineconn-conf user=bigdata --engineconn-conf wds.linkis.rm.yarnqueue=default --spring-conf eureka.client.serviceUrl.defaultZone=http://192.168.51.111:20303/eureka/ --spring-conf logging.config=classpath:log4j2-engineconn.xml --spring-conf spring.profiles.active=engineconn --spring-conf server.port=42018 --spring-conf spring.application.name=linkis-cg-engineconn
linkis_engineconn_errorcode=$?
if [ $linkis_engineconn_errorcode -ne 0 ]
then
  cat ${LOG_DIRS}/stderr
  exit $linkis_engineconn_errorcode
fi







${JAVA_HOME}/bin/java \
-server -Xmx1g -Xms1g -XX:+UseG1GC -XX:MaxPermSize=250m -XX:PermSize=128m \
-Xloggc:${LOG_DIRS}/gc.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps \
-Dwds.linkis.configuration=linkis-engineconn.properties -Dwds.linkis.gateway.url=http://192.168.51.111:9001 -Dlogging.file=log4j2-engineconn.xml  -DTICKET_ID=a689125c-dc7e-4f0f-8343-6fa6e93c4f48 -Djava.io.tmpdir=${TEMP_DIRS} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=${RANDOM_PORT} \
-cp ${CLASSPATH} com.webank.wedatasphere.linkis.engineconn.launch.EngineConnServer \
1> ${LOG_DIRS}/stdout 2>> ${LOG_DIRS}/stderr \
--engineconn-conf wds.linkis.rm.instance=1 --engineconn-conf label.userCreator=bigdata-LINKISCLI --engineconn-conf ticketId=a689125c-dc7e-4f0f-8343-6fa6e93c4f48 --engineconn-conf wds.linkis.rm.yarnqueue.memory.max=300G --engineconn-conf label.engineType=flink-1.12.2 --engineconn-conf wds.linkis.rm.yarnqueue.instance.max=30 --engineconn-conf flink.container.num=1 --engineconn-conf flink.taskmanager.memory=1 --engineconn-conf wds.linkis.rm.client.memory.max=20G --engineconn-conf wds.linkis.rm.client.core.max=10 --engineconn-conf wds.linkis.engineConn.memory=4G --engineconn-conf wds.linkis.rm.yarnqueue.cores.max=150 --engineconn-conf user=bigdata --engineconn-conf wds.linkis.rm.yarnqueue=default --spring-conf eureka.client.serviceUrl.defaultZone=http://192.168.51.111:20303/eureka/ --spring-conf logging.config=classpath:log4j2-engineconn.xml --spring-conf spring.profiles.active=engineconn --spring-conf server.port=42018 --spring-conf spring.application.name=linkis-cg-engineconn
