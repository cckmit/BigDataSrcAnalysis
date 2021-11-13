

_sdc运行所需环境变量:

${SDC_DIST}" ${SDC_HOME}" ${SDC_CONF}"     ${SDC_DATA}" ${SDC_LOG}" ${SDC_RESOURCES}" ${SDC_KAFKA_JAAS_CONF}"
${KRB5CCNAME}"  ${SDC_POLICY_FILE}"  ${JAVA}"
${BOOTSTRAP_CLASSPATH}"     ${API_CLASSPATH}"   ${CONTAINER_CLASSPATH}"     ${LIBS_COMMON_LIB_DIR}" ${STREAMSETS_LIBRARIES_DIR}"
${STREAMSETS_LIBRARIES_EXTRA_DIR}"  ${USER_LIBRARIES_DIR}"    ${SDC_JAVA_OPTS}"    ${SDC_MAIN_CLASS}"


${JAVA} -classpath ${BOOTSTRAP_CLASSPATH} ${SDC_JAVA_OPTS} com.streamsets.pipeline.BootstrapMain \
    -mainClass ${SDC_MAIN_CLASS} 
    -apiClasspath "${API_CLASSPATH}" 
    -containerClasspath "${CONTAINER_CLASSPATH}" 
    -streamsetsLibrariesDir "${STREAMSETS_LIBRARIES_DIR}" 
    -userLibrariesDir "${USER_LIBRARIES_DIR}" 
    -configDir ${SDC_CONF} 
    -libsCommonLibDir "${LIBS_COMMON_LIB_DIR}" ${EXTRA_OPTIONS}

	
1. sdc-env.sh 环境变量加载脚本分析

# 读取环境变量
${SDC_HOME}/libexec/sdc-env.sh ]

# 启动Java进程
if [ $EXEC -eq 1 ]; then
  exec ${JAVA} -classpath ${BOOTSTRAP_CLASSPATH} ${SDC_JAVA_OPTS} com.streamsets.pipeline.BootstrapMain \
	   -mainClass ${SDC_MAIN_CLASS} -apiClasspath "${API_CLASSPATH}" -containerClasspath "${CONTAINER_CLASSPATH}" \
	   -streamsetsLibrariesDir "${STREAMSETS_LIBRARIES_DIR}" -userLibrariesDir "${USER_LIBRARIES_DIR}" -configDir ${SDC
_CONF} \
	   -libsCommonLibDir "${LIBS_COMMON_LIB_DIR}" ${EXTRA_OPTIONS}

  status=$?
else
  # 如果是safeStop()并返回88代码, 则循环再启动;
  status=88
  while [ $status = 88 ]; do
	${JAVA} -classpath ${BOOTSTRAP_CLASSPATH} ${SDC_JAVA_OPTS} com.streamsets.pipeline.BootstrapMain \
	-mainClass ${SDC_MAIN_CLASS} -apiClasspath "${API_CLASSPATH}" -containerClasspath "${CONTAINER_CLASSPATH}" \
	-streamsetsLibrariesDir "${STREAMSETS_LIBRARIES_DIR}" -userLibrariesDir "${USER_LIBRARIES_DIR}" -configDir ${SDC_CO
NF} \
	-libsCommonLibDir "${LIBS_COMMON_LIB_DIR}" ${EXTRA_OPTIONS}

	status=$?
  done
fi
	

	

2.  Cluster Pipeline的启动脚本: _cluster-manager 脚本分析


# 定义Yarn,Spark,Kinit,Hadoop命令
KINIT_COMMAND=${KINIT_COMMAND:-kinit}
YARN_COMMAND=${YARN_COMMAND:-/usr/bin/yarn}
SPARK_SUBMIT_YARN_COMMAND=${SPARK_SUBMIT_YARN_COMMAND:-/usr/bin/spark-submit}
SPARK_SUBMIT_MESOS_COMMAND=${SPARK_SUBMIT_MESOS_COMMAND:-/usr/bin/spark-submit}
HADOOP_COMMAND=${HADOOP_COMMAND:-/usr/bin/hadoop}

# 启动yarn,mesos,mr集群任务;
if [[ $command == "start" ]]
then
  if [[ "$CLUSTER_TYPE" == "mesos" ]]
    ...
    exec $SPARK_SUBMIT_MESOS_COMMAND "$@"
  elif [[ "$CLUSTER_TYPE" == "yarn" ]]
  then
    exec $SPARK_SUBMIT_YARN_COMMAND "$@"
  elif [[ "$CLUSTER_TYPE" == "mr" ]]
  then
    exec $HADOOP_COMMAND "$@"
  else
    echo "ERROR: '$CLUSTER_TYPE' is not a supported cluster type" 1>&2
    exit 1
  fi


 最终运行的脚本命令: 
 spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --executor-memory 1024m \
    --executor-cores 1 \
    --num-executors 2 \
    --archives /home/app/stream/streamset/streamset-3.3.2_tag0529/data/temp/cluster-pipeline-HJQKafkaClusterTestRemoteDebugad64f0a7-dc26-4b5d-bf11-b1c6d5010b16-0/staging/libs.tar.gz,/home/app/stream/streamset/streamset-3.3.2_tag0529/data/temp/cluster-pipeline-HJQKafkaClusterTestRemoteDebugad64f0a7-dc26-4b5d-bf11-b1c6d5010b16-0/staging/etc.tar.gz,/home/app/stream/streamset/streamset-3.3.2_tag0529/data/temp/cluster-pipeline-HJQKafkaClusterTestRemoteDebugad64f0a7-dc26-4b5d-bf11-b1c6d5010b16-0/staging/resources.tar.gz \
    --files /home/app/stream/streamset/streamset-3.3.2_tag0529/data/temp/cluster-pipeline-HJQKafkaClusterTestRemoteDebugad64f0a7-dc26-4b5d-bf11-b1c6d5010b16-0/staging/log4j.properties \
    --jars /home/app/stream/streamset/streamset-3.3.2_tag0529/libexec/bootstrap-libs/main/streamsets-datacollector-bootstrap-3.3.2-SNAPSHOT.jar,/home/app/stream/streamset/streamset-3.3.2_tag0529/streamsets-libs/streamsets-datacollector-apache-kafka_0_10-lib/lib/kafka_2.11-0.10.2.1.jar,/home/app/stream/streamset/streamset-3.3.2_tag0529/streamsets-libs/streamsets-datacollector-apache-kafka_0_10-lib/lib/kafka-clients-0.10.2.1.jar,/home/app/stream/streamset/streamset-3.3.2_tag0529/streamsets-libs/streamsets-datacollector-apache-kafka_0_10-lib/lib/metrics-core-2.2.0.jar,/home/app/stream/streamset/streamset-3.3.2_tag0529/container-lib/streamsets-datacollector-container-3.3.2-SNAPSHOT.jar,/home/app/stream/streamset/streamset-3.3.2_tag0529/container-lib/streamsets-datacollector-common-3.3.2-SNAPSHOT.jar,/home/app/stream/streamset/streamset-3.3.2_tag0529/api-lib/streamsets-datacollector-api-3.3.2-SNAPSHOT.jar,/home/app/stream/streamset/streamset-3.3.2_tag0529/libexec/bootstrap-libs/cluster/streamsets-datacollector-cluster-bootstrap-3.3.2-SNAPSHOT.jar \
    --conf spark.running.mode=yarn \
    --conf spark.streaming.dynamicAllocation.maxExecutors=5 \
    --conf spark.driver.extraJavaOptions=-Duser.home=. \ 	# 在Pipeline.Cluster配置中设置
    --conf spark.executor.extraJavaOptions=-javaagent:./streamsets-datacollector-bootstrap-3.3.2-SNAPSHOT.jar -Duser.home=. -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -Dlog4j.debug' \
    --conf spark.executorEnv.JAVA_HOME=/home/app/java/jdk1.8.0_65  			# 在
    --conf spark.streaming.kafka.consumer.poll.max.retries=5 \
    --conf spark.yarn.appMasterEnv.JAVA_HOME=/home/app/java/jdk1.8.0_65 \
    --conf spark.streaming.dynamicAllocation.enabled=false \
    --conf spark.driver.memory=512m \
    --name 'StreamSets Data Collector:HJQ_Kafka_Cluster_TestRemoteDebug' \
    --class com.streamsets.pipeline.BootstrapClusterStreaming  /home/app/stream/streamset/streamset-3.3.2_tag0529/libexec/bootstrap-libs/cluster/streamsets-datacollector-cluster-bootstrap-api-3.3.2-SNAPSHOT.jar

  
  
