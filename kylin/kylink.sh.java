
# 执行 启动kylin 的命令 
kylin.sh start

# 最终拼接的是hbase的命令, 调用 hbase RunJar {className} {args} 执行
hbase ${KYLIN_EXTRA_START_OPTS} \
-Dkylin.hive.dependency=${hive_dependency} -Dkylin.hbase.dependency=${hbase_dependency} \
-Dkylin.spark.dependency=${spark_dependency} -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} \
-Dkylin.server.host-address=${kylin_rest_address} -Dspring.profiles.active=${spring_profile} \
org.apache.hadoop.util.RunJar ${tomcat_root}/bin/bootstrap.jar org.apache.catalina.startup.Bootstrap start


# hbase : hbase ${KYLIN_EXTRA_START_OPTS} -Dkylin.hive.dependency=${hive_dependency} 

	// hbase 拼接Java命令的一般格式: JAVA -Dproc_$COMMAND -XX:OnOutOfMemoryError="kill -9 %p" $HEAP_SETTINGS $HBASE_OPTS $CLASS "$@
	
	// hbase 拼接java命令一般格式2: 
	java -Dproc_$COMMAND \
	-XX:OnOutOfMemoryError="kill -9 %p" \
	$HEAP_SETTINGS \
	$HBASE_OPTS \
	$CLASS $@
	
	// hbase -Dkylin.hive.dependency=${hive_dependency} 对应的hbase各变量值: 
	COMMAND=		-Xms1024M
	HEAP_SETTINGS=	-Xmx512M 
	HBASE_OPTS=		-XX:+UseConcMarkSweepGC  -Dhbase.log.dir=/opt/hbase/hbase-release/logs -Dhbase.log.file=hbase.log -Dhbase.home.dir=/opt/hbase/hbase-release -Dhbase.id.str= -Dhbase.root.logger=INFO,console -Djava.library.path=/home/bigdata/app/hadoop-release/lib/native:/opt/hbase/hbase-release/lib/native/Linux-amd64-64 -Dhbase.security.logger=INFO,NullAppender
	CLASS=			-Xms1024M
	$\@=			-Xmx4096M -Xss1024K -XX:MaxPermSize=512M -verbose:gc -XX:+PrintGCDetails -XX:GCLogFileSize=64M -Dkylin.hive.dependency="/opt/hive/conf:/home/bigdata/app/hive-release/lib/*jar" -Dkylin.hbase.dependency=/opt/hbase/hbase-release/lib/hbase-common-1.2.0-cdh5.16.2.jar org.apache.kylin.tool.AclTableMigrationCLI CHECK


java \
-Dproc_-Xms1024M \						# -Dproc_$COMMAND
'-XX:OnOutOfMemoryError=kill -9 %p' \	# "kill -9 %p"
-Xmx512M \								# $HEAP_SETTINGS		下面这个是 $HBASE_OPTS
-XX:+UseConcMarkSweepGC -Dhbase.log.dir=/opt/hbase/hbase-release/logs -Dhbase.log.file=hbase.log -Dhbase.home.dir=/opt/hbase/hbase-release -Dhbase.id.str= -Dhbase.root.logger=INFO,console -Djava.library.path=/home/bigdata/app/hadoop-release/lib/native:/opt/hbase/hbase-release/lib/native/Linux-amd64-64 -Dhbase.security.logger=INFO,NullAppender \
-Xms1024M \								# $CLASS
-Xmx4096M -Xss1024K -XX:MaxPermSize=512M -verbose:gc -XX:+PrintGCDetails -XX:GCLogFileSize=64M -Dkylin.hive.dependency="/opt/hive/conf:/home/bigdata/app/hive-release/lib/*jar" -Dkylin.hbase.dependency=/opt/hbase/hbase-release/lib/hbase-common-1.2.0-cdh5.16.2.jar org.apache.kylin.tool.AclTableMigrationCLI CHECK




