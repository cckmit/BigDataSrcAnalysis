

https://mirrors.tuna.tsinghua.edu.cn/apache/spark/2.4.5/2.4.5.tgz

WebHandelr() -> run()
    -> ?
        -> SparkSubmit 
            -> org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer
            

   
[bigdata@ldsver54 bin]$ sh -x ZeppelinStart.sh 
+ sh -x /home/bigdata/app/zeppelin-release/bin/zeppelin-daemon.sh start
+ USAGE='-e Usage: zeppelin-daemon.sh\n\t
        [--config <conf-dir>] {start|stop|upstart|restart|reload|status}\n\t
        [--version | -v]'
+ [[ start == \-\-\c\o\n\f\i\g ]]
+ '[' -L /home/bigdata/app/zeppelin-release/bin/zeppelin-daemon.sh ']'
++ dirname /home/bigdata/app/zeppelin-release/bin/zeppelin-daemon.sh
+ BIN=/home/bigdata/app/zeppelin-release/bin
++ cd /home/bigdata/app/zeppelin-release/bin
++ pwd
+ BIN=/home/bigdata/app/zeppelin-release/bin
+ . /home/bigdata/app/zeppelin-release/bin/common.sh
++ '[' -L /home/bigdata/app/zeppelin-release/bin/common.sh ']'
+++ dirname /home/bigdata/app/zeppelin-release/bin/common.sh
++ FWDIR=/home/bigdata/app/zeppelin-release/bin
++ [[ -z /home/bigdata/app/zeppelin-release ]]
++ [[ -z /home/bigdata/app/zeppelin-release/conf ]]
++ [[ -z '' ]]
++ export ZEPPELIN_LOG_DIR=/home/bigdata/app/zeppelin-release/logs
++ ZEPPELIN_LOG_DIR=/home/bigdata/app/zeppelin-release/logs
++ [[ -z '' ]]
++ export ZEPPELIN_PID_DIR=/home/bigdata/app/zeppelin-release/run
++ ZEPPELIN_PID_DIR=/home/bigdata/app/zeppelin-release/run
++ [[ -z '' ]]
++ [[ -d /home/bigdata/app/zeppelin-release/zeppelin-web/dist ]]
+++ find -L /home/bigdata/app/zeppelin-release -name 'zeppelin-web*.war'
++ export ZEPPELIN_WAR=/home/bigdata/app/zeppelin-release/zeppelin-web-0.8.2.war
++ ZEPPELIN_WAR=/home/bigdata/app/zeppelin-release/zeppelin-web-0.8.2.war
++ [[ -f /home/bigdata/app/zeppelin-release/conf/zeppelin-env.sh ]]
++ . /home/bigdata/app/zeppelin-release/conf/zeppelin-env.sh
+++ export JAVA_HOME=/usr/java/jdk-release
+++ JAVA_HOME=/usr/java/jdk-release
+++ export ZEPPELIN_PORT=48080
+++ ZEPPELIN_PORT=48080
+++ export ZEPPELIN_LOG_DIR=/home/bigdata/log/zeppeline
+++ ZEPPELIN_LOG_DIR=/home/bigdata/log/zeppeline
+++ export ZEPPELIN_JMX_ENABLE=true
+++ ZEPPELIN_JMX_ENABLE=true
+++ export ZEPPELIN_JMX_PORT=10994
+++ ZEPPELIN_JMX_PORT=10994
+++ export ZEPPELIN_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=45075
+++ ZEPPELIN_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=45075
+++ export 'ZEPPELIN_MEM= -Xms512m -Xmx512m'
+++ ZEPPELIN_MEM=' -Xms512m -Xmx512m'
+++ export SPARK_HOME=/home/bigdata/app/spark-release
+++ SPARK_HOME=/home/bigdata/app/spark-release
+++ export 'SPARK_SUBMIT_OPTIONS=--driver-memory 512M --num-executors 1 --executor-memory 512M '
+++ SPARK_SUBMIT_OPTIONS='--driver-memory 512M --num-executors 1 --executor-memory 512M '
+++ export 'ZEPPELIN_INTP_JAVA_OPTS= -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=45076 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=10995 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false '
+++ ZEPPELIN_INTP_JAVA_OPTS=' -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=45076 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=10995 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false '
+++ export SPARK_APP_NAME=SparkOnZeppelin
+++ SPARK_APP_NAME=SparkOnZeppelin
+++ export HADOOP_CONF_DIR=/home/bigdata/data/hadoop/conf
+++ HADOOP_CONF_DIR=/home/bigdata/data/hadoop/conf
+++ export SPARK_CONF_DIR=/home/bigdata/data/spark/conf
+++ SPARK_CONF_DIR=/home/bigdata/data/spark/conf
+++ export 'ZEPPELIN_INTP_CLASSPATH_OVERRIDES=/home/bigdata/app/spark-release/jars/*.jar'
+++ ZEPPELIN_INTP_CLASSPATH_OVERRIDES='/home/bigdata/app/spark-release/jars/*.jar'
+++ export ZEPPELIN_IMPERSONATE_SPARK_PROXY_USER=false
+++ ZEPPELIN_IMPERSONATE_SPARK_PROXY_USER=false
++ ZEPPELIN_CLASSPATH+=:/home/bigdata/app/zeppelin-release/conf
++ ZEPPELIN_COMMANDLINE_MAIN=org.apache.zeppelin.utils.CommandLineUtils
++ [[ -z '' ]]
++ export ZEPPELIN_ENCODING=UTF-8
++ ZEPPELIN_ENCODING=UTF-8
++ [[ -z  -Xms512m -Xmx512m ]]
++ [[ -z '' ]]
++ export 'ZEPPELIN_INTP_MEM=-Xms1024m -Xmx1024m -XX:MaxPermSize=512m'
++ ZEPPELIN_INTP_MEM='-Xms1024m -Xmx1024m -XX:MaxPermSize=512m'
++ JAVA_OPTS+=' -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=45075 -Dfile.encoding=UTF-8  -Xms512m -Xmx512m'
++ JAVA_OPTS+=' -Dlog4j.configuration=file:///home/bigdata/app/zeppelin-release/conf/log4j.properties'
++ export JAVA_OPTS
++ [[ xtrue == x\t\r\u\e ]]
++ [[ -z 10994 ]]
++ JMX_JAVA_OPTS+=' -Dcom.sun.management.jmxremote'
++ JMX_JAVA_OPTS+=' -Dcom.sun.management.jmxremote.port=10994'
++ JMX_JAVA_OPTS+=' -Dcom.sun.management.jmxremote.authenticate=false'
++ JMX_JAVA_OPTS+=' -Dcom.sun.management.jmxremote.ssl=false'
++ JAVA_OPTS=' -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=10994 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false  -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=45075 -Dfile.encoding=UTF-8  -Xms512m -Xmx512m -Dlog4j.configuration=file:///home/bigdata/app/zeppelin-release/conf/log4j.properties'
++ export JAVA_OPTS
++ JAVA_INTP_OPTS=' -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=45076 -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=10995 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false  -Dfile.encoding=UTF-8'
++ [[ -z '' ]]
++ JAVA_INTP_OPTS+=' -Dlog4j.configuration=file:///home/bigdata/app/zeppelin-release/conf/log4j.properties'
++ export JAVA_INTP_OPTS
++ [[ -n /usr/java/jdk-release ]]
++ ZEPPELIN_RUNNER=/usr/java/jdk-release/bin/java
++ export ZEPPELIN_RUNNER
++ [[ -z '' ]]
++ export ZEPPELIN_IDENT_STRING=bigdata
++ ZEPPELIN_IDENT_STRING=bigdata
++ [[ -z '' ]]
++ export ZEPPELIN_INTERPRETER_REMOTE_RUNNER=bin/interpreter.sh
++ ZEPPELIN_INTERPRETER_REMOTE_RUNNER=bin/interpreter.sh
+ . /home/bigdata/app/zeppelin-release/bin/functions.sh
++ '[' -z '' ']'
++ COLUMNS=80
++ '[' -f /etc/sysconfig/i18n -a -z '' -a -z '' ']'
++ SET_OK=0
++ SET_ERROR=1
++ SET_WARNING=2
++ SET_PASSED=3
++ [[ -z '' ]]
++ [[ -f /etc/sysconfig/init ]]
++ . /etc/sysconfig/init
+++ BOOTUP=color
+++ RES_COL=60
+++ MOVE_TO_COL='echo -en \033[60G'
+++ SETCOLOR_SUCCESS='echo -en \033[0;32m'
+++ SETCOLOR_FAILURE='echo -en \033[0;31m'
+++ SETCOLOR_WARNING='echo -en \033[0;33m'
+++ SETCOLOR_NORMAL='echo -en \033[0;39m'
++ [[ '' = \s\e\r\i\a\l ]]
++ hostname
+ HOSTNAME=ldsver54
+ ZEPPELIN_NAME=Zeppelin
+ ZEPPELIN_LOGFILE=/home/bigdata/log/zeppeline/zeppelin-bigdata-ldsver54.log
+ ZEPPELIN_OUTFILE=/home/bigdata/log/zeppeline/zeppelin-bigdata-ldsver54.out
+ ZEPPELIN_PID=/home/bigdata/app/zeppelin-release/run/zeppelin-bigdata-ldsver54.pid
+ ZEPPELIN_MAIN=org.apache.zeppelin.server.ZeppelinServer
+ JAVA_OPTS+=' -Dzeppelin.log.file=/home/bigdata/log/zeppeline/zeppelin-bigdata-ldsver54.log'
+ [[ -d /home/bigdata/app/zeppelin-release/zeppelin-interpreter/target/classes ]]
+ [[ -d /home/bigdata/app/zeppelin-release/zeppelin-zengine/target/classes ]]
+ [[ -d /home/bigdata/app/zeppelin-release/zeppelin-server/target/classes ]]
+ [[ -n /home/bigdata/data/hadoop/conf ]]
+ [[ -d /home/bigdata/data/hadoop/conf ]]
+ ZEPPELIN_CLASSPATH+=:/home/bigdata/data/hadoop/conf
+ addJarInDir /home/bigdata/app/zeppelin-release
+ [[ -d /home/bigdata/app/zeppelin-release ]]
+ ZEPPELIN_CLASSPATH='/home/bigdata/app/zeppelin-release/*::/home/bigdata/app/zeppelin-release/conf:/home/bigdata/data/hadoop/conf'
+ addJarInDir /home/bigdata/app/zeppelin-release/lib
+ [[ -d /home/bigdata/app/zeppelin-release/lib ]]
+ ZEPPELIN_CLASSPATH='/home/bigdata/app/zeppelin-release/lib/*:/home/bigdata/app/zeppelin-release/*::/home/bigdata/app/zeppelin-release/conf:/home/bigdata/data/hadoop/conf'
+ addJarInDir /home/bigdata/app/zeppelin-release/lib/interpreter
+ [[ -d /home/bigdata/app/zeppelin-release/lib/interpreter ]]
+ ZEPPELIN_CLASSPATH='/home/bigdata/app/zeppelin-release/lib/interpreter/*:/home/bigdata/app/zeppelin-release/lib/*:/home/bigdata/app/zeppelin-release/*::/home/bigdata/app/zeppelin-release/conf:/home/bigdata/data/hadoop/conf'
+ addJarInDir /home/bigdata/app/zeppelin-release/zeppelin-interpreter/target/lib
+ [[ -d /home/bigdata/app/zeppelin-release/zeppelin-interpreter/target/lib ]]
+ addJarInDir /home/bigdata/app/zeppelin-release/zeppelin-zengine/target/lib
+ [[ -d /home/bigdata/app/zeppelin-release/zeppelin-zengine/target/lib ]]
+ addJarInDir /home/bigdata/app/zeppelin-release/zeppelin-server/target/lib
+ [[ -d /home/bigdata/app/zeppelin-release/zeppelin-server/target/lib ]]
+ addJarInDir /home/bigdata/app/zeppelin-release/zeppelin-web/target/lib
+ [[ -d /home/bigdata/app/zeppelin-release/zeppelin-web/target/lib ]]
+ CLASSPATH+=':/home/bigdata/app/zeppelin-release/lib/interpreter/*:/home/bigdata/app/zeppelin-release/lib/*:/home/bigdata/app/zeppelin-release/*::/home/bigdata/app/zeppelin-release/conf:/home/bigdata/data/hadoop/conf'
+ [[ '' = '' ]]
+ export ZEPPELIN_NICENESS=0
+ ZEPPELIN_NICENESS=0
+ case "${1}" in
+ start
+ local pid
+ [[ -f /home/bigdata/app/zeppelin-release/run/zeppelin-bigdata-ldsver54.pid ]]
+ initialize_default_directories
+ [[ ! -d /home/bigdata/log/zeppeline ]]
+ [[ ! -d /home/bigdata/app/zeppelin-release/run ]]
+ echo 'ZEPPELIN_CLASSPATH: :.:/usr/java/jdk-release/jre/lib/rt.jar:/usr/java/jdk-release/lib/dt.jar:/usr/java/jdk-release/lib/tools.jar:/home/bigdata/app/zeppelin-release/lib/interpreter/*:/home/bigdata/app/zeppelin-release/lib/*:/home/bigdata/app/zeppelin-release/*::/home/bigdata/app/zeppelin-release/conf:/home/bigdata/data/hadoop/conf'
+ echo 'ZEPPELIN_Start_CMDÆô¶¯ÃüÁî:  nohup nice -n 0 /usr/java/jdk-release/bin/java  -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=10994 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false  -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=45075 -Dfile.encoding=UTF-8  -Xms512m -Xmx512m -Dlog4j.configuration=file:///home/bigdata/app/zeppelin-release/conf/log4j.properties -Dzeppelin.log.file=/home/bigdata/log/zeppeline/zeppelin-bigdata-ldsver54.log -cp :CLASS_PATH org.apache.zeppelin.server.ZeppelinServer >> /home/bigdata/log/zeppeline/zeppelin-bigdata-ldsver54.out 2>&1 < /dev/null & '
+ pid=5714
+ [[ -z 5714 ]]
+ action_msg 'Zeppelin start' 0
+ local STRING rc ACT
+ STRING='Zeppelin start'
+ ACT=0
+ echo -n 'Zeppelin start '
Zeppelin start + shift
+ case $ACT in
+ success_msg 'Zeppelin start'
+ '[' color '!=' verbose -a -z '' ']'
+ echo_success_msg
+ '[' color = color ']'
+ echo -en '\033[60G'
                                                           + echo -n '['
[+ '[' color = color ']'
+ echo -en '\033[0;32m'
+ echo -n '  OK  '
  OK  + nohup nice -n 0 /usr/java/jdk-release/bin/java -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=10994 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=45075 -Dfile.encoding=UTF-8 -Xms512m -Xmx512m -Dlog4j.configuration=file:///home/bigdata/app/zeppelin-release/conf/log4j.properties -Dzeppelin.log.file=/home/bigdata/log/zeppeline/zeppelin-bigdata-ldsver54.log -cp ':.:/usr/java/jdk-release/jre/lib/rt.jar:/usr/java/jdk-release/lib/dt.jar:/usr/java/jdk-release/lib/tools.jar:/home/bigdata/app/zeppelin-release/lib/interpreter/*:/home/bigdata/app/zeppelin-release/lib/*:/home/bigdata/app/zeppelin-release/*::/home/bigdata/app/zeppelin-release/conf:/home/bigdata/data/hadoop/conf' org.apache.zeppelin.server.ZeppelinServer
+ '[' color = color ']'
+ echo -en '\033[0;39m'
+ echo -n ']'
]+ echo -ne '\r'
+ return 0
+ return 0
+ rc=0
+ echo

+ return 0
+ echo 5714
+ wait_zeppelin_is_up_for_ci
+ [[ '' == \t\r\u\e ]]
+ sleep 2
+ check_if_process_is_alive
+ local pid
++ cat /home/bigdata/app/zeppelin-release/run/zeppelin-bigdata-ldsver54.pid
+ pid=5714
+ kill -0 5714


  OK  + 
nohup nice -n 0 /usr/java/jdk-release/bin/java \
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=45075 \
-Dfile.encoding=UTF-8 -Xms512m -Xmx512m \
-Dlog4j.configuration=file:///home/bigdata/app/zeppelin-release/conf/log4j.properties \
-Dzeppelin.log.file=/home/bigdata/log/zeppeline/zeppelin-bigdata-ldsver54.log \
-cp '/usr/java/jdk-release/jre/lib/rt.jar:/usr/java/jdk-release/lib/dt.jar:/usr/java/jdk-release/lib/tools.jar:/home/bigdata/app/zeppelin-release/lib/interpreter/*:/home/bigdata/app/zeppelin-release/lib/*:/home/bigdata/app/zeppelin-release/*::/home/bigdata/app/zeppelin-release/conf:/home/bigdata/data/hadoop/conf' \
org.apache.zeppelin.server.ZeppelinServer



Job.java[run]:190) - Job failed
java.lang.RuntimeException: org.apache.thrift.transport.TTransportException
        at org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcess.callRemoteFunction(RemoteInterprete
rProcess.java:139)
        at org.apache.zeppelin.interpreter.remote.RemoteInterpreter.interpret(RemoteInterpreter.java:228)
        at org.apache.zeppelin.notebook.Paragraph.jobRun(Paragraph.java:449)
        at org.apache.zeppelin.scheduler.Job.run(Job.java:188)
        at org.apache.zeppelin.scheduler.RemoteScheduler$JobRunner.run(RemoteScheduler.java:315)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPool
Executor.java:180)
        at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecuto
r.java:293)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
        at java.lang.Thread.run(Thread.java:745)
Caused by: org.apache.thrift.transport.TTransportException
        at org.apache.thrift.transport.TIOStreamTransport.read(TIOStreamTransport.java:132)
        at org.apache.thrift.transport.TTransport.readAll(TTransport.java:86)
        
        
 Caused by: org.apache.thrift.transport.TTransportException
        at org.apache.thrift.transport.TIOStreamTransport.read(TIOStreamTransport.java:132)
        at org.apache.thrift.transport.TTransport.readAll(TTransport.java:86)
        at org.apache.thrift.protocol.TBinaryProtocol.readAll(TBinaryProtocol.java:429)
        at org.apache.thrift.protocol.TBinaryProtocol.readI32(TBinaryProtocol.java:318)
        at org.apache.thrift.protocol.TBinaryProtocol.readMessageBegin(TBinaryProtocol.java:219)
        at org.apache.thrift.TServiceClient.receiveBase(TServiceClient.java:69)
        at org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService$Client.recv_interpret(RemoteInterpr
eterService.java:274)
        at org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService$Client.interpret(RemoteInterpreterS
ervice.java:258)
        at org.apache.zeppelin.interpreter.remote.RemoteInterpreter$4.call(RemoteInterpreter.java:233)
        at org.apache.zeppelin.interpreter.remote.RemoteInterpreter$4.call(RemoteInterpreter.java:229)
        at org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcess.callRemoteFunction(RemoteInterprete
rProcess.java:135)
        ... 11 more
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
        at java.lang.Thread.run(Thread.java:745)       
        
        
// Spark DriverµÄÂß¼­

at com.fasterxml.jackson.module.scala.JacksonModule$class.setupModule(JacksonModule.scala:64)
  at com.fasterxml.jackson.module.scala.DefaultScalaModule.setupModule(DefaultScalaModule.scala:19)
  at com.fasterxml.jackson.databind.ObjectMapper.registerModule(ObjectMapper.java:747)
  at org.apache.spark.rdd.RDDOperationScope$.<init>(RDDOperationScope.scala:82)
  at org.apache.spark.rdd.RDDOperationScope$.<clinit>(RDDOperationScope.scala)
  at org.apache.spark.SparkContext.withScope(SparkContext.scala:699)
  at org.apache.spark.SparkContext.textFile(SparkContext.scala:828)

  
        
        