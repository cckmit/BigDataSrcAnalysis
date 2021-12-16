

// bin/hadoop 脚本
# hadoop 命令
. $HADOOP_LIBEXEC_DIR/hadoop-config.sh


if [ "$COMMAND" = "fs" ] ; then
    CLASS=org.apache.hadoop.fs.FsShell
elif [ "$COMMAND" = "jar" ] ; then
      CLASS=org.apache.hadoop.util.RunJar
      

    # Always respect HADOOP_OPTS and HADOOP_CLIENT_OPTS
    HADOOP_OPTS="$HADOOP_OPTS $HADOOP_CLIENT_OPTS"

    #make sure security appender is turned off
    HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.security.logger=${HADOOP_SECURITY_LOGGER:-INFO,NullAppender}"
    
    # 这里的cp,应该是上面hadoop-config.sh 中定义的;
    export CLASSPATH=$CLASSPATH
    echo "hadoop shell: $JAVA $JAVA_HEAP_MAX $HADOOP_OPTS $CLASS $@ "
    exec "$JAVA" $JAVA_HEAP_MAX $HADOOP_OPTS $CLASS "$@"
    ;;

// 1.2 libexec/hadoop-config.sh 脚本, 定义环境变量

HADOOP_COMMON_DIR=${HADOOP_COMMON_DIR:-"share/hadoop/common"}
HADOOP_COMMON_LIB_JARS_DIR=${HADOOP_COMMON_LIB_JARS_DIR:-"share/hadoop/common/lib"}
HADOOP_COMMON_LIB_NATIVE_DIR=${HADOOP_COMMON_LIB_NATIVE_DIR:-"lib/native"}
HDFS_DIR=${HDFS_DIR:-"share/hadoop/hdfs"}
HDFS_LIB_JARS_DIR=${HDFS_LIB_JARS_DIR:-"share/hadoop/hdfs/lib"}
YARN_DIR=${YARN_DIR:-"share/hadoop/yarn"}
YARN_LIB_JARS_DIR=${YARN_LIB_JARS_DIR:-"share/hadoop/yarn/lib"}
MAPRED_DIR=${MAPRED_DIR:-"share/hadoop/mapreduce"}
MAPRED_LIB_JARS_DIR=${MAPRED_LIB_JARS_DIR:-"share/hadoop/mapreduce/lib"}

# 这里定义 HADOOP_PREFIX 应该就是$HADOOP_HOME;
HADOOP_DEFAULT_PREFIX=$(cd -P -- "$common_bin"/.. && pwd -P)
HADOOP_PREFIX=${HADOOP_PREFIX:-$HADOOP_DEFAULT_PREFIX}
export HADOOP_PREFIX


#  这里定义Hadoop_CONF_DIR 配置; 重要! 
if [ -e "${HADOOP_PREFIX}/conf/hadoop-env.sh" ]; then
  DEFAULT_CONF_DIR="conf"
else
  DEFAULT_CONF_DIR="etc/hadoop"
fi
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_PREFIX/$DEFAULT_CONF_DIR}"

# 加载 hadoop-env.sh的环境变量;
if [ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]; then
  . "${HADOOP_CONF_DIR}/hadoop-env.sh"
fi




# 重新开始定义CLASSPATH;
CLASSPATH="${HADOOP_CONF_DIR}"
# for releases, add core hadoop jar & webapps to CLASSPATH
if [ -d "$HADOOP_COMMON_HOME/$HADOOP_COMMON_DIR/webapps" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_COMMON_HOME/$HADOOP_COMMON_DIR
fi
if [ -d "$HADOOP_COMMON_HOME/$HADOOP_COMMON_LIB_JARS_DIR" ]; then
  CLASSPATH=${CLASSPATH}:$HADOOP_COMMON_HOME/$HADOOP_COMMON_LIB_JARS_DIR'/*'
fi
CLASSPATH=${CLASSPATH}:$HADOOP_COMMON_HOME/$HADOOP_COMMON_DIR'/*'


# 可以通过 HADOOP_CLASSPATH 用户额外追加 包依赖;
# Add the user-specified CLASSPATH via HADOOP_CLASSPATH
# Add it first or last depending on if user has
# set env-var HADOOP_USER_CLASSPATH_FIRST
# if the user set HADOOP_USE_CLIENT_CLASSLOADER, HADOOP_CLASSPATH is not added
# to the classpath
if [[ ( "$HADOOP_CLASSPATH" != "" ) && ( "$HADOOP_USE_CLIENT_CLASSLOADER" = "" ) ]]; then
  # Prefix it if its to be preceded
  if [ "$HADOOP_USER_CLASSPATH_FIRST" != "" ]; then
    CLASSPATH=${HADOOP_CLASSPATH}:${CLASSPATH}
  else
    CLASSPATH=${CLASSPATH}:${HADOOP_CLASSPATH}
  fi
fi




# Yarn的 RM NM的启动

// 2. 1 start-yarn.sh; 在libexec目录下,有各种真正的环境变量配置;
{
    DEFAULT_LIBEXEC_DIR="$bin"/../libexec
    HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR} //设置LIBEXEC_DIR=/HADOOOP_HOME/libexec
    . $HADOOP_LIBEXEC_DIR/yarn-config.sh  // 执行yarn-config.sh脚本
        { //libexec/yarn-config.sh 脚本;
            // 1. 执行libexec下面的 hadoop-config.sh 加载hadoop配置;
            HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
            if [ -e "${HADOOP_LIBEXEC_DIR}/hadoop-config.sh" ]; then
              . ${HADOOP_LIBEXEC_DIR}/hadoop-config.sh
            elif [ -e "${HADOOP_COMMON_HOME}/libexec/hadoop-config.sh" ]; then
              . "$HADOOP_COMMON_HOME"/libexec/hadoop-config.sh
            elif [ -e "${HADOOP_HOME}/libexec/hadoop-config.sh" ]; then
              . "$HADOOP_HOME"/libexec/hadoop-config.sh
            else
              echo "Hadoop common not found."
              exit
            fi

            // 2. 确定Yarn_conf_dir(默认$HADOOP_CONF_DIR, HADOOOP_HOME/etc/hadoop);
            export YARN_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_YARN_HOME/conf}"

        }

        
    // 2. 启动ResourceMgr, NodeMgr;
    # start resourceManager
    "$bin"/yarn-daemon.sh --config $YARN_CONF_DIR  start resourcemanager
    # start nodeManager
    "$bin"/yarn-daemons.sh --config $YARN_CONF_DIR  start nodemanager

}


// 2. 2  sbin/yarn-daemon.sh  启动本机上的 ResourceManager进程;

{//yarn-daemon.sh
    . $HADOOP_LIBEXEC_DIR/yarn-config.sh // 先设置YARN_CONF_DIR变量;
    
export HADOOP_YARN_USER=${HADOOP_YARN_USER:-yarn}
export YARN_CONF_DIR="${YARN_CONF_DIR:-$HADOOP_YARN_HOME/conf}"
JAVA=$JAVA_HOME/bin/java

YARN_OPTS="$YARN_OPTS -Dhadoop.log.dir=$YARN_LOG_DIR"
YARN_OPTS="$YARN_OPTS -Dyarn.log.dir=$YARN_LOG_DIR"
YARN_OPTS="$YARN_OPTS -Dhadoop.log.file=$YARN_LOGFILE"
YARN_OPTS="$YARN_OPTS -Dyarn.log.file=$YARN_LOGFILE"
YARN_OPTS="$YARN_OPTS -Dyarn.home.dir=$YARN_COMMON_HOME"
YARN_OPTS="$YARN_OPTS -Dyarn.id.str=$YARN_IDENT_STRING"
YARN_OPTS="$YARN_OPTS -Dhadoop.root.logger=${YARN_ROOT_LOGGER:-INFO,console}"
YARN_OPTS="$YARN_OPTS -Dyarn.root.logger=${YARN_ROOT_LOGGER:-INFO,console}"
            
            //加载library.path
            if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
              YARN_OPTS="$YARN_OPTS -Djava.library.path=$JAVA_LIBRARY_PATH"
            fi  
            YARN_OPTS="$YARN_OPTS -Dyarn.policy.file=$YARN_POLICYFILE"
        { etc/hadoop/yarn-env.sh
            export HADOOP_YARN_USER=${HADOOP_YARN_USER:-yarn}
            export YARN_CONF_DIR="${YARN_CONF_DIR:-$HADOOP_YARN_HOME/conf}"
            JAVA=$JAVA_HOME/bin/java
            
            YARN_OPTS="$YARN_OPTS -Dhadoop.log.dir=$YARN_LOG_DIR"
            YARN_OPTS="$YARN_OPTS -Dyarn.log.dir=$YARN_LOG_DIR"
            YARN_OPTS="$YARN_OPTS -Dhadoop.log.file=$YARN_LOGFILE"
            YARN_OPTS="$YARN_OPTS -Dyarn.log.file=$YARN_LOGFILE"
            YARN_OPTS="$YARN_OPTS -Dyarn.home.dir=$YARN_COMMON_HOME"
            YARN_OPTS="$YARN_OPTS -Dyarn.id.str=$YARN_IDENT_STRING"
            YARN_OPTS="$YARN_OPTS -Dhadoop.root.logger=${YARN_ROOT_LOGGER:-INFO,console}"
            YARN_OPTS="$YARN_OPTS -Dyarn.root.logger=${YARN_ROOT_LOGGER:-INFO,console}"
            
            //加载library.path
            if [ "x$JAVA_LIBRARY_PATH" != "x" ]; then
              YARN_OPTS="$YARN_OPTS -Djava.library.path=$JAVA_LIBRARY_PATH"
            fi  
            YARN_OPTS="$YARN_OPTS -Dyarn.policy.file=$YARN_POLICYFILE"
            
        }
        
    
export YARN_LOGFILE=yarn-$YARN_IDENT_STRING-$command-$HOSTNAME.log
export YARN_ROOT_LOGGER=${YARN_ROOT_LOGGER:-INFO,RFA}
log=$YARN_LOG_DIR/yarn-$YARN_IDENT_STRING-$command-$HOSTNAME.out
pid=$YARN_PID_DIR/yarn-$YARN_IDENT_STRING-$command.pid
YARN_STOP_TIMEOUT=${YARN_STOP_TIMEOUT:-5}

// 启动命令
echo starting $command, logging to $log
echo "yarn-daemon.sh命令:  yarn --config $YARN_CONF_DIR $command $@ $log "
cd "$HADOOP_YARN_HOME"

nohup nice -n $YARN_NICENESS "$HADOOP_YARN_HOME"/bin/yarn --config $YARN_CONF_DIR $command "$@" > "$log" 2>&1 < /dev/null &
// yarn --config /home/bigdata/app/hadoop-release/etc/hadoop resourcemanager  > /home/bigdata/log/yarn/yarn-bigdata-resourcemanager-ldsver55.out 2>&1 < /dev/null &


}

// 2.3 sbin/yarn-daemons.sh  启动各Slave节点上的 NodeManger进程;
{
    最终还是调用各机器节点上的 yarn-daemon.sh nodemanager 
}


// 2.4 /bin/yarn 最近启动java进程
{/bin/yarn
    
    JAVA=$JAVA_HOME/bin/java
    COMMAND=$1 //
    
    echo "bin/yarn 脚本命令: java -Dproc_$COMMAND $JAVA_HEAP_MAX $YARN_OPTS -classpath $CLASSPATH $CLASS $@ "
    exec "$JAVA" -Dproc_$COMMAND $JAVA_HEAP_MAX $YARN_OPTS -classpath "$CLASSPATH" $CLASS "$@"
    { //yarn --config /home/bigdata/app/hadoop-release/etc/hadoop resourcemanager
        bin/yarn 脚本命令: 
        java -Dproc_resourcemanager \
        -Xmx1000m  \
        -Dhadoop.log.dir=/home/bigdata/log/yarn -Dyarn.log.dir=/home/bigdata/log/yarn -Dhadoop.log.file=yarn-bigdata-resourcemanager-ldsver55.log -Dyarn.log.file=yarn-bigdata-resourcemanager-ldsver55.log -Dyarn.home.dir= -Dyarn.id.str=bigdata -Dhadoop.root.logger=INFO,RFA -Dyarn.root.logger=INFO,RFA -Djava.library.path=/home/bigdata/app/hadoop-release/lib/native -Dyarn.policy.file=hadoop-policy.xml  -Dhadoop.log.dir=/home/bigdata/log/yarn -Dyarn.log.dir=/home/bigdata/log/yarn -Dhadoop.log.file=yarn-bigdata-resourcemanager-ldsver55.log -Dyarn.log.file=yarn-bigdata-resourcemanager-ldsver55.log -Dyarn.home.dir=/home/bigdata/app/hadoop-2.6.0-cdh5.16.2 -Dhadoop.home.dir=/home/bigdata/app/hadoop-2.6.0-cdh5.16.2 -Dhadoop.root.logger=INFO,RFA -Dyarn.root.logger=INFO,RFA -Djava.library.path=/home/bigdata/app/hadoop-release/lib/native \
        -classpath /home/bigdata/app/hadoop-release/etc/hadoop:/home/bigdata/app/hadoop-release/etc/hadoop:/home/bigdata/app/hadoop-release/etc/hadoop:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/common/lib/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/common/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/hdfs:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/hdfs/lib/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/hdfs/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/yarn/lib/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/yarn/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/mapreduce/lib/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/mapreduce/*:/home/bigdata/app/hadoop-release/contrib/capacity-scheduler/*.jar:/home/bigdata/app/hadoop-release/contrib/capacity-scheduler/*.jar:/home/bigdata/app/hadoop-release/contrib/capacity-scheduler/*.jar:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/yarn/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/yarn/lib/*:/home/bigdata/app/hadoop-release/etc/hadoop/rm-config/log4j.properties \
        org.apache.hadoop.yarn.server.resourcemanager.ResourceManager \
    }
    
    { //yarn application -list
        java -Dproc_application -Xmx1000m  \
        -Dhadoop.log.dir=/home/bigdata/log/yarn -Dyarn.log.dir=/home/bigdata/log/yarn -Dhadoop.log.file=yarn.log -Dyarn.log.file=yarn.log -Dyarn.home.dir= -Dyarn.id.str= -Dhadoop.root.logger=INFO,console -Dyarn.root.logger=INFO,console -Djava.library.path=/home/bigdata/app/hadoop-release/lib/native -Dyarn.policy.file=hadoop-policy.xml  -Dhadoop.log.dir=/home/bigdata/log/yarn -Dyarn.log.dir=/home/bigdata/log/yarn -Dhadoop.log.file=yarn.log -Dyarn.log.file=yarn.log -Dyarn.home.dir=/home/bigdata/app/hadoop-2.6.0-cdh5.16.2 -Dhadoop.home.dir=/home/bigdata/app/hadoop-2.6.0-cdh5.16.2 -Dhadoop.root.logger=INFO,console -Dyarn.root.logger=INFO,console -Djava.library.path=/home/bigdata/app/hadoop-release/lib/native \
        -classpath /home/bigdata/app/hadoop-release/etc/hadoop:/home/bigdata/app/hadoop-release/etc/hadoop:/home/bigdata/app/hadoop-release/etc/hadoop:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/common/lib/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/common/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/hdfs:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/hdfs/lib/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/hdfs/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/yarn/lib/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/yarn/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/mapreduce/lib/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/mapreduce/*:/home/bigdata/app/hadoop-release/contrib/capacity-scheduler/*.jar:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/yarn/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/yarn/lib/* \
        org.apache.hadoop.yarn.client.cli.ApplicationCLI application -list
    }

    
}








# 3. /bin/hadoop 的Hadoop命令脚本


{
     #core commands  
  *)
    # the core commands
    if [ "$COMMAND" = "fs" ] ; then
      CLASS=org.apache.hadoop.fs.FsShell
    elif [ "$COMMAND" = "version" ] ; then
      CLASS=org.apache.hadoop.util.VersionInfo
    elif [ "$COMMAND" = "jar" ] ; then
      CLASS=org.apache.hadoop.util.RunJar
    elif [ "$COMMAND" = "key" ] ; then
      CLASS=org.apache.hadoop.crypto.key.KeyShell
    elif [ "$COMMAND" = "checknative" ] ; then
      CLASS=org.apache.hadoop.util.NativeLibraryChecker
    elif [ "$COMMAND" = "distcp" ] ; then
      CLASS=org.apache.hadoop.tools.DistCp
      CLASSPATH=${CLASSPATH}:${TOOL_PATH}
    elif [ "$COMMAND" = "daemonlog" ] ; then
      CLASS=org.apache.hadoop.log.LogLevel
    elif [ "$COMMAND" = "archive" ] ; then
      CLASS=org.apache.hadoop.tools.HadoopArchives
      CLASSPATH=${CLASSPATH}:${TOOL_PATH}
    elif [ "$COMMAND" = "credential" ] ; then
      CLASS=org.apache.hadoop.security.alias.CredentialShell
    elif [ "$COMMAND" = "s3guard" ] ; then
      CLASS=org.apache.hadoop.fs.s3a.s3guard.S3GuardTool
      CLASSPATH=${CLASSPATH}:${TOOL_PATH}
    elif [ "$COMMAND" = "trace" ] ; then
      CLASS=org.apache.hadoop.tracing.TraceAdmin
    elif [ "$COMMAND" = "classpath" ] ; then
      if [ "$#" -eq 1 ]; then
        # No need to bother starting up a JVM for this simple case.
        echo $CLASSPATH
        exit
      else
        CLASS=org.apache.hadoop.util.Classpath
      fi
    elif [[ "$COMMAND" = -*  ]] ; then
        # class and package names cannot begin with a -
        echo "Error: No command named \`$COMMAND' was found. Perhaps you meant \`hadoop ${COMMAND#-}'"
        exit 1
    else
      CLASS=$COMMAND
    fi
    shift
    
    # Always respect HADOOP_OPTS and HADOOP_CLIENT_OPTS
    HADOOP_OPTS="$HADOOP_OPTS $HADOOP_CLIENT_OPTS"

    #make sure security appender is turned off
    HADOOP_OPTS="$HADOOP_OPTS -Dhadoop.security.logger=${HADOOP_SECURITY_LOGGER:-INFO,NullAppender}"

    export CLASSPATH=$CLASSPATH
    echo "hadoop shell: $JAVA $JAVA_HEAP_MAX $HADOOP_OPTS $CLASS $@ "
    exec "$JAVA" $JAVA_HEAP_MAX $HADOOP_OPTS $CLASS "$@"
    ;;

    
    // 这里导入Classpath作为环境变量; 应该是后面的 RunJar中会自动加载该$CLASSPATH到其环境变量中;
    export CLASSPATH=$CLASSPATH
    echo "hadoop shell: $JAVA $JAVA_HEAP_MAX $HADOOP_OPTS $CLASS $@ "
    exec "$JAVA" $JAVA_HEAP_MAX $HADOOP_OPTS $CLASS "$@"
    
}

// 3.1 hadoop jar -> RunJar的命令

    // export CLASSPATH=$CLASSPATH
    {
        export /home/bigdata/app/hadoop-release/etc/hadoop:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/common/lib/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/common/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/hdfs:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/hdfs/lib/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/hdfs/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/yarn/lib/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/yarn/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/mapreduce/lib/*:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/mapreduce/*:/home/bigdata/app/hadoop-release/contrib/capacity-scheduler/*.jar

        ClassPath: 
        /home/bigdata/app/hadoop-release/etc/hadoop:/home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/common/lib/*:
        /home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/common/*:
        /home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/hdfs:
        /home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/hdfs/lib/*:
        /home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/hdfs/*:
        /home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/yarn/lib/*:
        /home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/yarn/*:
        /home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/mapreduce/lib/*:
        /home/bigdata/app/hadoop-2.6.0-cdh5.16.2/share/hadoop/mapreduce/*:
        /home/bigdata/app/hadoop-release/contrib/capacity-scheduler/*.jar 
    }

    // exec "$JAVA" $JAVA_HEAP_MAX $HADOOP_OPTS $CLASS "$@"
    {
        java -Xmx1000m -Xmx512m \
        -agentlib:jdwp=transport=dt_socket,server=n,suspend=n,address=192.168.51.1:45040 \
        -Djava.net.preferIPv4Stack=true -Dhadoop.log.dir=/home/bigdata/log/hadoop -Dhadoop.log.file=hadoop.log \
        -Dhadoop.home.dir=/home/bigdata/app/hadoop-2.6.0-cdh5.16.2 -Dhadoop.id.str=bigdata -Dhadoop.root.logger=INFO,console \
        -Djava.library.path=/home/bigdata/app/hadoop-release/lib/native -Dhadoop.policy.file=hadoop-policy.xml \
        -Djava.net.preferIPv4Stack=true -Dhadoop.security.logger=INFO,NullAppender \
        org.apache.hadoop.util.RunJar /home/bigdata/app/hadoop-release/share/hadoop/mapreduce2/hadoop-mapreduce-examples-2.6.0-cdh5.16.2.jar pi 2 2 
        
    }

}
}
