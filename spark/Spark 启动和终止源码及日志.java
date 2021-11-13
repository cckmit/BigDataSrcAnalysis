

2. Spark运行终止相关源码和日志


Spark关闭相关源码和日志

相关源码:

_shutdownHookRef = ShutdownHookManager.addShutdownHook(ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY){() =>
    logInfo("Invoking stop() from shutdown hook")
		* INFO  SparkContext - Invoking stop() from shutdown hook
    stop(){
		if (_shutdownHookRef != null) {
			ShutdownHookManager.removeShutdownHook(_shutdownHookRef)
		}
		
		Utils.tryLogNonFatalError {
			postApplicationEnd()
		}
		// 停止相关UIWeb
		Utils.tryLogNonFatalError {
		  _ui.foreach(_.stop())
		}
		
		// 停止cleaner线程
		Utils.tryLogNonFatalError {
		  _cleaner.foreach(_.stop())
		}
		
		// 停止容器分配器
		Utils.tryLogNonFatalError {
		  _executorAllocationManager.foreach(_.stop())
		}
		  
		// 停止DAGScheduler
		if (_dagScheduler != null) {
		  Utils.tryLogNonFatalError {
			_dagScheduler.stop()
		  }
		  _dagScheduler = null
		}
		
		// 清空环境变量
		if (_env != null) {
		  Utils.tryLogNonFatalError {
			_env.stop()
		  }
		  SparkEnv.set(null)
		}
		
		SparkContext.clearActiveContext()
		logInfo("Successfully stopped SparkContext")
			* INFO  SparkContext - Successfully stopped SparkContext
		
	}
	  
};
	  
	

相关日志:
logInfo("Invoking stop() from shutdown hook")
	* INFO  SparkContext - Invoking stop() from shutdown hook

	* INFO  SparkContext - Successfully stopped SparkContext
	* INFO  ShutdownHookManager - Shutdown hook called

	
java -cp /home/app/stream/spark/spark-2.2.0-hdp2.7/conf/:/home/app/stream/spark/spark-2.2.0-hdp2.7/jars/*:/home/app/hadoop/hadoop-2.7.1/etc/hadoop/ -Xdebug -Xrunjdwp:transport=dt_socket,server=n,suspend=n,address=192.168.51.1:35056 org.apache.spark.deploy.SparkSubmit --master yarn --deploy-mode cluster --class org.apache.spark.examples.SparkPi --num-executors 2  /home/app/stream/spark/spark-2.2.0-hdp2.7/examples/jars/spark-examples_2.11-2.2.0.jar 10 