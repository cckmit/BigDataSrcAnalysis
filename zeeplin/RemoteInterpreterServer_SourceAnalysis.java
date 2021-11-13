

# RemoteInterpreterServer  服务的进程详解

/** "main"线程:
* - fromThread: "SchedulerFactory-n" 线程: JobRunner.run()-> RemoteInterprete.interpret() -> InterpreterLauncher.launcher()
*
* - nextThread: "Thread-1"线程 RemoteInterpreterServer.run()
*/

RemoteInterpreterServer.main(){
    //根据配置,创建相应解释器的实现类? SparkInterpreter/ FlinkInterpreter
    RemoteInterpreterServer remoteInterpreterServer =new RemoteInterpreterServer(zeppelinServerHost, port, interpreterGroupId, portRange);{
        this(intpEventServerHost, intpEventServerPort, portRange, interpreterGroupId, false);{
            intpEventClient = new RemoteInterpreterEventClient(intpEventServerHost, intpEventServerPort);
            RemoteInterpreterService.Processor<RemoteInterpreterServer> processor =new RemoteInterpreterService.Processor<>(this);
            
            if (null == intpEventServerHost) {//host为null时, 新建Socket;
                serverTransport = new TServerSocket(intpEventServerPort);
            }else{ //正常,进入这里: 打开本地端口;
                serverTransport = RemoteInterpreterUtils.createTServerSocket(portRange);
            }
            
            server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            
        }
    }
    // 启动Flink应该是在这里面;
    remoteInterpreterServer.start();{
        
    }
    System.exit(0);
}


/**
*
*/
RemoteInterpreterServer.run(){
    if (null != intpEventServerHost && !isTest) {
        // 启动线程2: "Thread-2"线程, 执行什么?
        new Thread(new Runnable() {
        boolean interrupted = false;

        @Override
        public void run() {
            //当被打断,且 server已经停掉是,才会进入下面的循环等待; 
            while (!interrupted && !server.isServing()) { //正常不会进入以下等待;
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                  interrupted = true;
                }
            }

          if (zconf.isClusterMode()) {//Zpn是集群时,才进入这里; 正常不进入;
            putClusterMeta();
          } else {// 正常进入这里;
            if (!interrupted) {
              RegisterInfo registerInfo = new RegisterInfo(host, port, interpreterGroupId);
              try {
                intpEventClient.registerInterpreterProcess(registerInfo);{
                    callRemoteFunction(client -> {
                        client.registerInterpreterProcess(registerInfo);
                        return null;
                    });{// callRemoteFunction() -> client.registerInterpreterProcess()
                        return remoteClient.callRemoteFunction(func);{
                            client = getClient();
                            if (client != null) {
                                return func.call(client);{// 这里的func.call()即外面定义的 client.registerInterpreterProcess()方法;
                                    client.registerInterpreterProcess();{//RemoteInterpreterEventClient.registerInterpreterProcess()
                                        send_registerInterpreterProcess(registerInfo);
                                        recv_registerInterpreterProcess();
                                    }
                                }
                            }
                        }
                    }
                }
                
                LOGGER.info("Registered interpreter process");
              } catch (Exception e) {
                shutdown();
              }
            }
          }

          if (launcherEnv != null && "yarn".endsWith(launcherEnv)) {
            try {
              YarnUtils.register(host, port);
              Thread thread = new Thread(() -> {
                while(!Thread.interrupted() && server.isServing()) {
                  YarnUtils.heartbeat();
                  try {
                    Thread.sleep(60 * 1000);
                  } catch (InterruptedException e) {
                    LOGGER.warn(e.getMessage(), e);
                  }
                }
              });
              thread.setName("RM-Heartbeat-Thread");
              thread.start();
            } catch (Exception e) {
              LOGGER.error("Fail to register yarn app", e);
            }
          }
        }
      }).start();
    }
    server.serve();{//TThreadPoolServer.serve()
        if (!preServe()) return;
        execute();
        waitForShutdown();
    }
}




// FIFOScheduler-interpreter_** 线程:

callRemoteFunction:80, PooledRemoteClient (org.apache.zeppelin.interpreter.remote)
callRemoteFunction:80, RemoteInterpreterEventClient (org.apache.zeppelin.interpreter.remote)
onInterpreterOutputUpdate:224, RemoteInterpreterEventClient (org.apache.zeppelin.interpreter.remote)
onUpdate:958, RemoteInterpreterServer$4 (org.apache.zeppelin.interpreter.remote)
onUpdate:115, InterpreterOutput$1 (org.apache.zeppelin.interpreter)
write:107, InterpreterResultMessageOutput (org.apache.zeppelin.interpreter)
write:269, InterpreterOutput (org.apache.zeppelin.interpreter)
write:63, InterpreterOutputStream (org.apache.zeppelin.interpreter.util)
write:75, InterpreterOutputStream (org.apache.zeppelin.interpreter.util)
writeBytes:221, StreamEncoder (sun.nio.cs)
implFlushBuffer:291, StreamEncoder (sun.nio.cs)
implFlush:295, StreamEncoder (sun.nio.cs)
flush:141, StreamEncoder (sun.nio.cs)
flush:229, OutputStreamWriter (java.io)
flush:254, BufferedWriter (java.io)
flush:320, PrintWriter (java.io)
flush:320, PrintWriter (java.io)
printMessage:45, ConsoleReporter (scala.tools.nsc.reporters)
printMessage:61, ReplReporter (scala.tools.nsc.interpreter)
apply:653, IMain$WrappedRequest$$anonfun$loadAndRunReq$1 (scala.tools.nsc.interpreter)
apply:644, IMain$WrappedRequest$$anonfun$loadAndRunReq$1 (scala.tools.nsc.interpreter)
asContext:31, ScalaClassLoader$class (scala.reflect.internal.util)
asContext:19, AbstractFileClassLoader (scala.reflect.internal.util)
loadAndRunReq:644, IMain$WrappedRequest (scala.tools.nsc.interpreter)
interpret:576, IMain (scala.tools.nsc.interpreter)
interpret:572, IMain (scala.tools.nsc.interpreter)
apply:616, FlinkScalaInterpreter$$anonfun$interpret$1$$anonfun$apply$3 (org.apache.zeppelin.flink)
apply:607, FlinkScalaInterpreter$$anonfun$interpret$1$$anonfun$apply$3 (org.apache.zeppelin.flink)
apply:733, TraversableLike$WithFilter$$anonfun$foreach$1 (scala.collection)
foreach:33, IndexedSeqOptimized$class (scala.collection)
foreach:186, ArrayOps$ofRef (scala.collection.mutable)
foreach:732, TraversableLike$WithFilter (scala.collection)
apply:607, FlinkScalaInterpreter$$anonfun$interpret$1 (org.apache.zeppelin.flink)
apply:598, FlinkScalaInterpreter$$anonfun$interpret$1 (org.apache.zeppelin.flink)
withValue:58, DynamicVariable (scala.util)
withOut:65, Console$ (scala)
interpret:598, FlinkScalaInterpreter (org.apache.zeppelin.flink)
interpret:94, FlinkInterpreter (org.apache.zeppelin.flink)
interpret:110, LazyOpenInterpreter (org.apache.zeppelin.interpreter)
jobRun:776, RemoteInterpreterServer$InterpretJob (org.apache.zeppelin.interpreter.remote)
jobRun:668, RemoteInterpreterServer$InterpretJob (org.apache.zeppelin.interpreter.remote)
run:172, Job (org.apache.zeppelin.scheduler)
runJob:130, AbstractScheduler (org.apache.zeppelin.scheduler)
lambda$runJobInScheduler$0:39, FIFOScheduler (org.apache.zeppelin.scheduler)
run:-1, 377906020 (org.apache.zeppelin.scheduler.FIFOScheduler$$Lambda$18)
runWorker:1149, ThreadPoolExecutor (java.util.concurrent)
run:624, ThreadPoolExecutor$Worker (java.util.concurrent)
run:748, Thread (java.lang)









