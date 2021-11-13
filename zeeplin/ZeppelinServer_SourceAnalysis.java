

# ZeppelinServer Web服务的进程详解




main.(args){
    ZeppelinServer.conf = ZeppelinConfiguration.create();
    public static Server jettyWebServer = setupJettyServer(conf);
    
    jettyWebServer.setHandler(contexts);
    
    // Cluster Manager Server: 这个是干嘛的?
    setupClusterManagerServer(sharedServiceLocator);
    
    try {// 这里正是启动Server; 干嘛?
        jettyWebServer.start(); // Instantiates ZeppelinServer
        
        List<ErrorData> errorData = handler.waitForAtLeastOneConstructionError(5 * 1000);
        if(errorData.size() > 0 && errorData.get(0).getThrowable() != null) {
            throw new Exception(errorData.get(0).getThrowable());
        }
    } catch (Exception e) {
        LOG.error("Error while running jettyServer", e);
        System.exit(-1);
    }
    LOG.info("Done, zeppelin server started");
    
    Runtime.getRuntime().addShutdownHook(shutdown(conf)); //加个退出的钩子函数;
    
    //获取Notebook; ?
    Notebook notebook = sharedServiceLocator.getService(Notebook.class);
    notebook.recoveryIfNecessary(); //恢复什么?
    
    jettyWebServer.join();// 阻塞等待请求;
    
    
}



/** "qtp*"线程:  NotebookSocket 完成一个网络请求的执行;
*   - fromThread: 
    NotebookSocket.onWebSocketText() -> NotebookServer.onMessage(){ //里面根据不同的操作类型,调用方法;
        case RUN_PARAGRAPH: runParagraph() -> Note.run() -> Paragraph.execute():
            -> InterpreterFactory.getInterpreter() -> ManagedInterpreterGroup.getOrCreate() -> createInterpreterGroup(groupId);
            -> interpreter.getScheduler()[AbstractScheduler].submit(this) ->  queue.put(job),jobs.put(job.getId(), job); 将Job存于对象,等待"SchedulerFactory2" 线程拉取运行该Job;
            
        case RUN_ALL_PARAGRAPHS:
    }
*   - nextThread: 
*/
NotebookSocket.onWebSocketText(){
    listener.onMessage(this, message);{//NotebookServer:
        Message messagereceived = deserializeMessage(msg);
        switch (messagereceived.op) {
            case RUN_PARAGRAPH:
                runParagraph(conn, messagereceived);{//NotebookServer:
                    String paragraphId = (String) fromMessage.get("id");
                    String noteId = getConnectionManager().getAssociatedNoteId(conn);
                    String text = (String) fromMessage.get("paragraph");
                    getNotebookService().runParagraph(noteId, paragraphId, title, text, params, config,
                        false, false, getServiceContext(fromMessage),
                        new WebSocketServiceCallback<Paragraph>(conn) {
                          @Override public void onSuccess(Paragraph p, ServiceContext context) throws IOException {
                              //todo
                          }
                        });{
                            // 获取该Note和Paragraph, 并运行该 Paragraph;
                            Note note = notebook.getNote(noteId);
                            notebook.saveNote(note, context.getAutheInfo());
                            
                            note.run(p.getId(), blocking, context.getAutheInfo().getUser());{//Note.run()
                                Paragraph p = getParagraph(paragraphId);
                                // 核心逻辑: 这里, 完成获取相应解释器,并执行计算, 修改相关状态;
                                return p.execute(blocking);{//Paragraph.execute(boolean blocking): 
                                    //初始化时,没有绑定要创建相应Interpreter(Spark/Flink)的实例?
                                    this.interpreter = getBindedInterpreter();{
                                        // 工厂类, 生成相应的解释器: Flink或Spark;
                                        return this.note.getInterpreterFactory().getInterpreter(intpText, executionContext);{//InterpreterFactory.getInterpreter():
                                            InterpreterSetting setting =interpreterSettingManager.getByName(executionContext.getDefaultInterpreterGroup());
                                            Interpreter interpreter = setting.getInterpreter(executionContext, replName);{//InterpreterSetting: 根据配置生成 Interpreter的实例
                                                List<Interpreter> interpreters = getOrCreateSession(executionContext);{
                                                    // 从INTP缓存组interpreterGroups:Map<String, ManagedInterpreterGroup> 中获取或新建 释器组;
                                                    ManagedInterpreterGroup interpreterGroup = getOrCreateInterpreterGroup(executionContext);{
                                                        //根据解释器组来 构建/返回 解释器的实例; 如是新建的组, 则新建后存于该 interpreterGroups INTP的组的缓存中;
                                                        String groupId = getInterpreterGroupId(executionContext);
                                                        if (!interpreterGroups.containsKey(groupId)) { 
                                                            ManagedInterpreterGroup intpGroup = createInterpreterGroup(groupId);
                                                            interpreterGroups.put(groupId, intpGroup);
                                                        }
                                                        return interpreterGroups.get(groupId);
                                                    }
                                                    
                                                    String sessionId = getInterpreterSessionId(executionContext);
                                                    //从缓存中,根据回话ID获取所有的 解释器;
                                                    return interpreterGroup.getOrCreateSession(executionContext.getUser(), sessionId);{
                                                        if (sessions.containsKey(sessionId)) {
                                                            return sessions.get(sessionId);
                                                        } else {
                                                            List<Interpreter> interpreters = interpreterSetting.createInterpreters(user, id, sessionId);
                                                            for (Interpreter interpreter : interpreters) {
                                                                interpreter.setInterpreterGroup(this);
                                                            }
                                                            sessions.put(sessionId, interpreters);
                                                            return interpreters;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    
                                    
                                    setStatus(Status.READY);{//Job.setStatus(): 有父类Job实现;
                                        if (this.status == status) return;
                                        if (listener != null && before != null && before != after) {
                                            listener.onStatusChange(this, before, after);{//NotebookServer.onStatusChange
                                                broadcastParagraph(p.getNote(), p);{//NotebookServer:
                                                    inlineBroadcastParagraph(note, p);
                                                    broadcastClusterEvent(ClusterEvent.BROADCAST_PARAGRAPH, note, p);
                                                }
                                                broadcastUpdateNoteJobInfo(p.getNote(), System.currentTimeMillis() - 5000);
                                            }
                                        }
                                    }
                                    
                                    if (getConfig().get("enabled") == null || (Boolean) getConfig().get("enabled")) {
                                        interpreter.getScheduler().submit(this);{//AbstractScheduler.submit()
                                            job.setStatus(Job.Status.PENDING);
                                            queue.put(job);
                                            jobs.put(job.getId(), job);
                                        }
                                    }
                                    
                                    if (blocking) {// 若设置了循环等待, 这里进入死循环; 
                                        
                                    }
                                    return true;
                                }
                            }
                            
                            callback.onSuccess(p, context);// 即上面WebSocketServiceCallback的匿名函数;
                        }
                }
                break;
            case RUN_ALL_PARAGRAPHS:
                runAllParagraphs(conn, messagereceived);
                break;
        }
    }
}


/** "SchedulerFactory2"线程: 
*   - fromThread: ?
        while(){
            Job runningJob = queue.take();
            runJobInScheduler(runningJob); => executor.execute(new JobRunner()): 另起第二个线程"SchedulerFactory-n" 完成Job的提交
                "SchedulerFactory-n"线程:JobRunner.run()
                    => AbstractScheduler.runJob() -> Job.run() -> Paragraph.jobRun() -> RemoteInterprete.interpret() => 
                        - getOrCreateInterpreterProcess(); 获取或新建远程解释器;
                        - client.interpret()    :   执行脚本;
        }
*   - nextThread: "SchedulerFactory3"线程
*/
  AbstractScheduler.run() {
    schedulerThread = Thread.currentThread();
    while (!terminate && !schedulerThread.isInterrupted()) {
        Job runningJob = null;
        try {
            runningJob = queue.take();//阻塞队列,没数据时,会一直阻塞等待着;
        } catch (InterruptedException e) {
            LOGGER.warn("{} is interrupted", getClass().getSimpleName());
            break;
        }
        // 在这里完成计算;
        runJobInScheduler(runningJob);{//RemoteScheduler.runJobInScheduler(): 另起"SchedulerFactory-n"线程,完成Job提交;
            JobRunner jobRunner = new JobRunner(this, job);
            //另起一线程,执行该Job: "SchedulerFactory-n" 线程的执行逻辑;
            executor.execute(jobRunner);{
                JobRunner.run(){
                    JobStatusPoller jobStatusPoller = new JobStatusPoller(job, this, 100);
                    jobStatusPoller.start();
                    scheduler.runJob(job);{//AbstractScheduler.runJob()
                        runningJob.run();{//Job.run()
                            onJobStarted();
                            completeWithSuccess(jobRun());{//Paragraph.jobRun()
                                this.interpreter = getBindedInterpreter();
                                String script = this.scriptText;
                                // 开始执行脚本
                                InterpreterContext context = getInterpreterContext();
                                /* 核心方法: 这里针对脚本,执行解释器;
                                *   RemoteInterprete: 是远程解释器的代理,应该是用于向远程的(Flink/Spark) 执行器实例,提交作业的;
                                */
                                InterpreterResult ret = interpreter.interpret(script, context);{//RemoteInterprete.interpret()
                                    RemoteInterpreterProcess interpreterProcess = getOrCreateInterpreterProcess();{
                                        ManagedInterpreterGroup intpGroup = getInterpreterGroup();
                                        this.interpreterProcess = intpGroup.getOrCreateInterpreterProcess(getUserName(), properties);{//ManagedInterpreterGroup.getOrCreateInterpreterProcess()
                                            if (remoteInterpreterProcess == null) {
                                                // 在这里,正式创建 一个远程解释器的进程: 
                                                remoteInterpreterProcess = interpreterSetting.createInterpreterProcess(id, userName, properties);{
                                                    InterpreterLauncher launcher = createLauncher(properties);
                                                    InterpreterLaunchContext launchContext = new InterpreterLaunchContext(properties, option, interpreterRunner, userName, interpreterGroupId, id, group, name, interpreterEventServer.getPort(), interpreterEventServer.getHost());
                                                    RemoteInterpreterProcess process = (RemoteInterpreterProcess) launcher.launch(launchContext);{
                                                        
                                                    }
                                                    
                                                    recoveryStorage.onInterpreterClientStart(process);
                                                    return process;
                                                }
                                                interpreterSetting.getLifecycleManager().onInterpreterProcessStarted(this);
                                                getInterpreterSetting().getRecoveryStorage().onInterpreterClientStart(remoteInterpreterProcess);
                                                
                                            }
                                        }
                                        return interpreterProcess;
                                    }
                                    
                                    if (!interpreterProcess.isRunning()) { //如果远程执行器不运行了,跑异常;
                                        return new InterpreterResult(InterpreterResult.Code.ERROR,"Interpreter process is not running\n" + interpreterProcess.getErrorMessage());
                                    }
                                    interpreterProcess.callRemoteFunction(client -> {
                                        RemoteInterpreterResult remoteResult = client.interpret(sessionId, className, st, convert(context));{//RemoteInterpreteService.interpret()
                                            send_interpret(sessionId, className, st, interpreterContext);
                                            //上面发出请求; 这里等待结果;
                                            return recv_interpret();{//RemoteInterpreteService.recv_interpret()
                                                receiveBase(result, "interpret");
                                            }
                                        }
                                        return convert(remoteResult);
                                    });
                                        
                                    
                                }
                                
                                return ret;
                            }
                        }
                        
                        Object jobResult = runningJob.getReturn();
                    }
                }
            }
            
            if (executionMode.equals("paragraph")) {
                while (!jobRunner.isJobSubmittedInRemote()) {
                    Thread.sleep(100);
                }
            }
        }
    }
  }



            


















