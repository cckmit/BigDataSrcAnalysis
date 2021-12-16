

StreamTask.invoke(){
    
    
}



open:51, StatefulFunction$class (org.apache.flink.streaming.api.scala.function)
open:572, KeyedStream$$anon$2 (org.apache.flink.streaming.api.scala)
openFunction:36, FunctionUtils (org.apache.flink.api.common.functions.util)
open:102, AbstractUdfStreamOperator (org.apache.flink.streaming.api.operators)
initializeStateAndOpen:990, StreamTask (org.apache.flink.streaming.runtime.tasks)
lambda$beforeInvoke$0:453, StreamTask (org.apache.flink.streaming.runtime.tasks)
run:-1, 528981627 (org.apache.flink.streaming.runtime.tasks.StreamTask$$Lambda$425)
runThrowing:94, StreamTaskActionExecutor$SynchronizedStreamTaskActionExecutor (org.apache.flink.streaming.runtime.tasks)
beforeInvoke:448, StreamTask (org.apache.flink.streaming.runtime.tasks)
invoke:460, StreamTask (org.apache.flink.streaming.runtime.tasks)
doRun:708, Task (org.apache.flink.runtime.taskmanager)
run:533, Task (org.apache.flink.runtime.taskmanager)
run:748, Thread (java.lang)




Task.run(){
    doRun();{
        TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());
        Environment env = new RuntimeEnvironment();
        invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
        
        // 这里invokable的实例对象是: OneInputStreamTask;是StreamTask的继承类:
        // StreamTask的还有其他3个继承类: AbstractTwoInputStreamTask, SourceReaderStreamTask, SourceStreamTask; ?
        invokable.invoke();{// OneInputStreamTask.invoke() -> 调用父类StreamTask.invoke()方法;
            StreamTask.invoke(){
                
                // 真正消费数据前,先进行初始化
                beforeInvoke();{ //StreamTask.beforeInvoke()
                    actionExecutor.runThrowing(() -> {
                        initializeStateAndOpen();{
                            StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
                            for (StreamOperator<?> operator : allOperators) {
                                if (null != operator) {
                                    operator.initializeState();
                                    /**
                                    * 在这里,把每个Operator打开; 
                                    */
                                    operator.open();{ // StatefulFunction.open()
                                        
                                        val info = new ValueStateDescriptor[S]("state", stateSerializer)
                                        // 创建一个 HeapValueState 作为(不同批次间的)状态;
                                        
                                        state = getRuntimeContext().getState(info)
                                    }
                                }
                            }
                        }
                    }
                }
                
                // 在这里里面循环接受消息,并运行;
                runMailboxLoop();
                
                // 结束消费和处理后,资源释放;
                afterInvoke();
                
            }
        }
    }
}

    runMailboxLoop();{
        mailboxProcessor.runMailboxLoop();{//MailboxProcessor.runMailboxLoop()
            final MailboxController defaultActionContext = new MailboxController(this);
            while (processMail(localMailbox)) {
                mailboxDefaultAction.runDefaultAction(defaultActionContext); {// 实现类: StreamTask.runDefaultAction()
                    // 这个mailboxDefaultAction()函数,即 new MailboxProcessor(this::processInput, mailbox, actionExecutor) 方法的 this::processInput 方法;
                    StreamTask.processInput(){
                        InputStatus status = input.emitNext(output);{//StreamTaskNetworkInput.
                            while (true) {
                                DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
                                if (result.isFullRecord()) {//isFullRecord字段为true;
                                    processElement(deserializationDelegate.getInstance(), output);{//StreamTaskNetworkInput.
                                        if (recordOrMark.isRecord()){ //return getClass() == StreamRecord.class;
                                            output.emitRecord(recordOrMark.asRecord());{
                                                
                                                OneInputStreamTask.StreamTaskNetworkOutput.emitRecord(){
                                                    operator.processElement(record);{
                                                        output.collect(element.replace(userFunction.map(element.getValue())));{
                                                            KeyedStream$$anon$2.map(){
                                                                applyWithState(in, cleanFun);{  // applyWithState(in: I, fun: (I, Option[S]) => (O, Option[S])): O
                                                                    val (o, s: Option[S]) = fun(in, Option(state.value())){
                                                                        // 这里就是用户定义的 计算逻辑: 累加求和;
                                                                        
                                                                    }
                                                                    
                                                                    s match {
                                                                        case Some(v) => state.update(v)
                                                                        case None => state.update(null.asInstanceOf[S])
                                                                    }
                                                                    
                                                                }
                                                                
                                                            }
                                                        }
                                                    }
                                                }
                                                
                                            }
                                            
                                            
                                        } else if (recordOrMark.isWatermark()) { // return getClass() == Watermark.class;
                                            statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);{//StatusWatermarkValue.inputWatermark()
                                               
                                            }
                                        } else if (recordOrMark.isLatencyMarker()) { // return getClass() == LatencyMarker.class;
                                            output.emitLatencyMarker(recordOrMark.asLatencyMarker());
                                        } else if (recordOrMark.isStreamStatus()) {
                                            statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), lastChannel);
                                        } else {
                                            throw new UnsupportedOperationException("Unknown type of StreamElement");
                                        } 
                                    }
                                }
                            }
                        }
                    }
                    
                }
            }
        }
    }




