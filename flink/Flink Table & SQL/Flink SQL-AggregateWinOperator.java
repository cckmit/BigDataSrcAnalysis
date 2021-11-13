



// 通用Task 运行框架;
Task.run(){
    doRun();{
        TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());
        Environment env = new RuntimeEnvironment();
        invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
        
        invokable.invoke();{// OneInputStreamTask.invoke() -> 调用父类StreamTask.invoke()方法;
            StreamTask.invoke(){
                beforeInvoke(); // 真正消费数据前,先进行初始化
                // 在这里里面循环接受消息,并运行;
                runMailboxLoop();{//StreamTask.runMailboxLoop()
                    mailboxProcessor.runMailboxLoop();{// MailboxProcessor.runMailboxLoop()
                        boolean hasAction = processMail(localMailbox);{//源码中while(processMail(localMailbox)); 没有Mail时会一直阻塞在此,有消息才
                           return isMailboxLoopRunning();// return mailboxLoopRunning;
                        }
                        
                        while (hasAction = processMail(localMailbox)) {//阻塞在条件判断的方法中, 判断还处于Running状态时,会进入下面的 runDefaultAction()
                            mailboxDefaultAction.runDefaultAction(defaultActionContext); {
                                this.processInput();{
                                    StreamTask.processInput();{
                                        // 这里不同的 inputProcessor:StreamInputProcessor 实现类,进行不同处理;
                                        InputStatus status = inputProcessor.processInput();{ //StreamoneInputProcessor.processInput()
                                            InputStatus status = input.emitNext(output);{ // StreamTaskNetworkInput.emitNext()
                                                
                                                
                                            }
                                        }
                                    }
                                
                                    SourceStreamTask.processInput(controller);{
                                        
                                    }
                                }
                            }
                        }
                    }
                }
                
                // 结束消费和处理后,资源释放;
                afterInvoke();
                
            }
        }
    }
}
 

accumulate:19, MultiArgSumAggFunc (com.bigdata.streaming.flink.mystudy.streamsql.sqlfunction.windowed.simpletest)
accumulate:-1, GroupingWindowAggsHandler$56
processElement:344, WindowOperator (org.apache.flink.table.runtime.operators.window)
emitRecord:173, OneInputStreamTask$StreamTaskNetworkOutput (org.apache.flink.streaming.runtime.tasks)


// GroupWindowAggregate(groupBy=...) 线程
StreamTaskNetworkInput.emitNext(DataOutput<T> output){
    while (true) {
        if (result.isFullRecord()) {
            processElement(deserializationDelegate.getInstance(), output);{
                if (recordOrMark.isRecord()){ // 是数据,直接往后流;
                    output.emitRecord(recordOrMark.asRecord());{//OneInputStreamTask$StreamTaskNetworkOutput.emitRecord()
                        operator.processElement(record);{
                            // 对于窗口算子: 
                            WindowOperator.processElement(record){
                                
                            }
                            
                        }
                    }
                    
                }else if (recordOrMark.isWatermark()) { // 是WM, 窗口聚合就是Watermark;
                    
                    statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);{ //StatusWatermarkValve.inputWatermark()
                        
                        if (watermarkMillis > channelStatuses[channelIndex].watermark) {
                            channelStatuses[channelIndex].watermark = watermarkMillis;
                            
                            findAndOutputNewMinWatermarkAcrossAlignedChannels();{
                                output.emitWatermark(new Watermark(lastOutputWatermark));
                                    -> operator.processWatermark(watermark);
                                        -> timeServiceManager.advanceWatermark(mark); -> service.advanceWatermark(watermark.getTimestamp());{// InternalTimerServiceImpl.advanceWatermark()
                                            while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
                                                eventTimeTimersQueue.poll();
                                                triggerTarget.onEventTime(timer);{//WindowOperator.onEventTime()
                                                
                                                    WindowOperator.onEventTime(){
                                                        if (triggerContext.onEventTime(timer.getTimestamp())) {
                                                            // 正在的窗口聚合算子 生成结果逻辑; ?
                                                            emitWindowResult(triggerContext.window);{//AggregateWindowOperator.emitWindowResult()
                                                                windowFunction.prepareAggregateAccumulatorForEmit(window);
                                                                BaseRow aggResult = aggWindowAggregator.getValue(window);
                                                                BaseRow previousAggResult = previousState.value();
                                                                if (previousAggResult != null) {
                                                                    collector.collect(reuseOutput);
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
                }else if (recordOrMark.isLatencyMarker()) {
                    
                }
                
            }
        }
    }
    
}




// 窗口核心逻辑1: 对每个事件(Record)的累加 : 时间窗口groupBy窗口,兼是此 WindowOperator算子;
WindowOperator.processElement(record){
    if (windowAssigner.isEventTime()) {
        timestamp = inputRow.getLong(rowtimeIndex);
    }
    // 计算出受影响的窗口
    Collection<W> affectedWindows = windowFunction.assignStateNamespace(inputRow, timestamp);
    for (W window : affectedWindows) {
        windowState.setCurrentNamespace(window);
        windowAggregator.setAccumulators(window, acc);
        if (BaseRowUtil.isAccumulateMsg(inputRow)) {
            // 核心的窗口累加逻辑;
            windowAggregator.accumulate(inputRow);{ //GroupingWindowAggsHandler$56
                // 是由 StreamExecGroupWindowAggregateBase.createAggsHandler() 生成的, 其里面可能包括sum,count,avg,last_value,MulitArgSum等多个函数;
                // 里面多个聚合函数, 应该是每个函数,都会调用下 .accumulate();
                windowAggregator = {GroupingWindowAggsHandler$56@10703} 
                     agg0_sum = 1
                     agg0_sumIsNull = false
                     agg1_count1 = 1
                     agg1_count1IsNull = false
                     function_org$apache$flink$table$planner$functions$aggfunctions$LastValueAggFunction$IntLastValueAggF = {LastValueAggFunction$IntLastValueAggFunction@10752} "IntLastValueAggFunction"
                     function_com$bigdata$streaming$flink$mystudy$streamsql$sqlfunction$windowed$simpletest$MultiArgSumAg = {MultiArgSumAggFunc@10753} "MultiArgSumAggFunc"
                     converter = {DataFormatConverters$PojoConverter@10754} 
                     converter = {DataFormatConverters$PojoConverter@10755} 
                     agg2_acc_internal = {GenericRow@10756} "(+|1,-9223372036854775808)"
                     converter = {DataFormatConverters$PojoConverter@10759}                 
                
                MultiArgSumAg.accumulate(){
                    
                }
                
                IntLastValueAggFunction.accumulate(){
                    
                }
            }
        }else{
            windowAggregator.retract(inputRow);
        }
        acc = windowAggregator.getAccumulators();
        windowState.update(acc);
    }
    
}


// 2 窗口算子核心逻辑2: 窗口结束,输出;
WindowOperator.onEventTime(){
    if (triggerContext.onEventTime(timer.getTimestamp())) {
        // 正在的窗口聚合算子 生成结果逻辑; ?
        emitWindowResult(triggerContext.window);{//AggregateWindowOperator.emitWindowResult()
            windowFunction.prepareAggregateAccumulatorForEmit(window);
            BaseRow aggResult = aggWindowAggregator.getValue(window);
            BaseRow previousAggResult = previousState.value();
            if (previousAggResult != null) {
                collector.collect(reuseOutput);
            }
        }
    }
}







