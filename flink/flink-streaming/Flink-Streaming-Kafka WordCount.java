

val stream: DataStream[String] = env.addSource(kafkaSource)
    stream.flatMap(new MySplitFlatMap)  // value.split(" ").foreach out.collect()
      .filter(new MyFilter)             // !value.isEmpty
      .map(new MyMapFunc)               // (value.trim.toLowerCase(),1)
      .keyBy(new MyKeySelector)         // value._1
      .reduce(new MyReduceFunc).setParallelism(2)   // (value1._1,value1._2+ value2._2)
      .print()

// 分两个Task;

FlinkKafkaConsumerBase.run(){
    while (running) {
        final ConsumerRecords<byte[], byte[]> records = handover.pollNext();
        for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
            emitRecord(value, partition, record.offset(), record);{//KafkaFetcher
                // StreamSourceContexts.NonTimestampContext.collect() -> Operator.CopyingChainingOutput.collect()
                pushToOperator(record);{// CopyingChainingOutput.
                    StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
                    operator.processElement(copy);{ // StreamFlatMap.processElement() -> 进入 MyMapFunc
                        
                        /** 第一个算子: flatMap  */
                        MySplitFlatMap.flatMap(value: String, out: Collector[String]){
                            for(ele <- value.split(" ")){
                                out.collect(ele);{// TimestampedCollector.collect() 
                                    // StreamSourceContexts.NonTimestampContext.collect() -> Operator.CopyingChainingOutput.collect()
                                        pushToOperator(record);{// CopyingChainingOutput.pushToOperator()
                                            StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
                                            operator.processElement(copy);{//StreamFilter.processElement()
                                                
                                                /** 进入第二个tranformer 算子: filter() */
                                                if(userFunction.filter(element.getValue()){ //userFunction.filter 实现如下: 
                                                    MyFilter.filter(value: String): Boolean = {
                                                        !value.isEmpty
                                                    }
                                                }){
                                                    output.collect(element);{// CountingOutput.collect()
                                                        // CopyingChainingOutput.collect() 
                                                            pushToOperator(record);{// CopyingChainingOutput.pushToOperator()
                                                                StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
                                                                operator.processElement(copy);{//StreamMap.processElement()
                                                                
                                                                    /** 第三个算子: map() */
                                                                    StreamRecord<X> newRecord = element.replace(userFunction.map(element.getValue()){
                                                                        MyMapFunc.map(value: String): (String,Int) ={
                                                                            (value.trim.toLowerCase(),1)
                                                                        }
                                                                    });
                                                                    output.collect(newRecord);{// CountingOutput.collect()
                                                                        // 因为后面接的是keyBy()算子, 需要shuffle, 这里进Writer 算子;
                                                                        output.collect(record);{// RecordWriterOutput.collect()
                                                                            pushToRecordWriter(record);{//RecordWriterOutput.
                                                                                serializationDelegate.setInstance(record); // 序列化
                                                                                recordWriter.emit(serializationDelegate);{
                                                                                    int nextChannelToSendTo = channelSelector.selectChannel(record); //计算分区?
                                                                                    emit(record, nextChannelToSendTo) -> copyFromSerializerToTargetChannel(targetChannel);
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
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}




//Flink Shuffle的 流程: RecordWriterOutput.collect()

output.collect(record);{//RecordWriterOutput.collect()
    if (this.outputTag != null) return;
    pushToRecordWriter(record);{//RecordWriterOutput.
        serializationDelegate.setInstance(record);
        recordWriter.emit(serializationDelegate);{
            int nextChannelToSendTo = channelSelector.selectChannel(record);{// RebalancePartitioner.selectChannel() 决定分区?
                // 当 keyBy()算子时: 
                KeyGroupStreamPartitioner.selectChannel(StreamRecord<T>> record){
                    key = keySelector.getKey(record.getInstance().getValue()); 
                    return KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfChannels);{
                        // return computeOperatorIndexForKeyGroup(maxParallelism, parallelism, assignToKeyGroup(key, maxParallelism));
                            return MathUtils.murmurHash(keyHash) % maxParallelism;
                    }
                }
                
                RebalancePartitioner.selectChannel(){
                    nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
                    return nextChannelToSendTo;
                }
                
            }
            emit(record, nextChannelToSendTo);{//ChannelSelectorRecordWriter.emit()
                serializer.serializeRecord(record);
                
                // 从序列化器中,把record对应bytes copy到目标 Channel中;
                boolean pruneTriggered = copyFromSerializerToTargetChannel(targetChannel);{// RecordWriter.copyFromSerializerToTargetChannel()
                    
                    // 一个channel就是一个BufferBuilder; 
                    BufferBuilder bufferBuilder = getBufferBuilder(targetChannel);{//ChannelSelectorRecordWriter.getBufferBuilder()
                        if (bufferBuilders[targetChannel] != null) {
                            return bufferBuilders[targetChannel];// 初始化时创建数组长度为1; 
                        }else{ //运行过程中发现 channel 少了就新建channel;
                            return requestNewBufferBuilder(targetChannel);
                        }
                    }
                    
                    SerializationResult result = serializer.copyToBufferBuilder(bufferBuilder);{// SpanningRecordSerializer.
                        targetBuffer.append(dataBuffer);
                        targetBuffer.commit();
                        return getSerializationResult(targetBuffer);
                    }
                    
                    
                    while (result.isFullBuffer()) {
                        finishBufferBuilder(bufferBuilder);
                    }
                    if (flushAlways) flushTargetPartition(targetChannel);
                }
                
                if (pruneTriggered) { //
                    serializer.prune();
                }
            }
        }
    }
}




// Window(TumblingEventTimeWindows(3000), EventTimeTrigger, CoGroupWindowFunction) -> Map -> Filter -> Sink: Print to Std. Err (2/4) 线程: 
Task.run(){
    doRun();{
        TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());
        Environment env = new RuntimeEnvironment();
        invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
        
        // 这里invokable的实例对象是: OneInputStreamTask;是StreamTask的继承类:
        // StreamTask的还有其他3个继承类: AbstractTwoInputStreamTask, SourceReaderStreamTask, SourceStreamTask; ?
        invokable.invoke();{// OneInputStreamTask.invoke() -> 调用父类StreamTask.invoke()方法;
            StreamTask.invoke(){
                beforeInvoke(); // 真正消费数据前,先进行初始化
                // 在这里里面循环接受消息,并运行;
                runMailboxLoop();{
                    mailboxProcessor.runMailboxLoop();{
                        boolean hasAction = processMail(localMailbox);{//源码中while(processMail(localMailbox)); 没有Mail时会一直阻塞在此,有消息才
                            if (!mailbox.createBatch(){// TaskMailboxImpl.createBatch()
                                if (!hasNewMail) {
                                    return !batch.isEmpty();
                                }
                                
                            }) {
                                return true;
                            }
                            
                            while (isDefaultActionUnavailable() && isMailboxLoopRunning()) { //循环阻塞在此,等到Mail消息; 有新消息才会进入while()循环中的 runDefaultAction();
                                // 阻塞方法, 一直等到直到 queue不为空,取出了一个 headMail:Mail
                                mailbox.take(MIN_PRIORITY).run();{//TaskMailboxImpl.take(int priority)
                                    Mail head = takeOrNull(batch, priority);
                                    while ((headMail = takeOrNull(queue, priority)) == null) {
                                        // 接受线程信号; 阻塞在此, 一旦("OutputFlusher for Source")线程发出 notEmpty.signal()信号,就结束等待,处理消息;
                                        notEmpty.await();
                                    }
                                    hasNewMail = !queue.isEmpty(); // 用于 createBatch()中判断;
                                    return headMail;
                                }
                            }
                            return isMailboxLoopRunning();// return mailboxLoopRunning;
                        }
                        
                        while (hasAction = processMail(localMailbox)) {//阻塞在条件判断的方法中, 判断还处于Running状态时,会进入下面的 runDefaultAction()
                            mailboxDefaultAction.runDefaultAction(defaultActionContext); {
                                this.processInput();{
                                    StreamTask.processInput();{
                                        // 这里不同的 inputProcessor:StreamInputProcessor 实现类,进行不同处理;
                                        InputStatus status = inputProcessor.processInput();{
                                            StreamoneInputProcessor.processInput();{}
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
 


"Legacy Source Thread - Source: Custom File Source "线程

"Source:Custom File Source"线程 进行读取和处理;

"OutputFlusher for Source: Custom Source -> FlatMap -> Filter ->Map " 线程;

"Keyed Reduce -> Sink: Print to Std.Out" 线程


// RecordWriter.OutputFlusher 的循环flush()线程: "OutputFlusher for Source: Custom Source -> FlatMap -> Filter " 线程


"OutputFlusher for Source: Custom Source -> FlatMap -> Filter ->Map " 线程: 
RecordWriter.OutputFlusher.run(){
    while(running){
        Thread.sleep(timeout); //默认100ms, 由 bufferTimeout
        flushAll();{ targetPartition.flushAll();{
            
            ReleaseOnConsumptionResultPartition.flushAll();{
                for (ResultSubpartition subpartition : subpartitions) {
                    subpartition.flush();{//PipelineSubpartition.flush()
                        // 当buffers 里面为空时, 跳出方法, 继续100ms的死循环; 
                        if (buffers.isEmpty()) { return;} 
                        
                        notifyDataAvailable = !flushRequested && buffers.size() == 1 && buffers.peek().isDataAvailable();
                        flushRequested = flushRequested || buffers.size() > 1 || notifyDataAvailable;
                        if (notifyDataAvailable) {
                            notifyDataAvailable();{//PipelinedSubpartition. 真正通知数据到了, 传递给下游线程; 
                                readView.notifyDataAvailable();{availabilityListener.notifyDataAvailable();{
                                    notifyChannelNonEmpty();-> inputGate.notifyChannelNonEmpty(this); queueChannel(checkNotNull(channel));{
                                        CompletableFuture<?> toNotify = availabilityHelper.getUnavailableToResetAvailable();
                                        toNotify.complete(null);{
                                            postComplete();-> h.tryFire(NESTED));{
                                                d.uniRun(a = src, fn, mode > 0 ? null : this))
                                                // -> 业务代码; jointFuture.thenRun(suspendedDefaultAction::resume);{
                                                    resume(){ //方法的执行; 
                                                        
                                                    }
                                                }
                                                
                                            }
                                        }
                                    }
                                }}
                            } 
                        }    
                    }
                }
            }
        }}
    }
}



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
 
