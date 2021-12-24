






/** 4.0 TM
*
*/




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
                
                // 真正消费数据前,先进行初始化
                beforeInvoke();
                
                // 在这里里面循环接受消息,并运行;
                runMailboxLoop();
                
                // 结束消费和处理后,资源释放;
                afterInvoke();
                
            }
        }
    }
}



Task.run(){
    doRun();{
        TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());
        Environment env = new RuntimeEnvironment();
        invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
        
        // 这里invokable的实例对象是: OneInputStreamTask;是StreamTask的继承类:
        // StreamTask的还有其他3个继承类: AbstractTwoInputStreamTask, SourceReaderStreamTask, SourceStreamTask; ?
        invokable.invoke();{// OneInputStreamTask.invoke() -> 调用父类StreamTask.invoke()方法;
            StreamTask.invoke(){
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
                                                        output.emitRecord(recordOrMark.asRecord());
                                                    } else if (recordOrMark.isWatermark()) { // return getClass() == Watermark.class;
                                                        statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);{//StatusWatermarkValue.inputWatermark()
                                                            // 重要逻辑: 基于水位的过滤?
                                                            if (watermark.getTimestamp() > channelStatuses[channelIndex].watermark) {
                                                                channelStatuses[channelIndex].watermark = watermarkMillis;
                                                                findAndOutputNewMinWatermarkAcrossAlignedChannels();{
                                                                    if (hasAlignedChannels && newMinWatermark > lastOutputWatermark) {
                                                                        lastOutputWatermark = newMinWatermark;
                                                                        output.emitWatermark(new Watermark(lastOutputWatermark));{//OneInputStreamTask.StreamTaskNetworkOutput 
                                                                            operator.processWatermark(watermark){//AbstractStreamOperator
                                                                                if (timeServiceManager != null) {
                                                                                    timeServiceManager.advanceWatermark(mark);{//InternalTimeServiceManager
                                                                                        for (InternalTimerServiceImpl<?, ?> service : timerServices.values()) {
                                                                                            service.advanceWatermark(watermark.getTimestamp());{//InternalTimeServiceManager
                                                                                                while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
                                                                                                    eventTimeTimersQueue.poll();
                                                                                                    triggerTarget.onEventTime(timer);{// WindowOperator.onEventTime
                                                                                                        TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());
                                                                                                        if (triggerResult.isFire()) {
                                                                                                            emitWindowContents(triggerContext.window, contents);{//WindowOperator.
                                                                                                                timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
                                                                                                                userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);{//InternalIterableWindowFunction.
                                                                                                                    wrappedFunction.apply(key, window, input, out);
                                                                                                                }
                                                                                                            }
                                                                                                        }
                                                                                                    }
                                                                                                }
                                                                                            }
                                                                                        }
                                                                                    }
                                                                                }
                                                                                output.emitWatermark(mark);
                                                                                
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
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
            }
        }
    }
}



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






// 对于 直接从Kafka消费的数据,或者直接fromElements 生成的数据: 

// 共同的函数逻辑:  LegacySourceFunctionThread.run() -> StreamSource.run()-> userFunction.run(ctx);

// Legacy Source: 遗留的Source: 遗留的数据源?
SourceStreamTask.LegacySourceFunctionThread.run(){
    headOperator.run(getCheckpointLock(), getStreamStatusMaintainer(), operatorChain);{//StreamSource
        run(lockingObject, streamStatusMaintainer, output, operatorChain);{
            this.ctx = StreamSourceContexts.getSourceContext();
            userFunction.run(ctx);{//FlinkKafkaConsumerBase.run()
                
                FlinkKafkaConsumerBase.run(){
                    this.kafkaFetcher = createFetcher();
                    kafkaFetcher.runFetchLoop();{
                        final Handover handover = this.handover;
                        // 启动Kafka消费线程, 持续从Kafka消费数据;
                        consumerThread.start();
                        
                        while (running) {
                            //consumerThread线程拉取的kafka数据存放在handover这个中间容器/缓存中;
                            final ConsumerRecords<byte[], byte[]> records = handover.pollNext();
                            for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitionStates()) {
                                List<ConsumerRecord<byte[], byte[]>> partitionRecords =records.records(partition.getKafkaPartitionHandle());
                                for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                                    
                                    // 这里是处理业务逻辑的核心方法: 
                                    emitRecord(value, partition, record.offset(), record);{//KafkaFetcher.
                                        emitRecordWithTimestamp(record, partition, offset, consumerRecord.timestamp());{//AbstractFetcher.
                                            sourceContext.collectWithTimestamp(record, timestamp);{//StreamSourceContexts.
                                                collect(element);{//StreamSourceContexts.NonTimestampContext
                                                    output.collect(reuse.replace(element));{//AbstractStreamOperator.CountingOutput
                                                        numRecordsOut.inc();
                                                        output.collect(record);{// Operator.CopyingChainingOutput.
                                                            pushToOperator(record);{//
                                                                StreamRecord<T> castRecord = (StreamRecord<T>) record;
                                                                StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
                                                                operator.processElement(copy);{//StreamMap.
                                                                    // 这个element:StreamRecord(value:T,hasTimestamp:Boolean,timestamp:Long);
                                                                    // 这个 userFunction 即用户定义的函数体: 相当于Java中的 @Interface 接口的函数实现; 
                                                                    // 这个userFunction 也可以是MapFunction的实现类: MyMapFuncImpl, 则直接调用 MyMapFuncImpl.map()进行处理;
                                                                    X element = userFunction.map(element.getValue());{// 
                                                                        
                                                                    }
                                                                    
                                                                    output.collect(element.replace(element){//StreamRecord.replace() 将原来.value字段替换成新的值,避免新new StreamRecord 的转换成本;
                                                                        this.value = (T) element; 
                                                                        return (StreamRecord<X>) this; //这里就是类型装换了, 因为原来可能是: value:String 类型 => value:(String,String) 
                                                                    });{//AbstractStreamOperator.CountingOutput.collect()
                                                                        numRecordsOut.inc(); //所谓的CountingOutput,就是这里简单对StreamRecord技术+1;
                                                                        output.collect(record);{//OperatorChain.WatermarkGaugeExposingOutput 的几个接口
                                                                            // outputs: 即addSink()所添加的输出; 
                                                                            OperatorChain.BroadcastingOutputCollector.collect(record){
                                                                                for (Output<StreamRecord<T>> output : outputs) {
                                                                                    output.collect(record);
                                                                                }    
                                                                            }
                                                                            
                                                                            // 当该算子后面还接另一个filter/map()/等算子时,
                                                                            OperatorChain.CopyingChainingOutput.collect(){
                                                                                if (this.outputTag != null) return;
                                                                                //将该Record发给 Operator操作器;
                                                                                pushToOperator(record);{//CopyingChainingOutput.
                                                                                    StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
                                                                                    operator.processElement(copy);{ //对于FilterStream的苏州尼
                                                                                        // 这里的userFunction: FilterFunction<T>的实现类 MyFilterFuncImpl的实例对象;
                                                                                        if (userFunction.filter(element.getValue())) {
                                                                                            output.collect(element);
                                                                                        }
                                                                                    }
                                                                                }
                                                                            }
                                                                            
                                                                            // Shuffle的写出的,就用这个Output;
                                                                            RecordWriterOutput[implements WatermarkGaugeExposingOutput].collect(){
                                                                                
                                                                            }
                                                                            
                                                                            // DirectedOutput[implements WatermarkGaugeExposingOutput]: 
                                                                            
                                                                            // ChiningOutput [implements WatermarkGaugeExposingOutput]: 
                                                                            
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            partitionState.setOffset(offset);
                                        }
                                    }
                                }
                            }
                            
                        }
                    }
                    
                }
                
                FromElementsFunction.run(){ // 
                    ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
                    // 循环遍历规定数量(numElements)的元素, 这个数是由Driver分配决定?
                    while (isRunning && numElementsEmitted < numElements) {
                        T next = serializer.deserialize(input);
                        // 所谓的collect()就是一层层的往下面传;当需要shuffle或者输出了,就停止;
                        ctx.collect(next);{//StreamSourceContexts.NonTimestampContext.collect()
                            //中间标准的装换, 最后还是调: element.replace(userFunction.map()); 或者 userFunction.xxx(): map(),filter(),sum(),reduce()..
                            output.collect(reuse.replace(element));-> output.collect(record);->pushToOperator(record);-> operator.processElement(copy);{
                                output.collect(element.replace(userFunction.map(element.getValue())));
                            }
                        }
                        numElementsEmitted++;
                    }
                }
                
            }
        }
    }
    completionFuture.complete(null);
}




// 4.2 对于KafkaConsumer的逻辑: 

FlinkKafkaConsumerBase.run(){
    this.kafkaFetcher = createFetcher();
    kafkaFetcher.runFetchLoop();{
        final Handover handover = this.handover;
        // 启动Kafka消费线程, 持续从Kafka消费数据;
        consumerThread.start();
        
        while (running) {
            //consumerThread线程拉取的kafka数据存放在handover这个中间容器/缓存中;
            final ConsumerRecords<byte[], byte[]> records = handover.pollNext();
            for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitionStates()) {
                List<ConsumerRecord<byte[], byte[]>> partitionRecords =records.records(partition.getKafkaPartitionHandle());
                for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                    
                    // 这里是处理业务逻辑的核心方法: 
                    emitRecord(value, partition, record.offset(), record);{//KafkaFetcher.
                        emitRecordWithTimestamp(record, partition, offset, consumerRecord.timestamp());{//AbstractFetcher.
                            sourceContext.collectWithTimestamp(record, timestamp);{//StreamSourceContexts.
                                collect(element);{//StreamSourceContexts.NonTimestampContext
                                    output.collect(reuse.replace(element));{//AbstractStreamOperator.CountingOutput
                                        numRecordsOut.inc();
                                        output.collect(record);{// Operator.CopyingChainingOutput.
                                            pushToOperator(record);{//
                                                StreamRecord<T> castRecord = (StreamRecord<T>) record;
                                                StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
                                                operator.processElement(copy);{//StreamMap.
                                                    // 这个element:StreamRecord(value:T,hasTimestamp:Boolean,timestamp:Long);
                                                    // 这个 userFunction 即用户定义的函数体: 相当于Java中的 @Interface 接口的函数实现; 
                                                    // 这个userFunction 也可以是MapFunction的实现类: MyMapFuncImpl, 则直接调用 MyMapFuncImpl.map()进行处理;
                                                    X element = userFunction.map(element.getValue());{// 
                                                        
                                                    }
                                                    
                                                    output.collect(element.replace(element){//StreamRecord.replace() 将原来.value字段替换成新的值,避免新new StreamRecord 的转换成本;
                                                        this.value = (T) element; 
                                                        return (StreamRecord<X>) this; //这里就是类型装换了, 因为原来可能是: value:String 类型 => value:(String,String) 
                                                    });{//AbstractStreamOperator.CountingOutput.collect()
                                                        numRecordsOut.inc(); //所谓的CountingOutput,就是这里简单对StreamRecord技术+1;
                                                        output.collect(record);{//OperatorChain.WatermarkGaugeExposingOutput 的几个接口
                                                            // outputs: 即addSink()所添加的输出; 
                                                            OperatorChain.BroadcastingOutputCollector.collect(record){
                                                                for (Output<StreamRecord<T>> output : outputs) {
                                                                    output.collect(record);
                                                                }    
                                                            }
                                                            
                                                            // 当该算子后面还接另一个filter/map()/等算子时,
                                                            OperatorChain.CopyingChainingOutput.collect(){
                                                                if (this.outputTag != null) return;
                                                                //将该Record发给 Operator操作器;
                                                                pushToOperator(record);{//CopyingChainingOutput.
                                                                    StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
                                                                    operator.processElement(copy);{ //对于FilterStream的苏州尼
                                                                        // 这里的userFunction: FilterFunction<T>的实现类 MyFilterFuncImpl的实例对象;
                                                                        if (userFunction.filter(element.getValue())) {
                                                                            output.collect(element);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            
                                                            // Shuffle的写出的,就用这个Output;
                                                            RecordWriterOutput[implements WatermarkGaugeExposingOutput].collect(){
                                                                
                                                            }
                                                            
                                                            // DirectedOutput[implements WatermarkGaugeExposingOutput]: 
                                                            
                                                            // ChiningOutput [implements WatermarkGaugeExposingOutput]: 
                                                            
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            partitionState.setOffset(offset);
                        }
                    }
                }
            }
            
        }
    }
    
}



// 4.3 对于 FromElements的计算逻辑: 
FromElementsFunction.run(){ // 
    ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
    // 循环遍历规定数量(numElements)的元素, 这个数是由Driver分配决定?
    while (isRunning && numElementsEmitted < numElements) {
        T next = serializer.deserialize(input);
        // 所谓的collect()就是一层层的往下面传;当需要shuffle或者输出了,就停止;
        ctx.collect(next);{//StreamSourceContexts.NonTimestampContext.collect()
            //中间标准的装换, 最后还是调: element.replace(userFunction.map()); 或者 userFunction.xxx(): map(),filter(),sum(),reduce()..
            output.collect(reuse.replace(element));-> output.collect(record);{//RecordWriterOutput.collect()
                if (this.outputTag != null) return;
                pushToRecordWriter(record);{//RecordWriterOutput.
                    serializationDelegate.setInstance(record);
                    recordWriter.emit(serializationDelegate);{
                        int nextChannelToSendTo = channelSelector.selectChannel(record);{// RebalancePartitioner.selectChannel() 决定分区?
                            nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
                            return nextChannelToSendTo;
                        }
                        emit(record, nextChannelToSendTo);{//ChannelSelectorRecordWriter.emit()
                            serializer.serializeRecord(record);
                            
                            boolean pruneTriggered = copyFromSerializerToTargetChannel(targetChannel);{// RecordWriter.copyFromSerializerToTargetChannel()
                                SerializationResult result = serializer.copyToBufferBuilder(bufferBuilder);
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
        }
        numElementsEmitted++;
    }
}

    // 下游子线程: Map -> Sink: Print to Std. Out

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
 
Task.run(){
    doRun();{
        TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());
        Environment env = new RuntimeEnvironment();
        invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
        
        // 这里invokable的实例对象是: OneInputStreamTask;是StreamTask的继承类:
        // StreamTask的还有其他3个继承类: AbstractTwoInputStreamTask, SourceReaderStreamTask, SourceStreamTask; ?
        invokable.invoke();{// OneInputStreamTask.invoke() -> 调用父类StreamTask.invoke()方法;
            StreamTask.invoke(){
                runMailboxLoop();{
                    mailboxProcessor.runMailboxLoop();{//MailboxProcessor.runMailboxLoop()
                        final MailboxController defaultActionContext = new MailboxController(this);
                        while (processMail(localMailbox)) {
                            mailboxDefaultAction.runDefaultAction(defaultActionContext); {// 实现类: StreamTask.runDefaultAction()
                                // 这个mailboxDefaultAction()函数,即 new MailboxProcessor(this::processInput, mailbox, actionExecutor) 方法的 this::processInput 方法;
                                StreamTask.processInput(){
                                    InputStatus status = input.emitNext(output);{//StreamTaskNetworkInput.
                                        // 这个地方循环遍历, 从缓存总依次读取每个record的 字节数组,反序列化后交给后面operator取处理;
                                        while (true) {
                                            DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
                                            if (result.isFullRecord()) {//isFullRecord字段为true;
                                                processElement(deserializationDelegate.getInstance(), output);{//StreamTaskNetworkInput.
                                                    if (recordOrMark.isRecord()){ //return getClass() == StreamRecord.class;
                                                        output.emitRecord(recordOrMark.asRecord());{//OneInputStreamTask.StreamTaskNetworkOutput.emitRecord()
                                                            operator.processElement(record);{ //StreamMap.
                                                                //这里执行用户定义 逻辑: userFunction.map();
                                                                output.collect(element.replace( userFunction.map(element.getValue())));
                                                            }
                                                        }
                                                    } else if (recordOrMark.isWatermark()) { // return getClass() == Watermark.class;
                                                        statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);
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
            }
        }
    }
}



StreamTask MailboxController  InputStatus StreamRecord
AbstractStreamOperator 
// 学习目的 
    - 了解 1个作业/算子 从数据read -> writer写出, 的完整流程和主要耗时;
















