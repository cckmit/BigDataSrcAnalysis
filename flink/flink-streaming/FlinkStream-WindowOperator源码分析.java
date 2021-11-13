

// Window().. GroupWindowAggregate(groupBy=...) 线程
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





# 1. Stream中 WindowOperator算子源码


WindowOperator.processElement(record){ // WindowOperator.processElement
    final Collection<W> elementWindows = windowAssigner.assignWindows();
    final K key = this.<K>getKeyedStateBackend().getCurrentKey(); // 取前面keyBy对应的key;
    // MergingWindowAssigner 是什么?
    if (windowAssigner instanceof MergingWindowAssigner) {

    }else{ //正常1个窗口是走这里;
        for (W window: elementWindows) {
            if (isWindowLate(window)) { // drop if the window is already late
                continue;
            }
            windowState.add(element.getValue());{// HeapListState.add()
                map.get(namespace);{// StateTable.get()
                    get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
                        -> stateMap.get(key, namespace);
                            -> CopyOnWriteStateMap.get(key,namespace): 这里会判断 key:DeviceKey.equals()
                }
            }
            TriggerResult triggerResult = triggerContext.onElement(element);{ //WindowOperator$Context.onElement()
                return trigger.onElement(element.getValue(), element.getTimestamp(), window, this);{
                    if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                    }else{ //
                        ctx.registerEventTimeTimer(window.maxTimestamp());
                            -> InternalTimerServiceImpl.registerEventTimeTimer(window, time);
                                -> eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
                        return TriggerResult.CONTINUE;
                    }
                }
            }
            if (triggerResult.isFire()) {
                ACC contents = windowState.get();
                emitWindowContents(window, contents);
            }
            if (triggerResult.isPurge()) {
                windowState.clear();
            }
            // 核心是 判断时间和Key是否相等:  timestamp == timer.getTimestamp() && key.equals(timer.getKey()
            registerCleanupTimer(window);{//WindowOperator.registerCleanupTimer()
                if (windowAssigner.isEventTime()) triggerContext.registerEventTimeTimer(cleanupTime);
                    -> InternalTimerServiceImpl.registerEventTimeTimer(){
                        eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));{//HeapPriorityQueueSet.add()
                            //HashMap.putIfAbsent(key:TimerHeapInternalTimer,value: TimerHeapInternalTimer)
                            getDedupMapForElement(element).putIfAbsent(element, element) == null && super.add(element);{ //HashMap.putIfAbsent() -> HashMap.putVal()
                                if (p.hash == hash && ((k = p.key) == key || (key != null && key.equals(k)))){ // 在这里会对 key:TimerHeapInternalTimer 进行.equal比较;
                                    {// TimerHeapInternalTimer.equals()
                                        if (this == o)  return true;
                                        if (o instanceof InternalTimer) {
                                            return timestamp == timer.getTimestamp() // 对时间戳比较;
                                                        && key.equals(timer.getKey()) // 对key:DeviceKey.equals()比较;
                                                        && namespace.equals(timer.getNamespace()); //对namespace 比较
                                        }
                                    }
                                    e = p;
                                }
                            }
                        }
                    }
            }

        }
    }
}


WindowOperator.onEventTime(){
    triggerContext.window = timer.getNamespace();
    TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

    if (triggerResult.isFire()) {
        ACC contents = windowState.get();
        if (contents != null) {
            emitWindowContents(triggerContext.window, contents);{
                userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);//InternalIterableWindowFunction.process()
                    -> wrappedFunction.apply(key, window, input, out);{// TestKeyedStateByPojo$3.apply()
                        // 这里就是用户自定义的 My WindowFunction.apply()
                        MyWindowFunction.apply(){
                            valueState.value();{// HeapValueState.value()
                                final V result = stateTable.get(currentNamespace);{
                                    return get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);{
                                        StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroupIndex);
                                        return stateMap.get(key, namespace); {//CopyOnWriteStateMap.get()
                                            // 里面方法对于 已存在的key:DeviceKey, 会调用 key.equals(eKey) 进行判断;
                                            // 如果 keyBy().window() 中需要用keyedState,要正确key需要对该Pojo: DeviceKey:  hashCode(),equals() 都重写,才能正确读取;
                                        }
                                    }
                                }
                            }
                        }
                    }
            }
        }
    }

    if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
        clearAllState(triggerContext.window, windowState, mergingWindows);{//WindowOperator.clearAllState()
            windowState.clear();
                -> stateTable.remove(currentNamespace);//StateTable.remove
                    -> remove(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);{//StateTable.remove
                        stateMap.remove(key, namespace); 
                    }
        }
    }

}





// Flink SQL的 WindowOperator:? 不一样? 窗口核心逻辑1: 对每个事件(Record)的累加 : 时间窗口groupBy窗口,兼是此 WindowOperator算子;
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







