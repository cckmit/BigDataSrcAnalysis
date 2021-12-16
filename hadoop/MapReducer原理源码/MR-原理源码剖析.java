
// 1. 提交线程： “main"线程： 核心方法: JobSubmitter.submitJobInternal()


UserDriver.main(){
    
    job.waitForCompletion(true){//Job: 
        if (state == JobState.DEFINE) {
            submit();{
                setUseNewAPI();//设置新的Api
                connect();{//创建Cluster对象,用于表示与目标FileSystem建立了连接;
                    if (cluster == null) {
                        
                        /*
                        *  创建Cluster对象, 主要是封装了clientProtocol :ClientProtocol =  LocalJobRunner();
                        */
                         cluster =  ugi.doAs(new PrivilegedExceptionAction<Cluster>() { 
                            return new Cluster(getConfiguration());{
                                this(null, conf);{
                                    this.conf = conf;
                                    this.ugi = UserGroupInformation.getCurrentUser();
                                    initialize(jobTrackAddr, conf);{
                                        initProviderList();
                                        // providerList 里面包括2个Provider的实现类: LocalClientProtocolProvider , YarnClientProtocolProvider
                                        for (ClientProtocolProvider provider : providerList) {
                                            if (jobTrackAddr == null) {
                                                clientProtocol = provider.create(conf);{
                                                    // ClientProtocolProvider的实现类: Local通信的Provider
                                                    LocalClientProtocolProvider.create(Configuration conf){
                                                        conf.setInt(JobContext.NUM_MAPS, 1);
                                                        return new LocalJobRunner(conf); 
                                                    }
                                                }
                                            }
                                            if (clientProtocol != null) {
                                                clientProtocolProvider = provider;
                                                client = clientProtocol;
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                         });
                    }
                }
                // 构建JobSubmitter , 主要是封装了: jtFs:目标文件系统(本地/hdfs), submitClient(通信客户端), hostAndPort: 地址端口;
                final JobSubmitter submitter = getJobSubmitter(cluster.getFileSystem(), cluster.getClient());
                
                status = ugi.doAs(new PrivilegedExceptionAction<JobStatus>() {
                    run(){return submitter.submitJobInternal(Job.this, cluster);{//JobSubmitter.submitJobInternal()
                        // 校验Jobs 的输出格式和合法性;
                        checkSpecs(job);
                        
                        //根据(哈希)算法创建本地 submitJobDir目录,用于收集/存放/提交 Job所需配置和资源;
                        Path jobStagingArea = JobSubmissionFiles.getStagingDir(cluster, conf);{
                            client.getStagingAreaDir();{
                                Path stagingRootDir = new Path(conf.get(JTConfig.JT_STAGING_AREA_ROOT, "/tmp/hadoop/mapred/staging"));
                                user = ugi.getShortUserName() + rand.nextInt(Integer.MAX_VALUE);//username-randid;
                                return fs.makeQualified(new Path(stagingRootDir, user+"/.staging")).toString();
                            }
                        }
                        JobID jobId = submitClient.getNewJobID();{//LocalJobRunner.getNewJobID(): 以local+上面rand.nextInt(MAX)生成的随机数+0/n, 作为jobId;
                            return new org.apache.hadoop.mapreduce.JobID("local" + randid, ++jobid);
                        }
                        Path submitJobDir = new Path(jobStagingArea, jobId.toString());
                        
                        try{
                            copyAndConfigureFiles(job, submitJobDir);//创建上面的submitJobDir目录;
                            
                            /* 核心代码: 根据file文件数量, 创建相关MR任务的分片/分区: splits
                            *
                            */
                            int maps = writeSplits(job, submitJobDir);{//JobSubmitter.writeSplits()
                                if (jConf.getUseNewMapper()) {//mapred.mapper.new-api==true属性时,用新的api
                                    maps = writeNewSplits(job, jobSubmitDir);{
                                        List<InputSplit> splits = input.getSplits(job);{//TextInputFormat 调用父类FileInputFormat.getSplits()
                                            List<FileStatus> files = listStatus(job);{
                                                Path[] dirs = getInputPaths(job);{//FileInputFormat.getInputPaths()
                                                    String dirs = context.getConfiguration().get(INPUT_DIR, "");// FileInputFormat.setInputPaths() => mapreduce.input.fileinputformat.inputdir (INPUT_DIR) ,即传入Input路劲;
                                                    Path[] result = new Path[list.length];
                                                    return result;
                                                }
                                                
                                                // 根据是否递归,递归获取子文件
                                                boolean recursive = getInputDirRecursive(job);//INPUT_DIR_RECURSIVE参数(mapreduce.input.fileinputformat.input.dir.recursive)设定
                                            }
                                        }
                                        
                                        T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);
                                        Arrays.sort(array, new SplitComparator()); //安装InputSplit.length尽心排序,size大的靠前先算;
                                        
                                        // 这里向jobSubmitDir目录写入 [job.split,job.splitmetainfo]这两个文件;
                                        JobSplitWriter.createSplitFiles(jobSubmitDir, conf, jobSubmitDir.getFileSystem(conf), array);
                                        return array.length;
                                    }
                                }
                            }
                            conf.setInt(MRJobConfig.NUM_MAPS, maps);//将Splits分片数量,写入mapreduce.job.maps的属性;
                            
                            // 将conf:Configuration 配置信息写入到submitJobDir临时目录下的job.xml 文件中;
                            writeConf(conf, submitJobFile);{
                                // 在jobSubmitDir下面的job.xml,
                                /tmp/hadoop-86177/mapred/staging/861771675066360/.staging/job_local1675066360_0001/job.xml
                                FSDataOutputStream out = FileSystem.create(jtFs, jobFile, new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
                                conf.writeXml(out);{//Configuration.writeXml(OutputStream out)
                                    writeXml(String propertyName, Writer out){//Configuration.writeXml()
                                        Document doc = asXmlDocument(propertyName);
                                        DOMSource source = new DOMSource(doc);
                                        Transformer transformer = transFactory.newTransformer();
                                        transformer.transform(source, result);
                                    }
                                }
                            }
                            // 提交任务
                            status = submitClient.submitJob( jobId, submitJobDir.toString(), job.getCredentials());{//LocalJobRunner.submitJob()
                                Job job = new Job(JobID.downgrade(jobid), jobSubmitDir);{
                                    this.localJobFile = new Path(this.localJobDir, id + ".xml");
                                    file:/tmp/hadoop-86177/mapred/local/localRunner/86177/job_local1675066360_0001/job_local1675066360_0001.xml
                                    
                                    // 
                                    OutputStream out = localFs.create(localJobFile);
                                    conf.writeXml(out);{//Configuration.writeXml( OutputStream out)
                                        // 代码同上;
                                    }
                                    this.start();// 启动Job线程,执行LocalJobRunner.Job.run(), 启动"Thread-n"线程, 完成MR计算; 
                                    
                                }
                                job.job.setCredentials(credentials);
                                return job.status;
                            }
                        }finally{
                            jtFs.delete(submitJobDir, true); //清除 submitJobDir临时提交目录;
                        }
                    }}
                });
            }
        }
        if (verbose) {//打印状态;
            monitorAndPrintJob();
        }
    }
}

    // 1.1 核心代码: 对Input文件进行分片(分区)的逻辑
    FileInputFormat.getSplits(JobContext job):List<InputSplit> {
        
        long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));{
            //getFormatMinSplitSize() return 1;
            getMinSplitSize(job) == return job.getConfiguration().getLong(SPLIT_MINSIZE, 1L);
            return Math.max(1, 1)
        }
        
        // 求分片的最大值；
        long maxSize = getMaxSplitSize(job);{
            // 默认Long.max, 否则取mapreduce.input.fileinputformat.split.maxsize 参数
            return context.getConfiguration().getLong(SPLIT_MAXSIZE,  Long.MAX_VALUE);
        }
        // 将Driver中 setInputPaths(inputs)中的字符串，按照分隔符(,;等?)切分后,拆分成多个Path[]返回;每个Path -> FileStatus;
        List<FileStatus> files = listStatus(job);{
            Path[] dirs = getInputPaths(job);{//FileInputFormat.getInputPaths()
                String dirs = context.getConfiguration().get(INPUT_DIR, "");// FileInputFormat.setInputPaths() => mapreduce.input.fileinputformat.inputdir (INPUT_DIR) ,即传入Input路劲;
                Path[] result = new Path[list.length];
                return result;
            }
            
            // 根据是否递归,递归获取子文件
            boolean recursive = getInputDirRecursive(job);//INPUT_DIR_RECURSIVE参数(mapreduce.input.fileinputformat.input.dir.recursive)设定
        }
        
        for (FileStatus file: files) {
            long length = file.getLen();
            if (length != 0) {
                BlockLocation[] blkLocations = file.getBlockLocations();//获取文件存储的Block位置
                
                boolean isSplitable = isSplitable(job, path);{//TextInputFormat.isSplitable()
                    final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file); //看有没有压缩?
                    if (null == codec) {// 没有压缩的话, 这里 codec ==null;
                      return true;
                    }
                    return codec instanceof SplittableCompressionCodec;
                }
                if (isSplitable) {
                    long blockSize = file.getBlockSize();{//FileStatus.getBlockSize()
                        return blockSize; //32M;
                        {//上面listStatus(job)方法中 -> singleThreadedListStatus() -> globStatus() -> fs.getFileStatus(path)
                            fs.getFileStatus(path){
                                return fs.getFileStatus(f);{//RawLocalFileSystem.getFileStatus(path)
                                    return getFileLinkStatusInternal(f, true);{//RawLocalFileSystem.getFileLinkStatusInternal(final Path f,boolean dereference)
                                        if (!useDeprecatedFileStatus) {
                                        }else if (dereference) {//进入这里;
                                            return deprecatedGetFileStatus(f);{
                                                if (path.exists()) {
                                                    int defaultBlockSize = getDefaultBlockSize(f);{//RawLocalFileSystem 调用父类 FileSystem.getDefaultBlockSize()
                                                        return getDefaultBlockSize();{
                                                            // 这里指定 默认的 defaultBlockSize =32M, 可以通过 fs.local.block.size 参数修改;
                                                            return getConf().getLong("fs.local.block.size", 32 * 1024 * 1024);
                                                        }
                                                    }
                                                    return new DeprecatedRawLocalFileStatus(pathToFile(f), defaultBlockSize, this);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    long splitSize = computeSplitSize(blockSize, minSize, maxSize);{//在minSize(1)和blockSize(32M)之间取最大值作为 每次分片的大小splitSize==32M;
                        return Math.max(minSize, Math.min(maxSize, blockSize));
                    }
                    long bytesRemaining = length;
                    //SPLIT_SLOP==1.1, 设置一些冗余，以防block被切断；
                    while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {//比较该文件的剩余大小 除以 分片大小(32M),
                        int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
                        splits.add(makeSplit(path, length-bytesRemaining, splitSize, blkLocations[blkIndex].getHosts(), blkLocations[blkIndex].getCachedHosts()));
                        bytesRemaining -= splitSize;
                    }

              
                }
                
            }
        }
        
    }





    
    
    
    
/** "Thread-"线程:执行Job.run(): 完成一次MapReducer计算任务的运行过程; 主要包括 mapRunnables,reduceRunnables任务的创建,和 MapTask,ReduceTask的提交运行;
*   - 引入线程(Local模式): "main"线程的Job.submitJobInternal -> LocalJobRunner.submitJob() -> new Job() 构造函数中 job.start()启动Job的线程
*   
*   - 下游线程: 
        * 1-多个"LocalJobRunner Map Task Executor # 0/n" 线程: 跑MapTask任务;
        * 1-多个 "pool-3-thread-1"的ReduceTask线程; 
*/
// "Thread-3" 线程
LocalJobRunner.Job.run(){
    JobID jobId = profile.getJobID();
    try{
        // 这个方法从submitJobDir下的job.splitmetainfo文件中读取分区位置信息:InputSplit在job.split中的位移量;
        TaskSplitMetaInfo[] taskSplitMetaInfos = SplitMetaInfoReader.readSplitMetaInfo(jobId, localFs, conf, systemJobDir);{
            FSDataInputStream in = fs.open(metaSplitFile);
            int numSplits = WritableUtils.readVInt(in); //第二行应该就是分片数量numSplits的值;读取;
            for (int i = 0; i < numSplits; i++) {
                splitMetaInfo.readFields(in);
                //从in流中读取该分片所在的 文件名和offset, splitIndex:TaskSplitIndex[splitLocaltion="**/job.split",startOffset=7];
                JobSplit.TaskSplitIndex splitIndex = new JobSplit.TaskSplitIndex(jobSplitFile, splitMetaInfo.getStartOffset());
                allSplitMetaInfo[i] = new JobSplit.TaskSplitMetaInfo(splitIndex, splitMetaInfo.getLocations(), splitMetaInfo.getInputDataLength());
            }
        }
        int numReduceTasks = job.getNumReduceTasks(); //分片数据决定 Reducer数量;
        outputCommitter.setupJob(jContext);
        
        //定义MapTask的线程;
        List<RunnableWithThrowable> mapRunnables = getMapTaskRunnables(taskSplitMetaInfos, jobId, mapOutputFiles);{//Job.getMapTaskRunnables()
            for (TaskSplitMetaInfo task : taskInfo) {//taskInfo 即 taskSplitMetaInfos
                list.add(new MapTaskRunnable(task, numTasks++, jobId,mapOutputFiles));//task -> splitIndex, 将分片索引封装进每个MapRunnable对象,用于"LocalMapTask"线程据此访问job.split和构建InputSplit对象;
            }
        }
        
        runTasks(mapRunnables, mapService, "map");{//Job.runTasks()
            for (Runnable r : runnables) {
                service.submit(r);{//ExecutorService.submit(Runnable r)
                    r.run()// 启动"LocalJobRunner Map Task Executor # 0/n" 线程,执行一个MapTask;
                }
            }
            service.shutdown();
            service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }
        
        if (numReduceTasks > 0) {
            //定义和运行ReduceTask任务;
            List<RunnableWithThrowable> reduceRunnables = getReduceTaskRunnables(jobId, mapOutputFiles);
            runTasks(reduceRunnables, reduceService, "reduce");{
                for (Runnable r : runnables) {
                    service.submit(r);{//ExecutorService.submit(Runnable r)
                        r.run()
                    }
                }
                service.shutdown();
                service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            }
        }
    }
}






// "LocalJobRunner Map Task Executor "线程：执行Mapper 线程: 
LocalJobRunner.JobRunner.MapTaskRunnable.run(){
    TaskAttemptID reduceId = new TaskAttemptID(new TaskID());
    MapTask map = new MapTask(systemJobFile.toString(), mapId, taskId, info.getSplitIndex(), 1);// 在这里将splitIndex分片信息赋给MapTask.splitMetaInfo成员变量;
    
    map.run(localConf, Job.this);{
        initialize(job, getJobID(), reporter, useNewApi);
        if (useNewApi) {
            runNewMapper(job, splitMetaInfo, umbilical, reporter);{//MapTask.runNewMapper(), 
                // 1. 第一步,先从splitIndex索引从job.split文件中读取InputSplit数据并反序列化成InputSplit对象;
                InputSplit split = getSplitDetails(new Path(splitIndex.getSplitLocation()), splitIndex.getStartOffset());{
                    FSDataInputStream inFile = fs.open(file);
                    inFile.seek(offset);
                    deserializer.open(inFile);//读取二进制字节流
                    T split = deserializer.deserialize(null);//把从job.split相应位置的二进制流,反序列成 InputSplit对象;
                }
                
                // 2. 构建 RecordReader,和 RecordWriter
                RecordReader<INKEY,INVALUE> input = new NewTrackingRecordReader<INKEY,INVALUE> (split, inputFormat, reporter, taskContext);
                RecordWriter output = new NewOutputCollector(taskContext, job, umbilical, reporter);
                
                Context mapperContext = new WrappedMapper<INKEY, INVALUE, OUTKEY, OUTVALUE>().getMapContext(mapContext);
                //在这里完成该分片对应 InputPath输入流的fileIn=fs.open();
                input.initialize(split, mapperContext);{//MapTask.NewTrackingRecordReader.initialize()
                    real.initialize(split, context);{//默认 LineRecordRecord.initialize()
                        start = split.getStart();
                        end = start + split.getLength();
                        final Path file = split.getPath();
                        fileIn = fs.open(file);
                        CompressionCodec codec = new CompressionCodecFactory(job).getCodec(file);
                        if (null!=codec) {
                            //todo
                        }else{
                            fileIn.seek(start); //定位到start位置;
                            in = new UncompressedSplitLineReader(fileIn, job, this.recordDelimiterBytes, split.getLength());
                        }
                    }
                }
                
                /** 核心代码: 执行Map计算任务,并把每个Map输出KV写入到缓冲区,并择时触发"SpillThread"线程溢写磁盘;
                *       都是在map(Key,Value)方法内:
                            - Map.map()中完成计算;
                            - Map.map() -> context.write(k,v)中, 完成溢写;
                */
                mapper.run(mapperContext);{
                    setup(context);
                    while (context.nextKeyValue(){//类似迭代器的hasHext(), WrappedMapper.Context.nextKeyValue()
                        return mapContext.nextKeyValue();{//MapContextImpl.nextKeyValue() -> NewTrackingRecordReader.nextKeyValue()
                            boolean result = real.nextKeyValue();{//LineRecordReader.nextKeyValue()
                                if (key == null)key = new LongWritable();
                                if (value == null) value = new Text();
                                while (getFilePosition() <= end || in.needAdditionalRecordAfterSplit()) {
                                    newSize = in.readLine(value, maxLineLength, maxBytesToConsume(pos));{//UncompressedSplitLineReader.readLine()
                                        return bytesRead = super.readLine(str, maxLineLength, maxBytesToConsume);{//LineReader.readLine()
                                            if (this.recordDelimiterBytes != null) {
                                              return readCustomLine(str, maxLineLength, maxBytesToConsume);
                                            } else { //进入这里,读取默认的一行;
                                                return readDefaultLine(str, maxLineLength, maxBytesToConsume);{
                                                    str.clear();//将里面原理的字符数据情况;
                                                    do{
                                                        if (appendLength > 0) {
                                                            int appendLength = readLength - newlineLength;
                                                            
                                                            str.append(buffer, startPosn, appendLength);
                                                            txtLength += appendLength;
                                                        }
                                                    }while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);
                                                }
                                            }
                                        }
                                    }
                                    if(newSize==0){
                                        return false;
                                    }else{
                                        return true;
                                    }
                                }
                            }
                            return result;
                        }
                    }) {
                        map(context.getCurrentKey(), context.getCurrentValue(), context);{//子类的map()方法
                            MyMapperImpl.map(KEYIN key, VALUEIN value, Context context){
                                // todo 业务处理
                                
                                context.write((KEYOUT) key, (VALUEOUT) value);{//WrappedMapper.write()
                                    mapContext.write(key, value);{//TaskInputOutputContextImpl.write()
                                        output.write(key, value);{//MapTask.NewOutputCollector.write()
                                            int = partitioner.getPartition(key, value, partitions);{//分区
                                                
                                            }
                                            
                                            collector.collect(key, value, partiton);{//MapOutputBuffer.collect()
                                               //代码如下;
                                            }
                                        }
                                    }
                                }
                            }
                            
                        }
                    }
                }
            
                input.close();
                output.close(mapperContext);{//NewOutputCollector.close()
                    collector.flush();{
                        while (spillInProgress) { //阻塞直到 溢写状态变为true;
                            reporter.progress();
                            spillDone.await();
                        }
                        
                        if (kvindex != kvend) {
                            kvend = (kvindex + NMETA) % kvmeta.capacity();
                            sortAndSpill();{//MapOutputBuffer.sortAndSpill()
                                    //代码详解下面的 sortAndSpill()方法;
                            }
                        }
                        
                    }
                }
                
            }
        }
    }
}

    // 将一个Map输出(Key,Value) 写入缓存,并溢写磁盘;
    output.write(key, value);{//MapTask.NewOutputCollector.write()
        int = partitioner.getPartition(key, value, partitions);{//分区
            
        }
        
        // 根据key,value,计算本条数据(Key,Value) 是落在哪个分区(号),即partition:int;
        //再将数据,根据key排序,写入到缓存区?
        collector.collect(key, value, partiton);{//MapOutputBuffer.collect()
            
            bufferRemaining -= METASIZE; //?
            
            if (bufferRemaining <= 0) {
                spillLock.lock();
                if (!spillInProgress) { //当没有spill时,才可以溢写;
                    
                    final int bUsed = distanceTo(kvbidx, bufindex); {// ?
                        return i <= j ? j - i : mod - i + j;
                    }
                    final boolean bufsoftlimit = bUsed >= softLimit; //?
                    
                    if ((kvbend + METASIZE) % kvbuffer.length != equator - (equator % METASIZE)) {
                    
                        resetSpill();
                        bufferRemaining = Math.min( distanceTo(bufindex, kvbidx) - 2 * METASIZE, softLimit - bUsed) - METASIZE;
                    }else if (bufsoftlimit && kvindex != kvend) {
                        startSpill();{//MapOutputBuffer.startSpill()
                            kvend = (kvindex + NMETA) % kvmeta.capacity();
                            spillInProgress = true; //标记正在进行溢写splill;
                            spillReady.signal(); //向"SpillThread"线程发成准备好spill的信号量;
                            {//"SpillThread"线程,MapTask.MapOutputBuffer.SplitThread.run()方法的执行
                                while(true){
                                    spillDone.signal();
                                    while (!spillInProgress) { //当溢写状态变量: spillInProgress==false时,一直阻塞在此等待上面 spillReady.signal()信号;
                                        spillReady.await();
                                    }
                                    //进入这里,说明spillInProgress==true,需要进行溢写了;
                                    spillLock.unlock(); //为什么释放锁? 保证上面的线程中哪个变量的安全? spillInProgress?
                                    /* 核心代码: 正式进行溢写,
                                    *
                                    */
                                    sortAndSpill();{//MapOutputBuffer.sortAndSpill()
                                        // 代码详解下面 的 MapOutputBuffer.sortAndSpill()
                                    }
                                }
                            }
                        }
                        
                        final int distkvi = distanceTo(bufindex, kvbidx);
                        
                    }
                }
            }
            
            
            
        }
    }


    // 2.3 排序并溢写输出的逻辑: MapOutputBuffer.sortAndSpill 

    sortAndSpill();{//MapOutputBuffer.sortAndSpill()
        
        final Path filename = mapOutputFile.getSpillFileForWrite(numSpills, size);// 
        // /tmp/**/jobcache/job_local831756809_0001/attempt_local831756809_0001_m_000000_0/output/spill0.out
        FSDataOutputStream out = rfs.create(filename);//创建该分区写出文件的输出流
        
        final int mend = 1 + // kvend is a valid record
              (kvstart >= kvend
              ? kvstart
              : kvmeta.capacity() + kvstart) / NMETA;
        //先进行排序; 快排算法;
        sorter.sort(MapOutputBuffer.this, mstart, mend, reporter);{//QuickSort.sort()
            sortInternal(s, p, r, rep, getMaxDepth(r - p));
        }
        
        for (int i = 0; i < partitions; ++i) {
            partitionOut = CryptoUtils.wrapIfNecessary(job, out, false);// 这里的 partitionOut就是 out,spill0文件的输出流;
            //创建Writer,保证该 OutputStream;
            IFile.Writer<K, V> writer = new Writer<K, V>(job, partitionOut, keyClass, valClass, codec,  spilledRecordsCounter);
            
            // 若需要Map端先预聚合,则排序前先conbiner下;
            if (combinerRunner == null) {//当没有设置预聚合时,代码进入这里;
                DataInputBuffer key = new DataInputBuffer();
                while (spindex < mend && kvmeta.get(offsetFor(spindex % maxRec) + PARTITION) == i) {
                    final int kvoff = offsetFor(spindex % maxRec);
                    int keystart = kvmeta.get(kvoff + KEYSTART);
                    int valstart = kvmeta.get(kvoff + VALSTART);
                    key.reset(kvbuffer, keystart, valstart - keystart);
                    getVBytesForOffset(kvoff, value); //获取数据写入到value中;
                    writer.append(key, value);{//IFile.Writer.append(): value==该key对应的Map结果? 将结果写到 key对应的文件中?
                        int keyLength = key.getLength() - key.getPosition();
                        int valueLength = value.getLength() - value.getPosition();
                        WritableUtils.writeVInt(out, keyLength);
                        WritableUtils.writeVInt(out, valueLength);
                        out.write(key.getData(), key.getPosition(), keyLength); 
                        //最近将bytes写出到 output/spill0.out 文件中;
                        out.write(value.getData(), value.getPosition(), valueLength); {//DataOutputStream.write()
                            out.write(b, off, len);{
                                sum.update(b, off,len);
                                out.write(b,off,len);{
                                    out.write(b, off, len);{//FSDataOutputStream.PositionCache.write()
                                        out.write(b, off, len);
                                        position += len;  
                                    }
                                }
                            }
                            incCount(len);
                        }
                          
                        ++numRecordsWritten;
                    } 
                    ++spindex;
                }
                
            }else{
                int spstart = spindex;
                while (spindex < mend && kvmeta.get(offsetFor(spindex % maxRec)+ PARTITION) == i) {
                    ++spindex;
                }
                if (spstart != spindex) {
                    combineCollector.setWriter(writer);
                    RawKeyValueIterator kvIter = new MRResultIterator(spstart, spindex);
                    // 这里运行Map端预集合;
                    combinerRunner.combine(kvIter, combineCollector);{//Task.NewCombinerRunner.combine()
                        reducer =(Reducer<K,V,K,V>) ReflectionUtils.newInstance(reducerClass, job);
                        Context 
                       reducerContext = createReduceContext(reducer, job, taskId,iterator, null, inputCounter,
                                   new OutputConverter(collector), committer, reporter,
                                   comparator, keyClass, valueClass);
                        //运行Map的预聚合 Reducer.run()
                        reducer.run(reducerContext);{//Reducer.run()
                            //同下Reducer线程中 Reducer.run()代码;
                        }
                    }
                }
            }
        }
        
    }



// "pool-3-thread-1"线程：执行Reducer线程
LocalJobRunner.JobRunner.ReduceTaskRunnable.run(){
    TaskAttemptID reduceId = new TaskAttemptID(new TaskID());
    ReduceTask reduce = new ReduceTask(systemJobFile.toString(),reduceId, taskId, mapIds.size(), 1);
    reduce.setLocalMapFiles(mapOutputFiles);//设置mapOutput文件路径用于获取Map的数据;
    
    
    //运行Reducer的逻辑;
    reduce.run(localConf, Job.this);{//ReduceTask.run()
        initialize(job, getJobID(), reporter, useNewApi);
        CombineOutputCollector combineCollector = (null != combinerClass) ?  new CombineOutputCollector(reduceCombineOutputCounter, reporter, conf) : null;
        Class<? extends ShuffleConsumerPlugin> clazz = job.getClass(MRConfig.SHUFFLE_CONSUMER_PLUGIN, Shuffle.class, ShuffleConsumerPlugin.class);
		
        if (useNewApi) {
            runNewReducer(job, umbilical, reporter, rIter, comparator, keyClass, valueClass);{//ReducerTask.runNewReducer()
                final RawKeyValueIterator rawIter = new RawKeyValueIterator(){};
                Context reducerContext = createReduceContext(reducer, job, getTaskID(), rIter, reduceInputKeyCounter, reduceInputValueCounter, 
                                               trackedRW,committer,reporter, comparator, keyClass,valueClass);
                
                reducer.run(reducerContext);{//reducer的实例是我们继承实现的 ReducerImpl, 这里调用其父类 Reducer.run()
                    setup(context);
                    while (context.nextKey()) {
                        reduce(context.getCurrentKey(), context.getValues(), context);{//调用子类的reduce()实现方法
                            MyReducerImpl.reduce(){}
                            
                        }
                        
                        Iterator<VALUEIN> iter = context.getValues().iterator();
                        if(iter instanceof ReduceContext.ValueIterator) {
                            ((ReduceContext.ValueIterator<VALUEIN>)iter).resetBackupStore();        
                        }
                    }
                }
            }
        } 
    }
}













