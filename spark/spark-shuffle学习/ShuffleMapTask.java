

//线程"Executor task launch work" 中: ResultTask.runTask()

// 2. 进行ShuffleMapTask的计算, 即Shuffle前的RDD数据的计算;
ShuffleMapTask.runTask(){
    val manager = SparkEnv.get.shuffleManager
    writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
    writer.write(rdd.iterator(partition, context){//writer.write()
        SortShuffleWriter.write(){
            sorter = if (dep.mapSideCombine) {//在Map阶段那边进行过合并了;
                require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
                new ExternalSorter[K, V, C](context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
            } else {
                new ExternalSorter[K, V, V](context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
            }
            sorter.insertAll(records);{//将数据加到内存buff中?
                if (shouldCombine){}else{
                  while (records.hasNext) {
                    addElementsRead()
                    val kv = records.next()
                    buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
                    maybeSpillCollection(usingMap = false)
                  }
                }
            }
            
            val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId);//计算合并后的数据写出到磁盘文件; C:\Users\86177\AppData\Local\Temp\blockmgr-3ac9f405-84bb-495d-adf1-015b1ee9d0c1\0a\shuffle_2_0_0.data
            val tmp = Utils.tempFileWith(output); // 以上面output文件名后加一个.UUID.randomUUID()后缀,作为临时文件;
            try {
                val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
                
                val partitionLengths = sorter.writePartitionedFile(blockId, tmp);{//ExternalSorter.writePartitionedFile()
                    // Track location of each range in the output file
                    val lengths = new Array[Long](numPartitions)
                    val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize, context.taskMetrics().shuffleWriteMetrics)
                    if (spills.isEmpty) {//shuffle数据太多,切分后?
                      val collection = if (aggregator.isDefined) map else buffer
                      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
                      while (it.hasNext) {
                        val partitionId = it.nextPartition()
                        while (it.hasNext && it.nextPartition() == partitionId) {
                            it.writeNext(writer);{//WritablePartitionedIterator.writeNext(writer: DiskBlockObjectWriter)
                                writer.write(cur._1._2, cur._2);{//DiskBlockObjectWriter.write(key: Any, value: Any)
                                    if (!streamOpen) {//先打开Stream; 哪个Stream? objOut:JavaSerializationStream
                                        open();{//DiskBlockObjectWriter.open()
                                            if (!initialized) {
                                              initialize();{
                                                fos = new FileOutputStream(file, true)
                                                channel = fos.getChannel()
                                                ts = new TimeTrackingOutputStream(writeMetrics, fos)
                                                class ManualCloseBufferedOutputStream extends BufferedOutputStream(ts, bufferSize) with ManualCloseOutputStream
                                                mcs = new ManualCloseBufferedOutputStream
                                              }
                                              initialized = true
                                            }
                                            bs = serializerManager.wrapStream(blockId, mcs)
                                            objOut = serializerInstance.serializeStream(bs)
                                            streamOpen = true
                                            this
                                        }
                                    }
                                    // JavaSerializationStream.writeKey()把数据写到哪里? 缓存?
                                    objOut.writeKey(key);{//JavaSerializationStream.writeKey()
                                        objOut.writeObject(t);//ObjectOutputStream.writeObject()
                                    }
                                    objOut.writeValue(value)
                                    recordWritten();{//记录下+1, 没什么用吧;
                                        numRecordsWritten += 1
                                        writeMetrics.incRecordsWritten(1)
                                        if (numRecordsWritten % 16384 == 0) {
                                          updateBytesWritten()
                                        }
                                    }
                                }
                                
                                cur = if (it.hasNext) it.next() else null
                            }
                        }
                        //正在刷新写出? 
                        val segment = writer.commitAndGet();{//DiskBlockObjectWriter.commitAndGet()
                            if (streamOpen) {//上面已经打开了;
                                objOut.flush()
                                bs.flush()//LZ4BlockOutputStream.flush()刷出到磁盘?
                                objOut.close()
                                streamOpen = false
                                if (syncWrites) {//没开,不进;
                                    val start = System.nanoTime()
                                    fos.getFD.sync()
                                    writeMetrics.incWriteTime(System.nanoTime() - start)
                                }
                                val pos = channel.position()
                                // 将刚写出Block的信息,保存到FileSegment对象总(file:文件位置, offset, 数据长度:lenght);
                                val fileSegment = new FileSegment(file, committedPosition, pos - committedPosition)
                                committedPosition = pos
                                // In certain compression codecs, more bytes are written after streams are closed
                                writeMetrics.incBytesWritten(committedPosition - reportedPosition)
                                reportedPosition = committedPosition
                                fileSegment
                            } else {
                                new FileSegment(file, committedPosition, 0)
                            }
                        }
                        
                        // lengths:Array[Long](numPartitions), 存储该ShuffledRDD存于目录下各分区对应文件中的数据字节大小;
                        lengths(partitionId) = segment.length;// 将刚写入的Segment小文件的长度, 记录在该partitionId分区数为下标的数组中;
                      }
                      
                    } else { // 当该Array[SpilledFile]不止1个时, 利用原有的SpilledFile文件;
                      for ((id, elements) <- this.partitionedIterator) {
                        if (elements.hasNext) {
                          for (elem <- elements) {
                            writer.write(elem._1, elem._2)
                          }
                          val segment = writer.commitAndGet()
                          lengths(id) = segment.length
                        }
                      }
                    }
                    
                    writer.close();{
                        if (initialized) {//进入;
                          Utils.tryWithSafeFinally {
                            commitAndGet();
                          } {
                            closeResources()
                          }
                        }
                    }
                    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
                    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
                    context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)
                    
                    lengths //返回该RDD shuffle完后, 给各分区的数据字节大小;
                }
                
                // 这里就是把index和.data数据进行一些捣腾而已;
                shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp);{
                    val indexFile = getIndexFile(shuffleId, mapId)
                    val indexTmp = Utils.tempFileWith(indexFile)
                    try {
                      val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
                      Utils.tryWithSafeFinally {
                        var offset = 0L
                        out.writeLong(offset)
                        for (length <- lengths) {
                          offset += length
                          out.writeLong(offset)
                        }
                      } {
                        out.close()
                      }
                      
                      val dataFile = getDataFile(shuffleId, mapId)
                      synchronized {
                        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
                        if (existingLengths != null) {
                          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
                          if (dataTmp != null && dataTmp.exists()) {
                            dataTmp.delete()
                          }
                          indexTmp.delete()
                        } else {//正常进入这里?
                          if (indexFile.exists()) {
                            indexFile.delete()
                          }
                          if (dataFile.exists()) {
                            dataFile.delete()
                          }
                          if (!indexTmp.renameTo(indexFile)) {//记那个 index文件 重命名;
                            throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
                          }
                          if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {// 将数据文件重命名;
                            throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
                          }
                        }
                      }
                    } finally {
                      if (indexTmp.exists() && !indexTmp.delete()) {// 把indexTmp临时文件删除;
                        logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
                      }
                    }
                }
                mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
            } finally {
                if (tmp.exists() && !tmp.delete()) {
                    logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
                }
            }
        }
        
        UnsafeShuffleWriter.write(){}
        HashShuffleWriter.write()// 以前应该还有HashShuffleWriter
        
    }
        .asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
    writer.stop(success = true).get
}
	


	// 1. ShuffledRDD的计算;

	ShuffledRDD.compute(split, context);{ 
		val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
		val aggrIter: Iterator[Product2[K, C]] = SparkEnv.get.shuffleManager
			.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
			.read();{//BlockStoreShuffleReader.read()
				val wrappedStreams = new ShuffleBlockFetcherIterator();{
					initialize();{//ShuffleBlockFetcherIterator.initialize()
						val numFetches = remoteRequests.size - fetchRequests.size
						fetchLocalBlocks();{//ShuffleBlockFetcherIterator.fetchLocalBlocks()
							val iter = localBlocks.iterator
							while (iter.hasNext) {
							  val blockId = iter.next()
							  try {
								// 这里buf是一个FileSegmentManagedBuffer(file,offset,length)
								val buf = blockManager.getBlockData(blockId);{//BlockManager.getBlockData()
									if (blockId.isShuffle) {//对于 ShuffleBlockId类型的blockId, 会调用ShuffleBlockResolver():IndexShuffleBlockResolver 这个类取加载block数据;
										// 默认用IndexShuffleBlockResolver, 因为只有这一个实现类,难道可以自己实现?
										shuffleManager.shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId]);{//IndexShuffleBlockResolver.getBlockData()
											val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId) //先读索引文件,是二进制的: ..\Temp\blockmgr-3ac9f405-84bb-495d-adf1-015b1ee9d0c1\0f\shuffle_0_1_0.index
											val in = new DataInputStream(new FileInputStream(indexFile))
											try {
												ByteStreams.skipFully(in, blockId.reduceId * 8)
												val offset = in.readLong()//读取其offset数据: 0 
												val nextOffset = in.readLong() // 再度下一个offset: 450;
												new FileSegmentManagedBuffer(transportConf,getDataFile(blockId.shuffleId, blockId.mapId),offset,nextOffset - offset)
											} finally {
												in.close()
											}
										}
									}else{}
								}
								shuffleMetrics.incLocalBlocksFetched(1)
								shuffleMetrics.incLocalBytesRead(buf.size)
								buf.retain()
								results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf, false));//这就算获取成功了? 
							  } catch {
								case e: Exception =>// 有任何一个异常,马上结束方法并返回;
									logError(s"Error occurred while fetching local blocks", e)
									results.put(new FailureFetchResult(blockId, blockManager.blockManagerId, e))
									return
							  }
							}
						}
					}
				}
				val recordIter = wrappedStreams.flatMap ()
				val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined){}else{};
			}
		aggrIter.asInstanceOf[Iterator[(K, C)]]
	}


	
	at com.esotericsoftware.kryo.serializers.ObjectField.write(ObjectField.java:80)
	at com.esotericsoftware.kryo.serializers.FieldSerializer.write(FieldSerializer.java:518)
	at com.esotericsoftware.kryo.Kryo.writeObject(Kryo.java:552)
	at com.esotericsoftware.kryo.serializers.ObjectField.write(ObjectField.java:80)
	at com.esotericsoftware.kryo.serializers.FieldSerializer.write(FieldSerializer.java:518)
	at com.esotericsoftware.kryo.Kryo.writeClassAndObject(Kryo.java:628)
	at org.apache.spark.serializer.KryoSerializationStream.writeObject(KryoSerializer.scala:195)
	at org.apache.spark.serializer.SerializationStream.writeValue(Serializer.scala:135)
	at org.apache.spark.storage.DiskBlockObjectWriter.write(DiskBlockObjectWriter.scala:185)
	at org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter.write(BypassMergeSortShuffleWriter.java:150)
	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:79)
	at org.apache.spark.scheduler.ShuffleMapTask.runTask(ShuffleMapTask.scala:47)
	at org.apache.spark.scheduler.Task.run(Task.scala:86)