



// "main"线程: 创建shuffle-client线程的源码;

new SparkContext(){
	_env.blockManager.initialize(_applicationId);{//BlockManager.initialize()
		blockTransferService.init(this);{//NettyBlockTransferService.init(blockDataManager)
			val rpcHandler = new NettyBlockRpcServer(conf.getAppId, serializer, blockDataManager)
			var serverBootstrap: Option[TransportServerBootstrap] = None
			var clientBootstrap: Option[TransportClientBootstrap] = None
			if (authEnabled) {
				serverBootstrap = Some(new AuthServerBootstrap(transportConf, securityManager))
				clientBootstrap = Some(new AuthClientBootstrap(transportConf, conf.getAppId, securityManager))
			}
			
			// 创建shuffle-client线程;
			clientFactory = transportContext.createClientFactory(clientBootstrap.toSeq.asJava);{//TransportContext.createClientFactory()
				return new TransportClientFactory(this, bootstraps);{
					IOMode ioMode = IOMode.valueOf(conf.ioMode());
					this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
					// 这里就是定义"shuffle-client"线程的地方;
					this.workerGroup = NettyUtils.createEventLoop(ioMode,conf.clientThreads(),conf.getModuleName() + "-client");{
						ThreadFactory threadFactory = createThreadFactory(threadPrefix);
						switch (mode) {
						  case NIO: return new NioEventLoopGroup(numThreads, threadFactory);
						  case EPOLL: return new EpollEventLoopGroup(numThreads, threadFactory);
						  default:throw new IllegalArgumentException("Unknown io mode: " + mode);
						}
					}
				}
			}
			
			// 创建shuffle-server线程的源码;
			server = createServer(serverBootstrap.toList);{
				def startService(port: Int): (TransportServer, Int) = {
					val server = transportContext.createServer(bindAddress, port, bootstraps.asJava)
					(server, server.getPort)
				}
				Utils.startServiceOnPort(_port, startService, conf, getClass.getName)._1;{//Utils.startServiceOnPort()
					val maxRetries = portMaxRetries(conf)
					for (offset <- 0 to maxRetries) {
					  val tryPort = if (startPort == 0) {
						startPort
					  } else {
						userPort(startPort, offset)
					  }
					  try {
						// 调用在上面函数中定义的入参方法: 
						val (service, port) = startService(tryPort);{//即上面入参方法: createServer().startService(port: Int)
							val server = transportContext.createServer(bindAddress, port, bootstraps.asJava);{//TransportContext.createServer()
								return new TransportServer(this, host, port, rpcHandler, bootstraps);{
									try {
										init(hostToBind, portToBind);{//TransportContext.init()
											IOMode ioMode = IOMode.valueOf(conf.ioMode());
											// 在这里, 定义了"shuffle-server"/ rpc-client, shuffle-server等线程; 等并未启动; 
											EventLoopGroup bossGroup = NettyUtils.createEventLoop(ioMode, conf.serverThreads(), conf.getModuleName() + "-server");{
												ThreadFactory threadFactory = createThreadFactory(threadPrefix);
												switch (mode) {
												  case NIO: return new NioEventLoopGroup(numThreads, threadFactory);
												  case EPOLL: return new EpollEventLoopGroup(numThreads, threadFactory);
												  default:throw new IllegalArgumentException("Unknown io mode: " + mode);
												}
											}
											
											// 这里才正式启动相关线程;
											channelFuture = bootstrap.bind(address);{//AbstractBootstrap.bind()
												validate();
												return doBind(localAddress//AbstractBootstrap.doBind()
													final ChannelFuture regFuture = initAndRegister();{
														Channel channel = channelFactory().newChannel();
														init(channel);
														ChannelFuture regFuture = group().register(channel);{//MultithreadEventLoopGroup.register()
															return next().register(channel);{//SingleThreadEventLoop.register()
																return register(channel, new DefaultChannelPromise(channel, this));{//SingleThreadEventLoop.register()
																	channel.unsafe().register(this, promise);{
																		AbstractChannel.AbstractUnsafe.register(){
																			AbstractChannel.this.eventLoop = eventLoop;
																			eventLoop.execute(new Runnable() {
																				@Override public void run() {
																					register0(promise);
																				}
																			});{//SingleThreadEventExecutor.execute(Runnable task)
																				boolean inEventLoop = inEventLoop();{
																					return inEventLoop(Thread.currentThread());
																				}
																				if (inEventLoop) {//若已经启动循环,则添加该任务;
																					addTask(task);
																				} else {// 若初次运行? 则启动线程,并添加任务到队里;
																					// 这里正式启动该线程;
																					startThread();{//SingleThreadEventExecutor.startThread()
																						if (STATE_UPDATER.get(this) == ST_NOT_STARTED) {
																							if (STATE_UPDATER.compareAndSet(this, ST_NOT_STARTED, ST_STARTED)) {
																								thread.start();{//正式启动线程运行;
																									// 这个thread的创建和运行代码,就如后面 NettyUtils.createEventLoop()中代码所示;
																									run(){
																										SingleThreadEventExecutor.this.run();{//.this即为NioEventLoop的实例对象;
																											//this的实例对象为 NioEventLoop
																											NioEventLoop.run(){
																												
																											}
																										}
																									}
																								}
																								
																							}
																						}
																					}
																					addTask(task);
																					if (isShutdown() && removeTask(task)) {
																						reject();
																					}
																				}
																			}
																		}
																	}
																	return promise;
																}
															}
														}
														
													}
													final Channel channel = regFuture.channel();
												}
											}
											
										}
									} catch (RuntimeException e) {
										JavaUtils.closeQuietly(this);
										throw e;
									}
								}
							}
							(server, server.getPort)
						}
						logInfo(s"Successfully started service$serviceString on port $port.")
						return (service, port)
					  } catch {
						case e: Exception if isBindCollision(e) =>
						  if (offset >= maxRetries) {
							val exception = new BindException(exceptionMessage)exception.setStackTrace(e.getStackTrace)
							throw exception
						  }
					  }
					}
				}
			}
		}
		
		shuffleClient.init(appId);
	}
}



//"dag-scheduler-event-loop"线程中: 依据shuffle算子切分ResultStage, ShuffleMapStage,并将ShuffleMapStage注册到MapOutputTracker.mapStatuses内存Map中;

DAGScheduler.handleJobSubmitted(){
	// 这里注册?
	finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite);{//DAGScheduler.createResultStage()
		val parents = getOrCreateParentStages(rdd, jobId);{
			getShuffleDependencies(rdd).map { shuffleDep =>
				getOrCreateShuffleMapStage(shuffleDep, firstJobId);{//DAGScheduler.getOrCreateShuffleMapStage()
					shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
					  case Some(stage) => stage
					  case None =>
					    // Create stages for all missing ancestor shuffle dependencies.
						getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
						  if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
							createShuffleMapStage(dep, firstJobId)// 代码如下;
						  }
						}
						// Finally, create a stage for the given shuffle dependency.
						//重要方法, 创建ShuffleMapStage,并注册;
						createShuffleMapStage(shuffleDep, firstJobId);{//DAGScheduler.createShuffleMapStage()
							val numTasks = rdd.partitions.length
							val parents = getOrCreateParentStages(rdd, jobId)
							val id = nextStageId.getAndIncrement()
							// 在这里,创建ShuffleMapStage;
							val stage = new ShuffleMapStage(id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep)
							if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
							  val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
							  val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
							  (0 until locs.length).foreach { i =>
								if (locs(i) ne null) {
								  // locs(i) will be null if missing
								  stage.addOutputLoc(i, locs(i))
								}
							  }
							} else {//新建的ShuffleStage就进这里了;
								logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
								mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length);{//MapOutputTracker.registerShuffle(): 所谓注册,即存于内存Map<shuffleId,Array[MapStatus]>中;
									// mapStatuses:HashMap[Int, Array[MapStatus]], 记录该shuffleId的状态记录, 若之前该map种存在过该shuffleId, 则报异常;
									if (mapStatuses.put(shuffleId, new Array[MapStatus](numMaps)).isDefined) {
									  throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
									}
									// add in advance
									shuffleIdLocks.putIfAbsent(shuffleId, new Object())
								}
							}
							stage //将ShuffleMapStage返回;
						}
					}
				}
			}.toList
		}
		val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
		
	}
	
	 val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
	 submitStage(finalStage)
}




//线程"Executor task launch work" 中: ResultTask.runTask()




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
	
	// 3. 进行ShuffleMapTask后的数据的计算;
	ResultTask.runTask(){
		val it = rdd.iterator(partition, context);{
			if (storageLevel != StorageLevel.NONE) {
				getOrCompute(split, context)
			} else {//未cache()过;
				computeOrReadCheckpoint(split, context);{//RDD.computeOrReadCheckpoint
					if (isCheckpointedAndMaterialized) {
						firstParent[T].iterator(split, context)
					} else {//未checkpoint()过;
						compute(split, context);{//MapPartitionsRDD.compute(): 从父RDD中算出;
							val firstParentData = firstParent[T].iterator(split, context);{
								if (storageLevel != StorageLevel.NONE) {//有缓存的
								  getOrCompute(split, context);{//RDD.getOrCompute();
									val blockId = RDDBlockId(id, partition.index)
									var readCachedBlock = true
									val someBlockResult = SparkEnv.get.blockManager.getOrElseUpdate(blockId, storageLevel, elementClassTag, () => {
									  readCachedBlock = false
									  computeOrReadCheckpoint(partition, context)
									});{//BlockManager.getOrElseUpdate()
										//先从BlockManager中查询获取该BlockId, 有直接返回结束; 没有再进入下面 (doPutIterator())计算
										get[T](blockId)(classTag) match {
											case Some(block) => return Left(block) //从BlockManager中获取到了,就返回;
											case _ => // 没有该BlockId,进入下面计算(获取)
										}
										
										// Initially we hold no locks on this block.
										val someResult = doPutIterator(blockId, makeIterator, level, classTag, keepReadLock = true);{//BlockManager.doPutIterator() -> doPut()
											val putBlockInfo = {new BlockInfo(level, classTag, tellMaster)}
											val result: Option[T] = try {
											  val res = putBody(putBlockInfo);{//BlockManager.doPutIterator() -> doPut(){body) 中的body代码;
												if (level.useMemory) {
													
												} else if (level.useDisk) {//为什么是存Disk了? 因为是Shuffle数据?
													diskStore.put(blockId) { channel =>
														val out = Channels.newOutputStream(channel)
														serializerManager.dataSerializeStream(blockId, out, iterator())(classTag)
													};{// DiskStore.put(blockId: BlockId)(writeFunc: WritableByteChannel => Unit): Unit
														val out = new CountingWritableChannel(openForWrite(file))
														writeFunc(out);{//writeFunc函数即上面传入的body函数体;
															val out = Channels.newOutputStream(channel)
															val iter = iterator();{ iterator()及 作为参数层层传入的
																readCachedBlock = false
																computeOrReadCheckpoint(partition, context);{//RDD.computeOrReadCheckpoint()
																	if (isCheckpointedAndMaterialized) {
																	  firstParent[T].iterator(split, context)
																	} else {
																		compute(split, context);{//MapPartitionsRDD.compute()
																			// 调用父RDD.iterator()数据, 而其父RDD是一个ShuffleRDD.
																			val parentIter = firstParent[T].iterator(split, context);{//ShuffledRDD.iterator => RDD.iterator()
																				if (storageLevel != StorageLevel.NONE) {
																					getOrCompute(split, context)
																				} else {
																					computeOrReadCheckpoint(split, context);{//ShuffledRDD.computeOrReadCheckpoint() => RDD.computeOrReadCheckpoint()
																						if(){}else{compute(split, context);{//ShuffledRDD.compute()
																							val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
																							val aggrIter: Iterator[Product2[K, C]] = SparkEnv.get.shuffleManager
																								.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
																								.read();{//BlockStoreShuffleReader.read()
																									val wrappedStreams = new ShuffleBlockFetcherIterator();{
																										initialize();{//ShuffleBlockFetcherIterator.initialize()
																											val numFetches = remoteRequests.size - fetchRequests.size
																											fetchLocalBlocks();
																										}
																									}
																									val recordIter = wrappedStreams.flatMap ()
																									val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined){}else{};
																								}
																							aggrIter.asInstanceOf[Iterator[(K, C)]]
																						}}
																					}
																				}
																			}
																			f(context, split.index, firstParent[T].iterator(split, context))
																		}
																	}
																}
															}
															
															serializerManager.dataSerializeStream(blockId, out, iterator())(classTag)
														}
													}
													size = diskStore.getSize(blockId)
												}
												val putBlockStatus = getCurrentBlockStatus(blockId, info)
												val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
												iteratorFromFailedMemoryStorePut
											  }
											  
											  exceptionWasThrown = false
											  if (res.isEmpty) {
												if (keepReadLock) {
												  blockInfoManager.downgradeLock(blockId)
												} else {
												  blockInfoManager.unlock(blockId)
												}
											  } else {
												removeBlockInternal(blockId, tellMaster = false)
												logWarning(s"Putting block $blockId failed")
											  }
											  res
											} finally {
											  if (exceptionWasThrown) {
												removeBlockInternal(blockId, tellMaster = tellMaster)
												addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
											  }
											}
										}
										
										someResult match {
										  case None =>
											val blockResult = getLocalValues(blockId).getOrElse {
											  releaseLock(blockId)
											  throw new SparkException(s"get() failed for block $blockId even though we held a lock")
											}
											releaseLock(blockId)
											Left(blockResult)
										  case Some(iter) => Right(iter)
										}
									}
									
									someBlockResult match {
									  case Left(blockResult) =>
										if (readCachedBlock) {
										  val existingMetrics = context.taskMetrics().inputMetrics
										  existingMetrics.incBytesRead(blockResult.bytes)
										  new InterruptibleIterator[T](context, blockResult.data.asInstanceOf[Iterator[T]]) {
											override def next(): T = {
											  existingMetrics.incRecordsRead(1)
											  delegate.next()
											}
										  }
										} else {
										  new InterruptibleIterator(context, blockResult.data.asInstanceOf[Iterator[T]])
										}
									  case Right(iter) => new InterruptibleIterator(context, iter.asInstanceOf[Iterator[T]])
									}
								  }
								} else {
								  computeOrReadCheckpoint(split, context)
								}
							}
							
							f(context, split.index, firstParentData);
						}
					}
				}
			}
		}
		
		func(context, it)
	}
	
	


OneForOneBlockFetcher.start(){
    client.sendRpc(openMessage.toByteBuffer(), new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        try {
          streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
          logger.trace("Successfully opened blocks {}, preparing to fetch chunks.", streamHandle);
          // Immediately request all chunks -- we expect that the total size of the request is
          // reasonable due to higher level chunking in [[ShuffleBlockFetcherIterator]].
          for (int i = 0; i < streamHandle.numChunks; i++) {
            if (shuffleFiles != null) {
              client.stream(OneForOneStreamManager.genStreamChunkId(streamHandle.streamId, i),
                new DownloadCallback(shuffleFiles[i], i));
            } else {
              client.fetchChunk(streamHandle.streamId, i, chunkCallback);
            }
          }
        } catch (Exception e) {
          logger.error("Failed while starting block fetches after success", e);
          failRemainingBlocks(blockIds, e);
        }
      }

      @Override
      public void onFailure(Throwable e) {
        logger.error("Failed while starting block fetches", e);
        failRemainingBlocks(blockIds, e);
      }
    });
}





NettyBlockTransferService.fetchBlocks(){
    logTrace(s"Fetch blocks from $host:$port (executor id $execId)")
    try {
      val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
        override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
          val client = clientFactory.createClient(host, port)
          new OneForOneBlockFetcher(client, appId, execId, blockIds.toArray, listener,
            transportConf, shuffleFiles).start()
        }
      }

      val maxRetries = transportConf.maxIORetries();//conf.getInt(spark.shuffle  io.maxRetries, 3);
      if (maxRetries > 0) {//默认进入这里, 返回RetryingBlockFetcher
        new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener).start()
      } else {
        blockFetchStarter.createAndStart(blockIds, listener)
      }
    } catch {
      case e: Exception =>
        logError("Exception while beginning fetchBlocks", e)
        blockIds.foreach(listener.onBlockFetchFailure(_, e))
    }
}








NettyUtils.createEventLoop(IOMode mode, int numThreads, String threadPrefix){
	ThreadFactory threadFactory = createThreadFactory(threadPrefix);
    switch (mode) {
		case NIO: return new NioEventLoopGroup(numThreads, threadFactory);{
			this(nThreads, threadFactory, SelectorProvider.provider());{
			this(nThreads, threadFactory, selectorProvider, DefaultSelectStrategyFactory.INSTANCE);{
			super(nThreads, threadFactory, selectorProvider, selectStrategyFactory, RejectedExecutionHandlers.reject());{
				super(nThreads == 0? DEFAULT_EVENT_LOOP_THREADS : nThreads, threadFactory, args);{//new MultithreadEventLoopGroup()
					children = new SingleThreadEventExecutor[nThreads];
					if (isPowerOfTwo(children.length)) {
						chooser = new PowerOfTwoEventExecutorChooser();
					} else {
						chooser = new GenericEventExecutorChooser();
					}
					for (int i = 0; i < nThreads; i ++) {
						boolean success = false;
						try {
							children[i] = newChild(threadFactory, args);{//NioEventLoopGroup.newChild()
								return new NioEventLoop(this, threadFactory, (SelectorProvider) args[0],
										((SelectStrategyFactory) args[1]).newSelectStrategy(), (RejectedExecutionHandler) args[2]);{
									super(parent, threadFactory, false, DEFAULT_MAX_PENDING_TASKS, rejectedExecutionHandler);{//new SingleThreadEventLoop()
										this.parent = parent;
										this.addTaskWakesUp = addTaskWakesUp;
										//该线程工厂启动的线程
										thread = threadFactory.newThread(()->{// 只创建一个线程,并没有启动;
											SingleThreadEventExecutor.this.run();
										});{//threadFactory接口的实现类: DefaultThreadFactory.newThread(Runnable r)
											Thread t = newThread(new DefaultRunnableDecorator(r), prefix + nextId.incrementAndGet());
											return t;
										}
										taskQueue = newTaskQueue();
									}
									
									provider = selectorProvider;
									selector = openSelector();
								}
							}
							success = true;
						}
					}
					
					for (EventExecutor e: children) {
						e.terminationFuture().addListener(new FutureListener<Object>{});
					}
				}
			}}}
		}
	  
      case EPOLL: return new EpollEventLoopGroup(numThreads, threadFactory);
      default: throw new IllegalArgumentException("Unknown io mode: " + mode);
    }
}












NioEventLoop.run(){
        for (;;) {
            try {
                switch (selectStrategy.calculateStrategy(selectNowSupplier, hasTasks())) {
                    case SelectStrategy.CONTINUE:
                        continue;
                    case SelectStrategy.SELECT:
						// 关机步骤1: 轮询注册到selector上面的事件
                        select(wakenUp.getAndSet(false));{// 检查是否有I/O消息;轮询注册到selector上面的事件
							Selector selector = this.selector;
							try {
								int selectCnt = 0;
								long currentTimeNanos = System.nanoTime();
								long selectDeadLineNanos = currentTimeNanos + delayNanos(currentTimeNanos);
								for (;;) {
									long timeoutMillis = (selectDeadLineNanos - currentTimeNanos + 500000L) / 1000000L;
									if (timeoutMillis <= 0) {// 如果超时,跳出for循环,结束select
										if (selectCnt == 0) {
											selector.selectNow();
											selectCnt = 1;
										}
										break;
									}
									// 进入这里,说明没超时; 会在任务队列不为空 且wakenUp成功从false置为true后; 则执行一次: 阻塞的selectNow(); 执行完结束循环;
									if (hasTasks() && wakenUp.compareAndSet(false, true)) {
										selector.selectNow();
										selectCnt = 1;
										break;
									}
									// 若执行到这里,说明taskQueue任务队列不为空,
									int selectedKeys = selector.select(timeoutMillis);
									selectCnt ++;
									if (selectedKeys != 0 || oldWakenUp || wakenUp.get() || hasTasks() || hasScheduledTasks()) {
										break;
									}
									if (Thread.interrupted()) {
										selectCnt = 1;
										break;
									}
									
									currentTimeNanos = time;
								}

							} catch (CancelledKeyException e) {  
							}
						}
                        if (wakenUp.get()) {
                            selector.wakeup();
                        }
                    default:
                        // fallthrough
                }

                cancelledKeys = 0;
                needsToSelectAgain = false;
                final int ioRatio = this.ioRatio;
                if (ioRatio == 100) {
                    try {
                        processSelectedKeys();
                    } finally {
                        runAllTasks();
                    }
                } else {//默认50 进入这里;
                    final long ioStartTime = System.nanoTime();
                    try {
						// 2-关键: 处理轮询到的事件
                        processSelectedKeys();// 也许先处理I/O事件;
                    } finally {
                        // Ensure we always run tasks.
                        final long ioTime = System.nanoTime() - ioStartTime;
						// 3- 关键: 处理外部线程扔到taskQueue里的任务
                        runAllTasks(ioTime * (100 - ioRatio) / ioRatio);{// 让后再处理异步任务队列;
							fetchFromScheduledTaskQueue();
							Runnable task = pollTask();
							if (task == null) {
								return false;
							}
							final long deadline = ScheduledFutureTask.nanoTime() + timeoutNanos;
							long runTasks = 0;
							long lastExecutionTime;
							for (;;) {//循环把Task都跑完;
								try {
									/* 执行一个Task任务;
									*/
									task.run();{//AbstractChannel.AbstactUnsafe.run()
										AbstractUnsafe.register0();
									}
									
								} catch (Throwable t) {
									logger.warn("A task raised an exception.", t);
								}
								runTasks ++;
								if ((runTasks & 0x3F) == 0) {
									lastExecutionTime = ScheduledFutureTask.nanoTime();
									if (lastExecutionTime >= deadline) {
										break;
									}
								}
								task = pollTask();
								if (task == null) {
									lastExecutionTime = ScheduledFutureTask.nanoTime();
									break;
								}
							}

							this.lastExecutionTime = lastExecutionTime;
							return true;
						}
                    }
                }
            } catch (Throwable t) {
                handleLoopException(t);
            }
            
			// Always handle shutdown even if the loop processing threw an exception.
            try {
                if (isShuttingDown()) {
                    closeAll();
                    if (confirmShutdown()) {
                        return;
                    }
                }
            } catch (Throwable t) {
                handleLoopException(t);
            }
        }
}


bootstrap.group(workerGroup)
	.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
	
	TransportConf.connectionTimeoutMs(){
		// 
		long defaultNetworkTimeoutS = JavaUtils.timeStringAsSec(conf.get("spark.network.timeout", "120s"));
		//spark.shuffle.io.connectionTimeout 若设置了, 就以"spark.shuffle.io.connectionTimeout"为准; 否则就以 spark.network.timeout为准(默认120s)
		long defaultTimeoutMs = JavaUtils.timeStringAsSec(conf.get(SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY, defaultNetworkTimeoutS + "s")) * 1000;
		return (int) defaultTimeoutMs;
	}

	
	
	
