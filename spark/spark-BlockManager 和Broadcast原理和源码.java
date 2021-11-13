wakeup:705,  (io.netty.channel.nio)


//Broadcase的存储和读取, 移除的完整流程:

"dag-scheduler-event-loop" 线程的提交Stage开始的
/**
*	存储Broadcast是以MEMORY_AND_DISK_SER ( 先Memory,再Disk,传进来就已经bytes不需要BM反序列化)的级别存储的; 其过程: 
*	- sc广播触发创建TorrentBroadcast实例对象: SparkContext.broadcast() -> TorrentBroadcastFactory.newBroadcast() -> new TorrentBroadcast() ->writeBlocks()
*		// 在TorrentBroadcast构造函数中,将 taskBinaryBytes封装进ChunkedByteBuffer,并通过BlockMananger put进MemoryStore.entries:Map[BlockId,Entry]中;
*		- writeBlocks() -> blockManager.putBytes(pieceId, bytes, MEMORY_AND_DISK_SER, tellMaster = true) -> 进入BlockManager的存储机制;
*	
*/
DAGScheduler.submitMissingTasks(){
	var taskBinaryBytes: Array[Byte] = stage match {
          case stage: ShuffleMapStage =>
            JavaUtils.bufferToArray(
              closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
          case stage: ResultStage =>
            JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
        }
	
	taskBinary = sc.broadcast(taskBinaryBytes);{//SparkContext.broadcast()
		val bc = env.broadcastManager.newBroadcast[T](value, isLocal);{//BroadcastManager.newBroadcast()
			broadcastFactory.newBroadcast[T](value_, isLocal, nextBroadcastId.getAndIncrement());{//TorrentBroadcastFactory.newBroadcast()
				new TorrentBroadcast[T](value_, id);{//在TorrentBroadcast的构造函数中完成 taskBinaryBytes的
					private val numBlocks: Int = writeBlocks(obj);{//TorrentBroadcast.writeBlocks()
						blocks.zipWithIndex.foreach { case (block, i) =>
							val pieceId = BroadcastBlockId(id, "piece" + i)
							val bytes = new ChunkedByteBuffer(block.duplicate())
							val isPutSucceed = blockManager.putBytes(pieceId, bytes, MEMORY_AND_DISK_SER, tellMaster = true);{//BlockManager.putBytes()
								doPutBytes(blockId, bytes, level, implicitly[ClassTag[T]], tellMaster);{//BlockManager.doPutBytes()
									doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock) { info => {}};{//BlockManager.doPut()
										val res = putBody(putBlockInfo);{//putBody()即为上面的 info => {}函数方法;
											if (level.useMemory) {//Broadcast的存储级别是: MEMORY_AND_DISK_SER
												val putSucceeded = if (level.deserialized) {
													//Broadcast 不需要序列化, 不进入这里;
												} else{
													memoryStore.putBytes(blockId, size, memoryMode, () => {});{//MemoryStore.putBytes()
														val entry = new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]]);
														entries.put(blockId, entry);// broadcast_1_piece0 -> ChunkedByteBuffer(HeapByteBuffer,size=21721);
													}
												}
											}
										}
									}
								}
							}
							if (! isPutSucceed) {//若写入BroadcastBlock失败, 则抛异常终止进程, 因为后面Executor就无法获取对应Task类了;
								throw new SparkException(s"Failed to store $pieceId of $broadcastId in local BlockManager")
							}
						}
					}
				}
			}
		}
	}
}





BlockManager.api {
	
	BlockManager.getOrElseUpdate(blockId: BlockId,level: StorageLevel,makeIterator: () => Iterator[T]){
		val blockResult: Option[BlockResult]= get[T](blockId)(classTag);{//BlockManager.get(blockId: BlockId)
			//1. 先尝试从本地/本机器上获取该Blcok: 
			val local = getLocalValues(blockId);// BlockManager.getLocalValues()代码详解后面;
			
			if (local.isDefined) {// 若本地能(从内存或磁盘)读取到该blockId的数据,则返回;
				logInfo(s"Found block $blockId locally")
				* INFO  BlockManager - Found block rdd_8153_13 locally
				return local
			}
			
			// 进入这里说明 local 没有该blockId的数据,需要从远程去获取下; 如果远程也没有该Block,就需要重新计算了;
			val remote = getRemoteValues[T](blockId);// 代码详解后面;
			if (remote.isDefined) {
				logInfo(s"Found block $blockId remotely")
				return remote
			}
			None
		}

		blockResult match {
		  case Some(block) => return Left(block)
		  case _ => // Need to compute the block.进入下面的计算
		}
		
		// Initially we hold no locks on this block.
		doPutIterator(blockId, makeIterator, level, classTag, keepReadLock = true) match {
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
	
	
	
	/* 尝试从本地获取该Block数据块: 1.先从bm.blockInfoManager.infos中读BlockInfo对象; 2.再尝试从memoryStore.entries中获取对象类型数据; 3.最终尝试从本地磁盘读取其bytes并反序列化以返回;
	*
	*/
	BlockManager.getLocalValues(blockId){//BlockManager.getLocalValues(blockId)
		logDebug(s"Getting local block $blockId")
		val someBlockInfo:Option[BlockInfo] = blockInfoManager.lockForReading(blockId);{//blockInfoManager.lockForReading(blockId,blocking: Boolean = true):默认阻塞方式获取该Info对象;
			do {
			  infos.get(blockId) match {
				case None => return None // 若用该blockId查询为空, 则返回空,后面会通过rdd.compute()来计算结果;
				
				case Some(info) => //若读到BlockInfo, 则判断该Block当前是否还尚处于写入(Writer)状态, 若尚在写入可能数据不完整; 
				  if (info.writerTask == BlockInfo.NO_WRITER) {// 当该Block不是在 Writting,就可以读取了;
					info.readerCount += 1 //给该Block的读取计数+1;
					readLocksByTask(currentTaskAttemptId).add(blockId)
					logTrace(s"Task $currentTaskAttemptId acquired read lock for $blockId")
					return Some(info) //读取 BlockInfo数据就结束方法返回;
				  }
			  }
			  if (blocking) {
				wait()
			  }
			} while (blocking)
		}
		
		someBlockInfo match {
		  case None => None
		  case Some(info) => //当有BlockInfo时,
			val level = info.level
			logDebug(s"Level for block $blockId is $level")
			val taskAttemptId = Option(TaskContext.get()).map(_.taskAttemptId())
			// 先尝试从内存中读取; 再从磁盘读取;
			if (level.useMemory && memoryStore.contains(blockId)) {// 先尝试从内存中读取: 只有配了storeMem且内存中有该blockId,才去读内存;
				val iter: Iterator[Any] = if (level.deserialized) {//先判断[已存储的]数据是否是 反序列化后的对象数据在存储; 若deserialized=true(已经反序列化成对象了),就直接返回而不需要再反序列化了;
					memoryStore.getValues(blockId).get ;{// MemoryStore.getValues(): 从entries中获取其entry.values对象,若为序列化的bytes数据则抛异常;
						val entry = entries.synchronized { entries.get(blockId) }
						entry match {
							case null => None
							case e: SerializedMemoryEntry[_] => //getValues()要求直接返回对象, 若entry是序列化的数据则要抛 IllegalArgument异常;
								throw new IllegalArgumentException("should only call getValues on deserialized blocks")
							case DeserializedMemoryEntry(values, _, _) => //将对象 values 返回; 
								val x = Some(values)
								x.map(_.iterator)
						}
					} 
				} else {// deserialized=false,还是字节数组,未反序列化, 还需要反序列化成对象返回;
					serializerManager.dataDeserializeStream(blockId, memoryStore.getBytes(blockId).get.toInputStream())(info.classTag)// 先读取bytes,再将其进行反序列化;
				}
				val ci = CompletionIterator[Any, Iterator[Any]](iter, {releaseLock(blockId, taskAttemptId)})//将获取的数据iter封装进迭代器 CompletionIterator中; 
				Some(new BlockResult(ci, DataReadMethod.Memory, info.size)) //由进一步将CompletionIterator封装进 BlockResult对象中,同一接口返回;
			  
			} else if (level.useDisk && diskStore.contains(blockId)) {//再尝试从磁盘读取;当上面内存中没有(可以没配Memory或配了storeMemory但没有该blcokId)
				val diskData = diskStore.getBytes(blockId);{//DiskStore.getBytes(blockId)
					val file = diskManager.getFile(blockId.name){//DiskBlockManager.getFile(): 根据blockId的哈希值映射到目录/子目录/blockId的文件,或新建此文件;
						val hash = Utils.nonNegativeHash(filename) //算出blockId的哈希值;
						val dirId = hash % localDirs.length // 对本地文件数量取余, 即将该blockId哈希映射到本地(多个)文件的某一个文件中; 得到的值为本地文件编号;
						val subDirId = (hash / localDirs.length) % subDirsPerLocalDir //? 按照localDirs切分,再按照64个子文件目录切分;?
						
						// Create the subdirectory if it doesn't already exist
						//subDirs是二维数组: File[][]: 外面那层是dir-> dirId-> subDirs(dirId), 里面那层是subDir->subDirId->subDirs(dirId)(subDirId)
						val subDir = subDirs(dirId).synchronized {// 先定位到目录dir,锁定该目录
							val old = subDirs(dirId)(subDirId) //再定位到子目录 subDir,
							if (old != null) {
								old
							} else {//若该目录没有子目录,则创建子目录: 去subDirId的十六进制为目录名称;
								val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
								if (!newDir.exists() && !newDir.mkdir()) {throw new IOException(s"Failed to create local dir in $newDir.")}
								subDirs(dirId)(subDirId) = newDir
								newDir //将该新建的目录返回;
							}
						}
						new File(subDir, filename) //有了子目录subDir,又知道要创建的文件名filname,新建此文件;
					}
					val blockSize = getSize(blockId)
					
					securityManager.getIOEncryptionKey() match {
						case Some(key) => new EncryptedBlockData(file, blockSize, conf, key)
						  
						case _ =>{//正常进入这里;
							val channel = new FileInputStream(file).getChannel()//打开文件FileIO
							if (blockSize < minMemoryMapBytes) {//当数据量 < 2097152 (2M大小)时,为小文件; 直接读取而不用buff了; 
							  // For small files, directly read rather than memory map.
							  Utils.tryWithSafeFinally {
								val buf = ByteBuffer.allocate(blockSize.toInt)
								JavaUtils.readFully(channel, buf)
								buf.flip() // 刷新缓存; 直接读取;
								new ByteBufferBlockData(new ChunkedByteBuffer(buf), true)
							  } {
								channel.close()
							  }
							} else {// 对于大于2M的数据,需要在内存总映射;
							  Utils.tryWithSafeFinally {
								new ByteBufferBlockData(// 先读取到缓存中;
								  new ChunkedByteBuffer(channel.map(MapMode.READ_ONLY, 0, file.length)), true)
							  } {
								channel.close()
							  }
							}
						}
					}
				}
				
				val iterToReturn: Iterator[Any] = {
					if (level.deserialized) {
					  val diskValues = serializerManager.dataDeserializeStream(blockId,diskData.toInputStream())(info.classTag)
					  maybeCacheDiskValuesInMemory(info, blockId, level, diskValues)
					} else {// 需要反序列化成Java对象,才能返回;
					  val stream = maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
						.map { _.toInputStream(dispose = false) }
						.getOrElse { diskData.toInputStream() }
					  serializerManager.dataDeserializeStream(blockId, stream)(info.classTag)
					}
				}
				val ci = CompletionIterator[Any, Iterator[Any]](iterToReturn, {releaseLockAndDispose(blockId, diskData, taskAttemptId)})
				Some(new BlockResult(ci, DataReadMethod.Disk, info.size))
			} else {
				handleLocalReadFailure(blockId)
			}
		}
		
	}
	
	
	
	// BaseApi 2: 从远程获取序列化Bytes: 1.先发GetLocations到Driver获取locations位置信息;2. 
	BlockManager.getRemoteBytes(blockId);{//BlockManager.getRemoteBytes(blockId: BlockId)
		logDebug(s"Getting remote block $blockId")
		require(blockId != null, "BlockId is null")
		var runningFailureCount = 0
		var totalFailureCount = 0
		// 关键1: 用TransportClient.sendRpc()发远程请求,并循环阻塞直到有返回结果;
		val locations = getLocations(blockId);{//BlockManager.getLocations(blockId):: Seq[BlockManagerId]
			val blocks: Seq[BlockManagerId]= master.getLocations(blockId);{//BlockMangerMaster.getLocations(blockId)
				driverEndpoint.askSync[Seq[BlockManagerId]](GetLocations(blockId));{//RpcEndpointRef.askSync(message): 发同步请求,实际是异步变同步;
					val future = ask[T](message, timeout)// 发出异步Rpc请求
					timeout.awaitResult(future) //阻塞等待同步结果;默认timeout等待时间是 120sec = 2分钟;"spark.rpc.askTimeout", "spark.network.timeout" 中取第一个有的; 这里取spark.rpc.askTimeout的120;
				}
				
				{//解释响应GetLocations请求的driver代码:
					
				}
			}
			val locs = Random.shuffle(blocks)
			val (preferredLocs, otherLocs) = locs.partition { loc => blockManagerId.host == loc.host }
			preferredLocs ++ otherLocs
		}
		val maxFetchFailures = locations.size
		var locationIterator = locations.iterator//将该Block数据分布哪些机器/BlockMangerIds的列表装入迭代器,迭代;
		while (locationIterator.hasNext) {//遍历各blockMangerId,若拿到的data不为空就结束返回;
			val loc = locationIterator.next()// BlockManagerId即为其Blcok块存储节点所在;
			logDebug(s"Getting remote block $blockId from $loc")
			val data = try {
				
				// 关键2: 用TransportClient.sendRpc()发远程消息,并循环阻塞等待远程结果;
				/* BlockTransferService发送Rpc消息并同步结果的调用关系;
					BlockTransferService.fetchBlockSync();{
						NettyBlockTransferService.fetchBlocks();{
							RetryingBlockFetcher.start();{
								RetryingBlockFetcher.fetchAllOutstanding(){
									NettyBlockTransferService.createAndStart();{
										OneForOneBlockFetcher.start();{
											TransportClient.sendRpc();//发送Rpc消息,并阻塞等待返回结果;
										}
									}
								}
							}
						}
					}
				*/
				val managedBuff: ManagedBuffer= blockTransferService.fetchBlockSync(loc.host, loc.port, loc.executorId, blockId.toString);{//BlockTransferService.fetchBlockSync(host, port, executorId, blockId)
					val result = Promise[ManagedBuffer]()
					fetchBlocks(host, port, execId, Array(blockId), new BlockFetchingListener {
							override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
								result.failure(exception)
							}
							override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
								val ret = ByteBuffer.allocate(data.size.toInt)
								ret.put(data.nioByteBuffer())
								ret.flip()
								result.success(new NioManagedBuffer(ret))
							}
						}, shuffleFiles = null);{//fetchBlocks()是抽象方法,由NettyBlockTransferService.fetchBlocks()实现
							try {
								val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
									override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
										val client = clientFactory.createClient(host, port)
										new OneForOneBlockFetcher(client, appId, execId, blockIds.toArray, listener, transportConf, shuffleFiles).start()
									}
								}

								val maxRetries = transportConf.maxIORetries()
								if (maxRetries > 0) {
									new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener)
									.start();{//RetryingBlockFetcher.start()
										fetchAllOutstanding();{//RetryingBlockFetcher.fetchAllOutstanding
											String[] blockIdsToFetch;
											int numRetries;
											RetryingBlockFetchListener myListener;
											synchronized (this) {
												blockIdsToFetch = outstandingBlocksIds.toArray(new String[outstandingBlocksIds.size()]);
												numRetries = retryCount;
												myListener = currentListener;
											}
											
											// Now initiate the fetch on all outstanding blocks, possibly initiating a retry if that fails.
											try {
												fetchStarter.createAndStart(blockIdsToFetch, myListener);{//抽象方法, 有上面匿名内部类代码实现;
													val client = clientFactory.createClient(host, port)
													new OneForOneBlockFetcher(client, appId, execId, blockIds.toArray, listener, transportConf, shuffleFiles)
														.start();{//OneForOneBlockFetcher.start()
															client.sendRpc(openMessage.toByteBuffer(), new RpcResponseCallback() {
																@Override
																public void onSuccess(ByteBuffer response) {
																	try {
																	  streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
																	  logger.trace("Successfully opened blocks {}, preparing to fetch chunks.", streamHandle);
																	  for (int i = 0; i < streamHandle.numChunks; i++) {
																		if (shuffleFiles != null) {
																			client.stream(OneForOneStreamManager.genStreamChunkId(streamHandle.streamId, i), new DownloadCallback(shuffleFiles[i], i));
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
												}
											} catch (Exception e) {
											  logger.error(String.format("Exception while beginning fetch of %s outstanding blocks %s",
												blockIdsToFetch.length, numRetries > 0 ? "(after " + numRetries + " retries)" : ""), e);

											  if (shouldRetry(e)) {
												initiateRetry();
											  } else {
												for (String bid : blockIdsToFetch) {
												  listener.onBlockFetchFailure(bid, e);
												}
											  }
											}
										}
									}
								} else {
									blockFetchStarter.createAndStart(blockIds, listener)
								}
							} catch {
								case e: Exception =>
									logError("Exception while beginning fetchBlocks", e)
									blockIds.foreach(listener.onBlockFetchFailure(_, e))
							}
						}
					
					ThreadUtils.awaitResult(result.future, Duration.Inf)
				}
				managedBuff.nioByteBuffer()//远程可能有多个文件,会先用缓存装着?
			} catch {
				case NonFatal(e) =>{
					runningFailureCount += 1
					totalFailureCount += 1
					if (totalFailureCount >= maxFetchFailures) {
						return None
					}
					null
				}
			}
			
			if (data != null) {//当从该远程BlockMangerId中拿到数据,就结束循环,返回;
				return Some(new ChunkedByteBuffer(data))
			}
			logDebug(s"The value of block $blockId is null")
		}
		logDebug(s"Block $blockId not found")
		None
	}
	
	
	
	// 写入基本Api: doPutIterator(), doPutBytes()
	
	BlockManager.doPutBytes(){
		val putBody=  (info)=>{};//代码详见下面 doPut()中 putBody()部分代码;
		
		val someResult = doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock)(putBody){//BlockManager.doPut(blockId: BlockId)(putBody: BlockInfo => Option[T]): Option[T] 
			require(blockId != null, "BlockId is null")
			require(level != null && level.isValid, "StorageLevel is null or invalid")
			val putBlockInfo = {// 将存储级别/类信息等封装到 BlockInfo对象中;
				val newInfo = new BlockInfo(level, classTag, tellMaster)
				if (blockInfoManager.lockNewBlockForWriting(blockId, newInfo)) {//正常进入这里,即先获得写出锁;
					newInfo
				} else {
					logWarning(s"Block $blockId already exists on this machine; not re-adding it")
					if (!keepReadLock) {releaseLock(blockId)}
					return None
				}
			}
			val startTimeMs = System.currentTimeMillis
			var exceptionWasThrown: Boolean = true
			val result: Option[T] = try {
				val res = putBody(putBlockInfo){// 上面doPut()()的第二个参数体: (info)=>{}
					val startTimeMs = System.currentTimeMillis
					
					val replicationFuture = if (level.replication > 1) {// _replication默认==1, 所以不进入;
						Future {
							replicate(blockId, new ByteBufferBlockData(bytes, false), level, classTag)
						}(futureExecutionContext)
					} else {//正常进入这里;
						null
					}

					val size = bytes.size
					// 若有选memory,则优先storeMemory,包括(12种的8种): MEMORY_ONLY, MEMORY_ONLY_SER, MEMORY_AND_DISK, MEMORY_AND_DISK_SER, OFF_HEAP
					if (level.useMemory) {//StorageLevel._userMemory=true时, 只要有选storeMemory,就会先进入这里; 若Memory存的下,就不会存Disk了;
						val putSucceeded = if (level.deserialized) {//当选序列化存储时: MEMORY_ONLY, MEMORY_AND_DISK; StorageLevel._deserialized=true,表示需要先反序列化成Java对象;
							// 若选MEMORY_ONLY, MEMORY_AND_DISK, 这需要额外的序列化操作;
							val values =serializerManager.dataDeserializeStream(blockId, bytes.toInputStream())(classTag)
							memoryStore.putIteratorAsValues(blockId, values, classTag) match {
								case Right(_) => true
								case Left(iter) => iter.close(); false
						  }
						} else {//若StorageLevel._deserialized=false,表示不需要 反序列化存储; 则直接存储;
							val memoryMode = level.memoryMode
							
							memoryStore.putBytes(blockId, size, memoryMode, () => {
								if (memoryMode == MemoryMode.OFF_HEAP && bytes.chunks.exists(buffer => !buffer.isDirect)) {
								  bytes.copy(Platform.allocateDirectBuffer)
								} else {
								  bytes
								}
							});{//MemoryStore.putBytes(blockId: BlockId, _bytes: () => ChunkedByteBuffer)
								require(!contains(blockId), s"Block $blockId is already present in the MemoryStore") //先确保该blockId未在内存中,若已存在则抛IllegalArgumentEx异常; 被哪个catch?
								
								/* 利用两种内存分配器中的一种计算 storagePool.freeMemory(可用内存) 是否> size(要存储字节大小), 若够用了则返回true;
								*	1.若用动态内存分配器: 
										- 若storageMemroy不够先从executionMemory执行内存中挤占内存, 
										- 若挤占executionMemory后还不够, 则删除已存数据entries中的MemoryEntrys,来腾出内存;
										- 若 挤占executionMemory和删除缓存entries 后还不够, 这时才返回false;
									2. 若用静态内存分配器: StaticMemoryManager
								*/
								val canStoreMem = memoryManager.acquireStorageMemory(blockId, size, memoryMode);{
									//默认 MemoryManger中此每次分配算法: 动态内存分配概念
									UnifiedMemoryManager.acquireStorageMemory(blockId: BlockId,numBytes: Long,memoryMode: MemoryMode){
										assertInvariants();{//UnifiedMemoryManager.assertInvariants(): 保证总堆内存和总堆外内存都是等于 执行内存+存储内存之和;
											assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)// 保证执行内存+store内存==总可用堆内存;
											assert(offHeapExecutionMemoryPool.poolSize + offHeapStorageMemoryPool.poolSize == maxOffHeapMemory) //保证堆外内存是相对的;
										}
										assert(numBytes >= 0)
										
										// 获取用于相应存储的内存池资源, 即该种存储的总可用内存maxMemory;
										val (executionPool, storagePool, maxMemory) = memoryMode match {// 根据是onHeap还是offHeap策略,返回相应的计算和存储内存池: executionPool & storagePool
											case MemoryMode.ON_HEAP => (onHeapExecutionMemoryPool, onHeapStorageMemoryPool, maxOnHeapStorageMemory)
											case MemoryMode.OFF_HEAP => (offHeapExecutionMemoryPool, offHeapStorageMemoryPool, maxOffHeapStorageMemory)
										}
										if (numBytes > maxMemory) {//若要存的字节数 比 总可用内存(计算+存储的总可用内存),直接返回false, 不存了;
											logInfo(s"Will not store $blockId as the required space ")
											return false
										}
										if (numBytes > storagePool.memoryFree) {//当 总可用内存还有但存储内存不够时: 从执行内存中挤出空间来用于存储, 这样影响性能?
											//numBytes - storagePool.memoryFree 算出存储内存的缺口, 若缺口比 execPool的剩余还大, 则把execPool所有剩余内存都分配给storagePool; 此时就会存储也不够,计算内存也不够了;
											val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree, numBytes - storagePool.memoryFree)
											executionPool.decrementPoolSize(memoryBorrowedFromExecution)
											storagePool.incrementPoolSize(memoryBorrowedFromExecution)
										}
										storagePool.acquireMemory(blockId, numBytes);{//StorageMemoryPool.acquireMemory()
											// 表示numBytesToFree 表示 存储不够时还需要额外的存储空间; 当存储空间够用是==0;
											val numBytesToFree = math.max(0, numBytes - memoryFree) //正常返回0, 正常情况memoryFree 大于数据字节数时, numBytes - memoryFree <0,非负数;
											acquireMemory(blockId, numBytes, numBytesToFree);{//StorageMemoryPool.acquireMemory(numBytesToAcquire: Long, numBytesToFree: Long)
												assert(numBytesToAcquire >= 0) //验证要存数据的大小 >=0;
												assert(numBytesToFree >= 0)		//numBytesToFree: 存储还需扩容的大小,  这个肯定大于=0;
												assert(memoryUsed <= poolSize)	
												if (numBytesToFree > 0) {//当存储空间不够用时,进入这里: 从已存储数据中删除部分 以腾出空间;
													memoryStore.evictBlocksToFreeSpace(Some(blockId), numBytesToFree, memoryMode);{//MemoryStore.evictBlocksToFreeSpace()
														assert(space > 0)
														memoryManager.synchronized {
															var freedMemory = 0L
															val rddToAdd = blockId.flatMap(getRddId)
															val selectedBlocks = new ArrayBuffer[BlockId]
															def blockIsEvictable(blockId: BlockId, entry: MemoryEntry[_]): Boolean = {
																entry.memoryMode == memoryMode && (rddToAdd.isEmpty || rddToAdd != getRddId(blockId))
															}
															entries.synchronized {//在这里取出所有缓存的内存数据 entries,从中计算哪些BlockIds/MemoryEntirys需要被删除; 
																val iterator = entries.entrySet().iterator()
																// 该算法应该有问题, entries是LinkedHashMap结构, 会先头开始遍历,而头元素往往都是最新添加进去的.就有可能末尾的过期/陈旧的元素迟迟不被删除;
																while (freedMemory < space && iterator.hasNext) {//freedMemory代表清理所释放出的空间, 当freedMemory还< 要求腾出的空间space时,一直循环删除entries中元素MemoryEntry;
																  val pair = iterator.next()
																  val blockId = pair.getKey
																  val entry = pair.getValue
																  if (blockIsEvictable(blockId, entry)) {//先判断该MemoryEntry是否是可删除的; 判断依据是? 该block正在被读取reading;
																	if (blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
																	  selectedBlocks += blockId //选中该BlockId, 这里只是计算哪些Block/ MemoryEntry将被删除, 尚不执行remove操作;
																	  freedMemory += pair.getValue.size //用于计算释放空间;
																	}
																  }
																}
															}

															def dropBlock[T](blockId: BlockId, entry: MemoryEntry[T]): Unit = {
															val data = entry match {
															  case DeserializedMemoryEntry(values, _, _) => Left(values)
															  case SerializedMemoryEntry(buffer, _, _) => Right(buffer)
															}
															val newEffectiveStorageLevel =
															  blockEvictionHandler.dropFromMemory(blockId, () => data)(entry.classTag)
															if (newEffectiveStorageLevel.isValid) {
																blockInfoManager.unlock(blockId)
															} else {
																blockInfoManager.removeBlock(blockId)
															}
															}

															// 真正执行删除内存缓存数据的操作;
															if (freedMemory >= space) {//本次所释放的空间够用; 进入这里,所以存储空间+执行空间不够这次存储, 当释放了freedMemory空间后就够了;
																logInfo(s"${selectedBlocks.size} blocks selected for dropping " + s"(${Utils.bytesToString(freedMemory)} bytes)")
																for (blockId <- selectedBlocks) {
																	val entry = entries.synchronized { entries.get(blockId) }
																	if (entry != null) {
																		dropBlock(blockId, entry)//从内存entries中删除该entry数据;
																	}
																}
																logInfo(s"After dropping ${selectedBlocks.size} blocks, " + s"free memory is ${Utils.bytesToString(maxMemory - blocksMemoryUsed)}")
																freedMemory
															} else {// 进入这里, 说明存储空间不够, 即便删除所有(可删除)的缓存MemoryEntiry 仍不够; 就放弃本次存储;
																blockId.foreach { id =>
																	logInfo(s"Will not store $id")
																}
																selectedBlocks.foreach { id =>
																  blockInfoManager.unlock(id)
																}
																0L
															}

	}
													}
												}
												//计算是否空间足够了, 若空间足够则 enoughMemory==true; 若空间还不够,则enoughMemory==false;
												val enoughMemory = numBytesToAcquire <= memoryFree; //若上面的 挤占执行内存/移除memory缓存 后有足够空间了:enoughMemory ==true,
												if (enoughMemory) {//若空间足够,就在这里扣除掉 本次存储数据的大小; 并返回true; 后面 memoryManager.acquireStorageMemory()==true, 进入if()语句取执行 entries.put()操作;
													_memoryUsed += numBytesToAcquire
												}
												enoughMemory
											}
										}
									}
									// 当"spark.memory.useLegacyMode"=true时(默认==false), 采用静态内存分配: 对各部分内存静态划分好后便不可变化
									StaticMemoryManager.acquireStorageMemory(blockId: BlockId,numBytes: Long,memoryMode: MemoryMode){
										
									}
									
								}
								if (canStoreMem) {//当内存还够用时: 先将bytes封装到SerializedMemoryEntry对象中,让后put进: MemoryStore.entries:HashMap[BlockId, MemoryEntry[_]] 这个哈希表中;
									val bytes = _bytes()
									assert(bytes.size == size)
									val entry = new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]])
									entries.synchronized {
										entries.put(blockId, entry) //将该数据缓存进内存: 以BlockId为key, 用SerializedMemoryEntry对象封装数据存入;
									}
									logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
									blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
									true
								}else{ false }
							}
						  
						  
						}
						if (!putSucceeded && level.useDisk) {// 若存内存(不够)StoreMemory失败,且开启了存Disk时, 这里进入distStore: 
							logWarning(s"Persisting block $blockId to disk instead.")
							diskStore.putBytes(blockId, bytes){//调用distStore的Api将序列化的数据存磁盘: 
								
							}
						}
					} else if (level.useDisk) {//只有当用户 StorageLevel.DISK_ONLY时,才进入这里;
						diskStore.putBytes(blockId, bytes)
					}

					val putBlockStatus = getCurrentBlockStatus(blockId, info)
					val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
					if (blockWasSuccessfullyStored) {
						info.size = size
						if (tellMaster && info.tellMaster) {
							reportBlockStatus(blockId, putBlockStatus)
						}
						addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
					}
					logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))
					if (level.replication > 1) {
						try {
							ThreadUtils.awaitReady(replicationFuture, Duration.Inf)
						} catch {
							case NonFatal(t) => throw new Exception("Error occurred while waiting for replication to finish", t)
						}
					}
					if (blockWasSuccessfullyStored) {
						None
					} else {
						Some(bytes)
					}
					
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
					logWarning(s"Putting block $blockId failed due to an exception")
					removeBlockInternal(blockId, tellMaster = tellMaster)
					addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
				}
			}
			if (level.replication > 1) {
			  logDebug("Putting block %s with replication took %s"
				.format(blockId, Utils.getUsedTimeMs(startTimeMs)))
			} else {
			  logDebug("Putting block %s without replication took %s"
				.format(blockId, Utils.getUsedTimeMs(startTimeMs)))
			}
			result
		
		
		}
		
		someResult.isEmpty //判断put操作释放成功的依据: 返回结果是否为空;
	}
	
	BlockManager.doPutIterator()
	
	
	
	// 写入BlockManager的更高层(包装)Api: putBytes(), 
	BlockManager.putBytes(){
		/* BlockManger直接存储序列化后的bytes: BlockManager.putBytes() 和RDD存储计算方法: BlockManager.getOrElseUpdate()
		*	- BlockManager.putBytes() 调用该方法地方: 
		*		1. "dag-scheduler"在将Stage分成多个Task提交的 DAGScheduler.submitMissingTasks(stage)中, 会将stage序列化成task字节数组并广播给各Executor:  sc.broadcast(taskBinaryBytes)
		*		2. "Executor task launch.."中某个Task计算完将序列化的serDirResult结果要返回给Driver时,但其大小> maxDirectResultSize时,按MEMORY_AND_DISK_SER先存于内存中: Executor.TaskRunner.run(): env.blockManager.putBytes()
		*	- BlockManager.getOrElseUpdate() 被调用的地方: 
		*		1. "Executor task launch.."中, Task.runTask()进行Task计算而调RDD.getOrCompute()时,先尝试从缓存读取,若没有再计算和更新: SparkEnv.get.blockManager.getOrElseUpdate()
		*/
		
		require(bytes != null, "Bytes is null")
		doPutBytes(blockId, bytes, level, implicitly[ClassTag[T]], tellMaster)
	}
	
	
	
}



# 临时存放 Netty Rpc通信源码
	
	
	//Rpc-BaseApi: 同步请求并等待结果;
	RpcEndpointRef.askSync(message: Any, timeout: RpcTimeout);{//NettyRpcEndpointRef直接调其父类RpcEndpointRef的方法;
		val future = ask[T](message, timeout);{//NettyRpcEndpointRef[extends RpcEndpointRef].ask(): 
			nettyEnv.ask(new RequestMessage(nettyEnv.address, this, message), timeout);{//NettyRpcEnv.ask(message: RequestMessage, timeout: RpcTimeout)
				val promise = Promise[Any]()
				val remoteAddr = message.receiver.address
				try {
				  if (remoteAddr == address) {//当获取地址就是本地时,调用Dispatcher进行本地消息处理;
					val p = Promise[Any]()
					p.future.onComplete {
					  case Success(response) => onSuccess(response)
					  case Failure(e) => onFailure(e)
					}(ThreadUtils.sameThread)
					dispatcher.postLocalMessage(message, p)
				  } else {//若为远程发送,进入这里: 
					// 包装一下Rpc消息;
					val rpcMessage = RpcOutboxMessage(message.serialize(this), onFailure, (client, response) => onSuccess(deserialize[Any](client, response)))
					
					postToOutbox(message.receiver, rpcMessage);{
						if (receiver.client != null) {//什么时候,进入这里?
							message.sendWith(receiver.client)
						} else {// 进入这里
						  require(receiver.address != null, "Cannot send message to client endpoint with no listen address.")
						  
						  // outboxes:Map[RpcAddress, Outbox],各个远程Rpc存放的,要处理消息的队列(消息盒子); 该rpcAddress若还没Out消息盒子则为其新建Outbox,并将后续所有该地址的消息都存于此盒子中;
						  val targetOutbox = {
							val outbox = outboxes.get(receiver.address)
							if (outbox == null) {// 若该地址,还是本NettyEnv.this第一次发送该远程地址,则为其新建消息缓存盒子Outbox;
							  val newOutbox = new Outbox(this, receiver.address)
							  val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
							  if (oldOutbox == null) {
								newOutbox
							  } else {
								oldOutbox
							  }
							} else {//outboxed里有这个远程连接(地址),
							  outbox
							}
						  }
						  if (stopped.get) {
							outboxes.remove(receiver.address)
							targetOutbox.stop()
						  } else {//正常,进入这里: 向该RpcAddress对应的Outbox.messages<List[OutboxMessage]>消息列表中添加该消息进去;
							targetOutbox.send(message);{//Outbox.send(message: OutboxMessage)
								val dropped = synchronized {
									if (stopped) {
										true
									} else {//正常,直接将message添加到列表; 若添加成功再drainOutbox()
										messages.add(message)
										false
									}
								}
								if (dropped) { 
									message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
								} else {//正常, 排空消息盒子,即循环取出消息发送,直到完毕;
									drainOutbox();{//Outbox.drainOutbox()
										var message: OutboxMessage = null
										synchronized {// 一堆的状态确认正常;
										  if (stopped) {return}
										  if (connectFuture != null) {return}
										  if (client == null) {
											launchConnectTask()
											return
										  }
										  if (draining) {return}
										  message = messages.poll()
										  if (message == null) {return}
										  draining = true
										}
										
										while (true) {
										  try {
											val _client = synchronized { client }
											if (_client != null) {//_client==TransportClient,作为真正的转发客户端,当然不能为空;
												message.sendWith(_client);{
													// 观察来看, 当发单路(不需要reply)的消息,就用这个:比如UpStatus
													OneWayOutboxmMessage.sendWith(client: TransportClient){
														client.send(content);//TransportClient.send(): 代码详解后面
													}
													
													// 当调用askSync(),而需要回复的时候: GetLocations,
													RpcOutboxMessage.sendWith(client: TransportClient){
														this.client = client
														this.requestId = client.sendRpc(content, this);//TransportClient.sendRpc(); 代码详解 BaseApi;
														
													}
												}
											} else {
												assert(stopped == true)
											}
										  } catch {
											case NonFatal(e) =>
											  handleNetworkFailure(e)
											  return
										  }
										  
										  synchronized {
											if (stopped) {
											  return
											}
											message = messages.poll()
											if (message == null) {
											  draining = false
											  return
											}
										  }
										}
									}
								}
							}
						  }
						}
					
					}
					promise.future.onFailure {
					  case _: TimeoutException => rpcMessage.onTimeout()
					  case _ =>
					}(ThreadUtils.sameThread)
				  }
				  
				  promise.future.onComplete { v =>
					timeoutCancelable.cancel(true)
				  }(ThreadUtils.sameThread)
				} catch {
				  case NonFatal(e) =>
					onFailure(e)
				}
				promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)
				
			}
		}
		timeout.awaitResult(future) //阻塞等待同步结果;默认timeout等待时间是 120sec = 2分钟;"spark.rpc.askTimeout", "spark.network.timeout" 中取第一个有的; 这里取spark.rpc.askTimeout的120;
		
	}
	
	
	
	/* Rpc-BaseApi: 转发Rpc请求;
	 * 将 RequestMessage任务请求 转换 Runnable task, 并将该task存于 NioEventLoop.takeQueue任务队列中,由其内部线程循环取出task并执行:完成请求;
		TransportClient.sendRpc(){
			AbstractChannel.writeAndFlush() ->DefaultChannelPipeline.writeAndFlush();{
				AbstractChannelHandlerContext.writeAndFlush(){
					AbstractChannelHandlerContext.write(){
						AbstractChannelHandlerContext.safeExecute(){
							executor.execute(runnable);{//executor==NioEventLoop,这里调用执行其父类的SingleThreadEventExecutor.execute()方法
								if (inEventLoop) {//Thread.currentThread() == this.thread, 判断当前线程是否是其创建线程;
									addTask(task);
								}else{
									SingleThreadEventExecutor.startThread()
									SingleThreadEventExecutor.addTask(Runnable task);
								}
								SingleThreadEventExecutor.wakeup(inEventLoop);
							}
						}
					}
				}
			}
		}
	*/
	TransportClient.sendRpc(content, this);{//TransportClient.sendRpc(ByteBuffer message, RpcResponseCallback callback)
		long startTime = System.currentTimeMillis();
		long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
		handler.addRpcRequest(requestId, callback);
		ChannelFuture channelFuture= channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message)));{
			NioSocketChannel.writeAndFlush(Object msg);-> AbstractChannel.writeAndFlush(Object msg);{//其父类的实现方法;
				return pipeline.writeAndFlush(msg);{//DefaultChannelPipeline.writeAndFlush(Object msg) 
					return tail.writeAndFlush(msg);{//AbstractChannelHandlerContext.writeAndFlush(Object msg)
						return writeAndFlush(msg, newPromise());{//AbstractChannelHandlerContext.writeAndFlush(Object msg, ChannelPromise promise)
							write(msg, true, promise);{//AbstractChannelHandlerContext.write()
								AbstractChannelHandlerContext next = findContextOutbound();
								EventExecutor executor = next.executor();{//AbstractChannelHandlerContext.executor(): executor== NioEventLoop的实例
									if (executor == null) {//默认==null,进入这里; 此case executor== NioEventLoop 的实例对象;
										Channel channel = channel();{//AbstractChannelHandlerContext.channel()
											return pipeline.channel();{//DefaultChannelPipeline.channel()
												return channel;//返回 NioSocketChannel 的实例对象;
											}
										}
										return channel.eventLoop();{//NioSocketChannel的父类AbstractNioChannel.eventLoop()
												return (NioEventLoop) super.eventLoop();{//AbstractChannel.eventLoop()
													EventLoop eventLoop = this.eventLoop;//此时的EventLoop的实现对象为: NioEventLoop
													if (eventLoop == null) {
														throw new IllegalStateException("channel not registered to an event loop");
													}
													return eventLoop;//NioEventLoop的实例对象;
												}
											}
									} else {
										return executor;
									}
								}
								if (executor.inEventLoop(){return Thread.currentThread() == this.thread}) {//当为同一线程时,进入这里;
									if (flush) {
										next.invokeWriteAndFlush(msg, promise);
									} else {
										next.invokeWrite(msg, promise);
									}
								} else {//当不是创建线程时: 本访问线程为"Executor task launch",而executor中的线程为"rpc-client"线程;
									AbstractWriteTask task;
									if (flush) {//进入这里, 创建一个Write且会刷新的Task任务;
										task = WriteAndFlushTask.newInstance(next, msg, promise);{
											WriteAndFlushTask task = RECYCLER.get();
											init(task, ctx, msg, promise);{//将HandlerContext,msg,ChannelPromise,size等都加到Task变量中; 用于后面取出计算?
												task.ctx = ctx;
												task.msg = msg;
												task.promise = promise;
												if (ESTIMATE_TASK_SIZE_ON_SUBMIT) {
													ChannelOutboundBuffer buffer = ctx.channel().unsafe().outboundBuffer();
													if (buffer != null) {
														task.size = ctx.pipeline.estimatorHandle().size(msg) + WRITE_TASK_OVERHEAD;
														buffer.incrementPendingOutboundBytes(task.size);
													} else {
														task.size = 0;
													}
												} else {
													task.size = 0;
												}
											}
											return task;
										}
									}  else {
										task = WriteTask.newInstance(next, msg, promise);
									}
									safeExecute(executor, task, promise, msg);{//AbstractChannelHandlerContext.safeExecute(EventExecutor executor, Runnable runnable, ChannelPromise promise, Object msg)
										// 调用
										executor.execute(runnable);{//NioEventLoop.executor(Runnable runnable)
											boolean inEventLoop = inEventLoop();{return Thread.currentThread() == this.thread}
											if (inEventLoop) {
												addTask(task);
											} else {//正常,进入这里, 因为不是创建线程;
												startThread();{//SingleThreadEventExecutor.startThread(): 里面一般不会再启动新线程了;
													if (STATE_UPDATER.get(this) == ST_NOT_STARTED) {//正常这里为false,进不去;
														if (STATE_UPDATER.compareAndSet(this, ST_NOT_STARTED, ST_STARTED)) {
															thread.start();
														}
													}
												}
												
												addTask(task);{//SingleThreadEventExecutor.addTask(Runnable task)
													boolean offerSucceed = offerTask(task);{
														if (isShutdown()) {
															reject();
														}
														return taskQueue.offer(task);//向 taskQueue:Queue<Runnable> 队列的末尾追加一个Task;
													}
													if (!offerSucceed) {
														rejectedExecutionHandler.rejected(task, this);
													}
												}
												if (isShutdown() && removeTask(task)) {
													reject();
												}
											}
											
											if (!addTaskWakesUp && wakesUpForTask(task)) {//正常为ture,进入这里;
												wakeup(inEventLoop);{//NioEventLoop重写了其父类SingleThreadEventExecutor的方法, 进行selector的唤醒;
													if (!inEventLoop && wakenUp.compareAndSet(false, true)) {
														selector.wakeup();{
															EPollSelectorImpl.wakeup(){//对于Linux的实现;
																
															}
														}
													}
												}
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
		}
		
		channelFuture.addListener(future -> {
			  if (future.isSuccess()) {
				long timeTaken = System.currentTimeMillis() - startTime;
				if (logger.isTraceEnabled()) {
				  logger.trace("Sending request {} to {} took {} ms", requestId,
					getRemoteAddress(channel), timeTaken);
				}
			  } else {
				String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
				  getRemoteAddress(channel), future.cause());
				logger.error(errorMsg, future.cause());
				handler.removeRpcRequest(requestId);
				channel.close();
				try {
				  callback.onFailure(new IOException(errorMsg, future.cause()));
				} catch (Exception e) {
				  logger.error("Uncaught exception in RPC response callback handler!", e);
				}
			  }
			});
		return requestId;
	}
	
	







// Driver端的 Broadcast写入和持久化;

Broadcast.value() = {
	assertValid()
	getValue();{//TorrentBroadcast.getValue()	Torrent:绵延激流, TorrentBroadcast:流水一样的广播变量?
		_value = readBroadcastBlock();{//TorrentBroadcast.readBroadcastBlock()
			setConf(SparkEnv.get.conf)
			val blockManager = SparkEnv.get.blockManager
			//尝试从本地获取该Block数据块: 1.先尝试从memoryStore.entries中获取对象类型数据; 2.最终尝试从本地磁盘读取其bytes并反序列化以返回;
			blockManager.getLocalValues(broadcastId) match {
				case Some(blockResult) => val x = blockResult.data.next().asInstanceOf[T]
				
				case None => //若本地(内存/磁盘)没有该数据块(blockId==broadcastId),  才会触发 TorrentBroadcast.readBlocks()读取Broadcast的Block;
					logInfo("Started reading broadcast variable " + id)
					val startTimeMs = System.currentTimeMillis()
					
					val blocks = readBlocks();{//TorrentBroadcast.readBlocks()
						
						for (pid <- Random.shuffle(Seq.range(0, numBlocks))) {// 根据该TorrentBroadcast.numBlocks文件数量,遍历获取其各piece:
							val pieceId = BroadcastBlockId(id, "piece" + pid)// 用该TorrentBroadcat.id_piece_pid(文件序号)拼接成pieceId作为查数据块的BlockId;
							logDebug(s"Reading piece $pieceId of $broadcastId")
							// 获取该TorrentBroadcast的每一piece数据块: 1.先尝试本机读取; 2.再尝试bm.getRemoteBytes()远程获取; 3.再没有就抛异常;
							bm.getLocalBytes(pieceId) match {//
								case Some(block) => 
									blocks(pid) = block //将从BlockManger中获取的数据存于内存,以供下次使用;
									releaseLock(pieceId)// 释放锁;
									
								case None =>  bm.getRemoteBytes(pieceId) match {//BlockManager.getRemoteBytes(blockId),实现细节 BlockManager_源码详解;
									case Some(b) => {
										// 将从远程driver/executor获取的pieceId数据,存于本机的BlockManager中(MEMORY_AND_DISK_SER,先内存再磁盘,是已序列化的); 若存储本机失败就抛异常;
										if (!bm.putBytes(pieceId, b, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)) {//BlockManager.putBytes(),实现细节,位于BlockManager解释文档中;
											throw new SparkException(s"Failed to store $pieceId of $broadcastId in local BlockManager")
										}
										blocks(pid) = new ByteBufferBlockData(b, true)
									}
									case None => throw new SparkException(s"Failed to get $pieceId of $broadcastId")
								}
							}
							
						}

					}
					
					logInfo("Reading broadcast variable " + id + " took" + Utils.getUsedTimeMs(startTimeMs))
					try {
						//把刚才读取的数据,再写入本地?
						val storageLevel = StorageLevel.MEMORY_AND_DISK
						if (!blockManager.putSingle(broadcastId, obj, storageLevel, tellMaster = false)) {
							throw new SparkException(s"Failed to store $broadcastId in BlockManager")
						}
						obj
					} finally {
						blocks.foreach(_.dispose())
					}
			
			}
		}
	}
}





// Executor端的Broadcast的读取;



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

	
	
	
