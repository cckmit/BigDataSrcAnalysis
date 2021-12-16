
// 在内核态的通道里,完成数据的copy 转送
// 接口
FileChannel.transferTo()

FileChannelImpl.transferTo(long position, long count, WritableByteChannel target){
	this.ensureOpen();
	if (!var5.isOpen()) {
		
	}else if (var5 instanceof FileChannelImpl && !((FileChannelImpl)var5).writable) {
		throw new NonWritableChannelException();
	}else if (var1 >= 0L && var3 >= 0L) {
		long var6 = this.size();
		if (var1 > var6) {
			return 0L;
		}else {
			int var8 = (int)Math.min(var3, 2147483647L);
			if ((var9 = this.transferToDirectly(var1, var8, var5)) >= 0L) {
				return var9;
			}else {
				var9 = this.transferToTrustedChannel(var1, (long)var8, var5);{//FileChannelImpl.transferToTrustedChannel()
					boolean var6 = var5 instanceof SelChImpl;
					if (!(var5 instanceof FileChannelImpl) && !var6) { // 对于既非FileChannelImpl 又非 SelChImpl的 Channer,读写不了;
						return -4L;
					} else {// 是 FileChannelImpl 或者 SelChImpl的,才可执行这里的 通道复制
						while(var7 > 0L) {
							long var9 = Math.min(var7, 8388608L);
							// 内存映射,仅在堆内 引用执行 实际byte缓存内存对象, 不copy内存;
							MappedByteBuffer var11 = this.map(MapMode.READ_ONLY, var1, var9);
							try{
								int var12 = var5.write(var11);{//FileChannelImpl.write()
									synchronized(this.positionLock) {
										this.begin();
										var4 = this.threads.add();
										do {
											var3 = IOUtil.write(this.fd, var1, -1L, this.nd);{//sun.nio.ch.IOUtil
												if (var1 instanceof DirectBuffer) {
													return writeFromNativeBuffer(var0, var1, var2, var4);{
														int var6 = var1.limit();
														var9 = var4.write(var0, ((DirectBuffer)var1).address() + (long)var5, var7);{
															
															FileDispatcherImpl.write(){
																// write0 调native本地方法了
																return write0(var1, var2, var4, this.append);
															}
															
														}
														
													}
												}else{
													assert var5 <= var6;
													int var7 = var5 <= var6 ? var6 - var5 : 0;
													ByteBuffer var8 = Util.getTemporaryDirectBuffer(var7);
													int var9 = writeFromNativeBuffer(var0, var8, var2, var4);
												}
											}
										}while(var3 == -3 && this.isOpen());
										int var12 = IOStatus.normalize(var3);
									}
								}
								var7 -= (long)var12;
								assert var12 > 0;
								var1 += (long)var12;
							}finally{
								unmap(var11);
							}
						}
					}
				}
				return (var9 >= 0L ? var9 : this.transferToArbitraryChannel(var1, var8, var5);
			}
		}
	}
}
