针对HotSpot VM的实现，它里面的GC其实准确分类只有两大种：

Partial GC：并不收集整个GC堆的模式
	Young GC：只收集young gen的GC; 
	Old GC：只收集old gen的GC。只有CMS的concurrent collection是这个模式; 
	Mixed GC：收集整个young gen以及部分old gen的GC。只有G1有这个模式

Full GC：收集整个堆，包括young gen、old gen、perm gen（如果存在的话）等所有部分的模式。

Major GC通常是跟full GC是等价的，收集整个GC堆。但因为HotSpot VM发展了这么多年，各种名词解读混乱, 
当有人说“major GC”的时候一定要问清楚他想要指的是上面的full GC还是old GC



serial GC (Serial Young GC ＋ Serial Old GC), 触发Full GC的条件
	- 统计数据说之前young GC的平均晋升大小比目前old gen剩余的空间大，则不会触发young GC而是转为触发full GC（
	- perm gen/Metaspace/方法区? 需要分配但已经没有足够空间时, Full GC; 
Serial GC触发的Full GC的清理项
		- 能收集old gen的GC都会同时收集整个GC堆，包括young gen
		- 清理 OldGen, 清理NewSize, 清理其他所有区?(方法区?)

Serial GC: Serial Young GC ＋ Serial Old GC 
	A. YGC
		- 触发FullGC的条件
			* eden区满了,就YoungGC; 
			* 并且还要判断 promoteSize < oldFreeSize, 则仅执行YoungGC; 
		- 执行的清理区域
			* Eden区 + From/To 区; 

	B. Full GC; Serial Old GC
		- 触发FullGC的条件
			* 执行Young GC时候预测其promote的object的总size超过老生代剩余size; 
			* 如果 promoteSize >= oldFreeSize 
		- 执行的清理区域
			* 执行一次Full GC: Eden, NewSize,OldGen, Metaspace/Perm, Code, CSS, 
			


Parallel GC: 
	Parallel YoungGC + 非并行的PS MarkSweep GC
		A. YGC
			- 触发FullGC的条件
				* eden区满了,就YoungGC; 
				* 并且还要判断 promoteSize < oldFreeSize, 则仅执行YoungGC; 
			- 执行的清理区域
				* Eden区 + From/To 区; 
			
		B. Full GC
			- 触发FullGC的条件
				* case 1: 调用System.gc();
				* case 2: OldGen 不足: newUsed > oldFreeSize, 检查 [老年代最大可用的连续空间] 是否大于 [Survivor区移至老年区的对象的平均大小] ;
					- 版本2: newUsedSize > oldFreeSize:  判断老年代最大的可用连续空间是否大于新生代的所有对象总空间
				* case 3: OldGen 不足: historyPromoteAvgSize > oldFreeSize;  检查[老年最大可用的连续空间]是否大于[历次晋升到老年代对象的平均大小]
				* case 4: Perm/ Metaspace/方法区不足
				
			- 执行的清理区域
				* 先执行一次 YoungGC;
				* 执行一次Full GC: Eden, NewSize,OldGen, Metaspace/Perm, Code, CSS, 
		
		
	Parallel YoungGC + Parallel Old: （-XX:+UseParallelGC）
		A. YGC
		
		B. Full GC
			- 触发FullGC的条件
			
			- 执行的清理区域
				* 先执行一次young GC
				* 执行一次Full GC: Eden, NewSize,OldGen, Metaspace/Perm, Code, CSS, 


CMS算法: ParNew（Young）GC + CMS（Old）GC
		A. YGC
			- 触发FullGC的条件
				* eden区满了,就YoungGC; 
				* 并且还要判断 promoteSize < oldFreeSize, 则仅执行YoungGC; 
			- 执行的清理区域
				* Eden区 + From/To 区; 
		B. Initial Mark 初始化标记 (STW)
			- 触发条件: 
				* 老生代使用比率超过某值
				
			- 执行的清理区域
				* ?
				
		B. Full GC
			- 触发FullGC的条件
				* case 1: 调用System.gc();
				* case 2: OldGen 不足: newUsed > oldFreeSize, 检查 [老年代最大可用的连续空间] 是否大于 [Survivor区移至老年区的对象的平均大小] ;
					- 版本2: newUsedSize > oldFreeSize:  判断老年代最大的可用连续空间是否大于新生代的所有对象总空间
				* case 3: OldGen 不足: historyPromoteAvgSize > oldFreeSize;  检查[老年最大可用的连续空间]是否大于[历次晋升到老年代对象的平均大小]
				* case 4: Perm/ Metaspace/方法区不足
				* case 5: YGC promotion failed (仅CMS?), survivor放不下,old也放不下; 
				* case 6: CMS中 concurrent mode failure, ?
				
			- 执行的清理区域
				* 先执行一次 YoungGC;
				* 执行一次Full GC: Eden, NewSize,OldGen, Metaspace/Perm, Code, CSS, 
				

G1 GC: Young GC + mixed GC（新生代，再加上部分老生代）＋ Full GC for G1 GC




















