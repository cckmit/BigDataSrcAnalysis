private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    => Runtime.currentRuntime = JVM.maxMemory()
    - 518979584 => 494.93M;
    - 470286336 => 448.5M;


reservedMemory= conf.getLong("spark.testing.reservedMemory", 
                    if (conf.contains("spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES)//RESERVED_SYSTEM_MEMORY_BYTES=300*1024*1024=300M;
    = >  固定大小 314572800, 300M; 
 

val minSystemMemory = (reservedMemory *1.5).ceil.toLong 
    - 471859200, 450M( 300 * 1.5)

    
    
// Spark 初始化设定内存的逻辑

getMaxMemory:213, UnifiedMemoryManager$ (org.apache.spark.memory)
apply:199, UnifiedMemoryManager$ (org.apache.spark.memory)
create:330, SparkEnv$ (org.apache.spark)
createDriverEnv:175, SparkEnv$ (org.apache.spark)
createSparkEnv:256, SparkContext (org.apache.spark)
<init>:423, SparkContext (org.apache.spark)
createNewSparkContext:838, StreamingContext$ (org.apache.spark.streaming)
<init>:85, StreamingContext (org.apache.spark.streaming)

new SparkContext(){
    createSparkEnv()-> SparkEnv.createDriverEnv()-> SparkEnv.create(){
        val memoryManager: MemoryManager = 
          if (useLegacyMemoryManager) {
                new StaticMemoryManager(conf, numUsableCores)
          } else { //1.6版本后,默认进入这里; 动态内存分配(不限定内存大小);
                UnifiedMemoryManager(conf, numUsableCores);{
                    // Runtime代表JVM交互环境; Runtime.maxMemory() 获取JVM能从操作申请的最大内存; 可由-Xmx指定;
                    val systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
                    //这里硬性指定要给系统预留(reservedMemory) 300M的内存,(若Executor是本地, 则包括SparkContext,Executor,SparkEnv,BlockManger,Mertis,WebServer等都共此300M);
                    val reservedMemory = conf.getLong("spark.testing.reservedMemory",
                        if (conf.contains("spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES) //RESERVED_SYSTEM_MEMORY_BYTES为常量: 30 *1024*1024=30M;
                    val minSystemMemory = (reservedMemory * 1.5).ceil.toLong // 300M*1.5=450M 作为对JVM maxMemory的最小要求;
                    
                    // 要求JVM能申请到的内存, 至少要大于 450M;
                    if (systemMemory < minSystemMemory) {
                      throw new IllegalArgumentException(s"System memory $systemMemory must " +s"be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
                        s"option or spark.driver.memory in Spark configuration.")
                    }
                    // SPARK-12759 Check executor memory to fail fast if memory is insufficient
                    if (conf.contains("spark.executor.memory")) {
                      val executorMemory = conf.getSizeAsBytes("spark.executor.memory")
                      if (executorMemory < minSystemMemory) { //确保用户自己指定的 executorMemory也要比 450M大;
                        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
                          s"$minSystemMemory. Please increase executor memory using the " +
                          s"--executor-memory option or spark.executor.memory in Spark configuration.")
                      }
                    }
                    
                    // JVM最大内存 - 系统预留450M内存, 剩下的才能进 
                    val usableMemory = systemMemory - reservedMemory
                    val memoryFraction = conf.getDouble("spark.memory.fraction", 0.6)
                    (usableMemory * memoryFraction).toLong
                }
          }
    }
        SparkEnv.createDriverEnv(conf, isLocal, listenerBus, SparkContext.numDriverCores(master)){
            create(){
                val memoryManager: MemoryManager =
                  if (useLegacyMemoryManager) {
                        new StaticMemoryManager(conf, numUsableCores)
                  } else {
                        UnifiedMemoryManager(conf, numUsableCores)
                  }
            }
        }
    }
}