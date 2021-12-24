
HiveSource: flink-connector-hive_2.11 

Caused by: java.lang.ClassNotFoundException: org.apache.flink.connectors.hive.HiveSource
	at java.net.URLClassLoader.findClass(URLClassLoader.java:382) ~[?:1.8.0_261]
	at java.lang.ClassLoader.loadClass(ClassLoader.java:418) ~[?:1.8.0_261]
	at org.apache.flink.util.FlinkUserCodeClassLoader.loadClassWithoutExceptionHandling(FlinkUserCodeClassLoader.java:64) ~[flink-core-1.12.2.jar:1.12.2]
	at org.apache.flink.util.ChildFirstClassLoader.loadClassWithoutExceptionHandling(ChildFirstClassLoader.java:65) ~[flink-core-1.12.2.jar:1.12.2]
	at org.apache.flink.util.FlinkUserCodeClassLoader.loadClass(FlinkUserCodeClassLoader.java:48) ~[flink-core-1.12.2.jar:1.12.2]
	at java.lang.ClassLoader.loadClass(ClassLoader.java:351) ~[?:1.8.0_261]
	at org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders$SafetyNetWrapperClassLoader.loadClass(FlinkUserCodeClassLoaders.java:172) ~[flink-runtime_2.11-1.12.2.jar:1.12.2]
	at java.lang.Class.forName0(Native Method) ~[?:1.8.0_261]
	at java.lang.Class.forName(Class.java:348) ~[?:1.8.0_261]
	at org.apache.flink.util.InstantiationUtil$ClassLoaderObjectInputStream.resolveClass(InstantiationUtil.java:76) ~[flink-core-1.12.2.jar:1.12.2]
	at java.io.ObjectInputStream.readNonProxyDesc(ObjectInputStream.java:1946) ~[?:1.8.0_261]
	at java.io.ObjectInputStream.readClassDesc(ObjectInputStream.java:1829) ~[?:1.8.0_261]
	at java.io.ObjectInputStream.readOrdinaryObject(ObjectInputStream.java:2120) ~[?:1.8.0_261]
	at java.io.ObjectInputStream.readObject0(ObjectInputStream.java:1646) ~[?:1.8.0_261]
	at java.io.ObjectInputStream.defaultReadFields(ObjectInputStream.java:2365) ~[?:1.8.0_261]
	at java.io.ObjectInputStream.readSerialData(ObjectInputStream.java:2289) ~[?:1.8.0_261]
	at java.io.ObjectInputStream.readOrdinaryObject(ObjectInputStream.java:2147) ~[?:1.8.0_261]
	at java.io.ObjectInputStream.readObject0(ObjectInputStream.java:1646) ~[?:1.8.0_261]
	at java.io.ObjectInputStream.readObject(ObjectInputStream.java:482) ~[?:1.8.0_261]
	at java.io.ObjectInputStream.readObject(ObjectInputStream.java:440) ~[?:1.8.0_261]
	at org.apache.flink.util.InstantiationUtil.deserializeObject(InstantiationUtil.java:615) ~[flink-core-1.12.2.jar:1.12.2]
	at org.apache.flink.util.InstantiationUtil.deserializeObject(InstantiationUtil.java:600) ~[flink-core-1.12.2.jar:1.12.2]
	at org.apache.flink.util.InstantiationUtil.deserializeObject(InstantiationUtil.java:587) ~[flink-core-1.12.2.jar:1.12.2]
	at org.apache.flink.util.SerializedValue.deserializeValue(SerializedValue.java:67) ~[flink-core-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder.create(OperatorCoordinatorHolder.java:337) ~[flink-runtime_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.executiongraph.ExecutionJobVertex.<init>(ExecutionJobVertex.java:225) ~[flink-runtime_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.executiongraph.ExecutionGraph.attachJobGraph(ExecutionGraph.java:866) ~[flink-runtime_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder.buildGraph(ExecutionGraphBuilder.java:257) ~[flink-runtime_2.11-1.12.2.jar:1.12.2]


FlinkUserCodeClassLoader.loadClass(){
	return loadClassWithoutExceptionHandling(name, resolve);{//ChildFirstClassLoader.loadClassWithoutExceptionHandling
		Class<?> c = findLoadedClass(name);
		if (c == null) {
			for (String alwaysParentFirstPattern : alwaysParentFirstPatterns) {
				if (name.startsWith(alwaysParentFirstPattern)) {
					return super.loadClassWithoutExceptionHandling(name, resolve);{// FlinkUserCodeClassLoader.
						return super.loadClass(name, resolve);{//ClassLoader.loadClass()
							Class<?> c = findLoadedClass(name);
							if (c == null) {
								c = findBootstrapClassOrNull(name);
								if (c == null) {
									// 这里在找 org.apache.flink.connectors.hive.HiveSource 时,进到这里
									c = findClass(name);{//URLClassLoader.findClass()
										result = AccessController.doPrivileged();
										if (result == null) {
											throw new ClassNotFoundException(name);
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


