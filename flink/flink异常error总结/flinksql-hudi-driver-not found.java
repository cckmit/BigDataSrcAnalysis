2022-01-15 14:27:39,232 INFO  org.apache.hudi.common.table.timeline.HoodieActiveTimeline   [] - Create new file for toInstant ?hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi/.hoodie/20220115142739.deltacommit.inflight
2022-01-15 14:27:39,232 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Create instant [20220115142739] for table [lakehouse2_dwd_order_hudi] with type [MERGE_ON_READ]
2022-01-15 14:27:39,232 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [commits the instant 20220115142639] success!
2022-01-15 14:27:39,234 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Loading HoodieTableMetaClient from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-15 14:27:39,238 INFO  org.apache.hudi.common.table.HoodieTableConfig               [] - Loading table properties from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi/.hoodie/hoodie.properties
2022-01-15 14:27:39,240 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=PARQUET) from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-15 14:27:39,240 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Loading Active commit timeline for hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-15 14:27:39,241 INFO  org.apache.hudi.common.table.timeline.HoodieActiveTimeline   [] - Loaded instants upto : Option{val=[==>20220115142739__deltacommit__INFLIGHT]}
2022-01-15 14:27:39,242 ERROR org.apache.hudi.hive.ddl.QueryBasedDDLExecutor               [] - Unable to load Hive driver class
java.lang.ClassNotFoundException: org.apache.hudi.org.apache.hive.jdbc.HiveDriver
	at java.net.URLClassLoader.findClass(URLClassLoader.java:382) ~[?:1.8.0_261]
	at java.lang.ClassLoader.loadClass(ClassLoader.java:418) ~[?:1.8.0_261]
	at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:355) ~[?:1.8.0_261]
	at java.lang.ClassLoader.loadClass(ClassLoader.java:351) ~[?:1.8.0_261]
	at java.lang.Class.forName0(Native Method) ~[?:1.8.0_261]
	at java.lang.Class.forName(Class.java:264) ~[?:1.8.0_261]
	at org.apache.hudi.hive.ddl.JDBCExecutor.createHiveConnection(JDBCExecutor.java:86) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.ddl.JDBCExecutor.<init>(JDBCExecutor.java:48) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HoodieHiveClient.<init>(HoodieHiveClient.java:81) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.<init>(HiveSyncTool.java:80) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.utils.HiveSyncContext.hiveSyncTool(HiveSyncContext.java:51) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteOperatorCoordinator.syncHive(StreamWriteOperatorCoordinator.java:279) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.utils.NonThrownExecutor.lambda$execute$0(NonThrownExecutor.java:67) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [?:1.8.0_261]
	at java.lang.Thread.run(Thread.java:748) [?:1.8.0_261]
2022-01-15 14:27:39,242 ERROR org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [sync hive metadata for instant 20220115142739] error
java.lang.NoSuchMethodError: org.apache.hadoop.hive.ql.metadata.Hive.get(Lorg/apache/hudi/org/apache/hadoop/hive/conf/HiveConf;)Lorg/apache/hadoop/hive/ql/metadata/Hive;
	at org.apache.hudi.hive.HoodieHiveClient.<init>(HoodieHiveClient.java:89) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.<init>(HiveSyncTool.java:80) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.utils.HiveSyncContext.hiveSyncTool(HiveSyncContext.java:51) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteOperatorCoordinator.syncHive(StreamWriteOperatorCoordinator.java:279) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.utils.NonThrownExecutor.lambda$execute$0(NonThrownExecutor.java:67) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [?:1.8.0_261]
	at java.lang.Thread.run(Thread.java:748) [?:1.8.0_261]
2022-01-15 14:28:37,124 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Triggering checkpoint 14 (type=CHECKPOINT) @ 1642228117110 for job 7f3a8e7be48135b5211f8da36d237680.
2022-01-15 14:28:37,125 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [taking checkpoint 14] success!
2022-01-15 14:28:37,335 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [handle write metadata event for instant 20220115142739] success!
2022-01-1

NoClassDefFoundError: org/apache/hudi/org/apache/hadoop/hive/metastore/api/NoSuchObjectException


// 真的在 JobMgr中有次线程


	StreamWriteOperatorCoordinator.notifyCheckpointComplete(){
		executor.execute(()->{
			final boolean committed = commitInstant(this.instant);
			if (committed) {
				writeClient.scheduleCompaction(Option.empty());
			}
			startInstant();
			// sync Hive if is enabled
			syncHiveIfEnabled();{//StreamWriteOperatorCoordinator.syncHiveIfEnabled
				//另起线程 执行 hive同步
				if (tableState.syncHive) {this.hiveSyncExecutor.execute(this::syncHive, this.instant);}{//StreamWriteOperatorCoordinator.syncHive()
					hiveSyncContext
						.hiveSyncTool(){//HiveSyncContext.hiveSyncTool
							return new HiveSyncTool(this.syncConfig, this.hiveConf, this.fs);{
								super(configuration.getAllProperties(), fs);
								this.hoodieHiveClient = new HoodieHiveClient(cfg, configuration, fs);{
									if (!StringUtils.isNullOrEmpty(cfg.syncMode)) {
										HiveSyncMode syncMode = HiveSyncMode.of(cfg.syncMode);
										switch (syncMode) {
											case HMS: ddlExecutor = new HMSDDLExecutor(configuration, cfg, fs); break;
											case HIVEQL: ddlExecutor = new HiveQueryDDLExecutor(cfg, fs, configuration); break;
											case JDBC: 
												ddlExecutor = new JDBCExecutor(cfg, fs);{
													this.config = config;
													createHiveConnection(config.jdbcUrl, config.hiveUser, config.hivePass);{
														// HiveDriver 位于 hive-exec-xx.jar中 ; 报错ClassNotFoundException: org.apache.hudi.org.apache.hive.jdbc.HiveDriver
														Class.forName("org.apache.hive.jdbc.HiveDriver");
													}
												}
												break;
										}
									}else{
										ddlExecutor = cfg.useJdbc ? new JDBCExecutor(cfg, fs) : new HiveQueryDDLExecutor(cfg, fs, configuration);
									}
									// 这里调用了 hive-exec-xx.jar包的 Hive.get(HiveConf c) 方法; 会报错 NoSuchMethodError: org.apache.hadoop.hive.ql.metadata.Hive.get(Lorg
									this.client = Hive.get(configuration).getMSC();{// org.apache.hadoop.hive.ql.metadata.Hive.get(HiveConf c)
										return getInternal(c, false, false, true);
									}
								}
							}
						}
						.syncHoodieTable();
				}
			}
			
			// sync metadata if is enabled
			syncMetadataIfEnabled();
			
		});
	}

