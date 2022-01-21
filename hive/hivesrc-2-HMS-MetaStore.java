


// 2.3.1 

HiveMetaStore.main(){
	HiveMetastoreCli cli = new HiveMetastoreCli(conf);
    cli.parse(args);
	
	startMetaStoreThreads(conf, startLock, startCondition, startedServing);
	startMetaStore(cli.getPort(), conf, startLock,startCondition, startedServing);{
		long maxMessageSize = conf.getLongVar(HiveConf.ConfVars.METASTORESERVERMAXMESSAGESIZE);
		int minWorkerThreads = conf.getIntVar(HiveConf.ConfVars.METASTORESERVERMINTHREADS);
		HMSHandler baseHandler = new HiveMetaStore.HMSHandler("new db based metaserver", conf,false);
		IHMSHandler handler = newRetryingHMSHandler(baseHandler, conf);{//HiveMetaStore.newRetryingHMSHandler
			return newRetryingHMSHandler(baseHandler, hiveConf, false);{RetryingHMSHandler.getProxy(hiveConf, baseHandler, local);{
				// 各种转换和 反射, new RetryingHMSHandler() => invoke("init")=> invokeInternal()
				HiveMetaStore.HMSHandler.init(){
					initListeners = MetaStoreUtils.getMetaStoreListeners();
					wh = new Warehouse(hiveConf);
					// 初始化的关键在这里, 创建 DefaultDB, DefaultRoles, currentUrl
					if (currentUrl == null || !currentUrl.equals(MetaStoreInit.getConnectionURL(hiveConf))) {
						createDefaultDB();{//HMSHandler.createDefaultDB
							createDefaultDB_core(getMS());{
								Configuration conf = getConf();
								return getMSForConf(conf);{
									RawStore ms = threadLocalMS.get();
									if (ms == null) {
										ms = newRawStoreForConf(conf);{// HMSHandler.newRawStoreForConf
											// rawStoreClassName=org.apache.hadoop.hive.metastore.ObjectStore
											String rawStoreClassName = hiveConf.getVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL);
											return RawStoreProxy.getProxy(hiveConf, conf, rawStoreClassName, threadLocalId.get());{
												init();
												this.base = ReflectionUtils.newInstance(rawStoreClass, conf);{
													Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
													result = meth.newInstance();// 反射创建 org.apache.hadoop.hive.metastore.ObjectStore 示例对象;
													setConf(result, conf);{//ObjectStore.setConf()
														Properties propsFromConf = getDataSourceProps(conf);
														
														initialize(propsFromConf);
													}
												}
											}
										}
										ms.verifySchema();
										threadLocalMS.set(ms);
									}
									return ms;
								}
							}
						}
						
						createDefaultRoles();
						addAdminUsers();
						currentUrl = MetaStoreInit.getConnectionURL(hiveConf);
					}
					
					if (hiveConf.getBoolVar(ConfVars.METASTORE_METRICS)) {
						MetricsFactory.init(hiveConf);
					}
					
					expressionProxy = PartFilterExprUtil.createExpressionProxy(hiveConf);
					fileMetadataManager = new FileMetadataManager((ThreadLocalRawStore)this, hiveConf);
					
				}
			}}
		}
		
		if (useSasl) {
			saslServer = bridge.createServer();
		}
		
	}
	
}

	// Metastore初始过程中, 使用 对象存储的初始化: 
	
	ObjectStore.setConf(){
		Properties propsFromConf = getDataSourceProps(conf);
		initialize(propsFromConf);{//ObjectStore.initialize
		int retryLimit = HiveConf.getIntVar(hiveConf,HiveConf.ConfVars.HMSHANDLERATTEMPTS);//hive.hmshandler.retry.attempts指定,默认10;
			int numTries = retryLimit;
			while (numTries > 0){
				initializeHelper(dsProps);{//
					LOG.info("ObjectStore, initialize called");
					pm = getPersistenceManager();// 持久化管理器, 即MySQL/DB ?
					if (pm != null) {
						expressionProxy = createExpressionProxy(hiveConf);
						if (HiveConf.getBoolVar(getConf(), ConfVars.METASTORE_TRY_DIRECT_SQL)) {//hive.metastore.try.direct.sql 参数,默认true;
							directSql = new MetaStoreDirectSql(pm, hiveConf);{
								defaultPartName = HiveConf.getVar(conf, ConfVars.DEFAULTPARTITIONNAME);
								
								String jdoIdFactory = HiveConf.getVar(conf, ConfVars.METASTORE_IDENTIFIER_FACTORY);// datanucleus.identifierFactory 参数,默认 datanucleus1
								if (! ("datanucleus1".equalsIgnoreCase(jdoIdFactory))){//
									isCompatibleDatastore = false;
								}else{ // 默认 datanucleus1, 进入这里 
									boolean isInit= ensureDbInit();{
										
									}
									boolean testOk = runTestQuery(){// MetaStoreDirectSql.runTestQuery()
										Transaction tx = pm.currentTransaction();
										if (!tx.isActive()) {
											tx.begin();
										}
										String selfTestQuery = "select \"DB_ID\" from \"DBS\"";
										query = pm.newQuery("javax.jdo.query.SQL", selfTestQuery);
										query.execute();
										return true;
									}
									isCompatibleDatastore = isInit && testOk;
								}
							}
						}
					}
				}
				return; 
			}
		}
	}

	
	// datanucleus 框架 执行JDBC
	org.datanucleus.api.jdo.JDOQuery.execute(){
		return this.executeInternal();{
			return query.execute();{//Query.execute()
				return this.executeWithArray(new Object[0]);{//SQLQuery.executeWithArray()
					Map executionMap = prepareForExecution(parameterMap);
					return super.executeQuery(executionMap);{
						this.prepareDatastore();
						Object result = this.performExecute(this.inputParameters);{//SQLQuery.performExecute()
							ManagedConnection mconn = storeMgr.getConnection(ec);
							SQLController sqlControl = storeMgr.getSQLController();
							PreparedStatement ps = RDBMSQueryUtils.getPreparedStatementForQuery(mconn, compiledSQL, this);
							RDBMSQueryUtils.prepareStatementForExecution(ps, this, true);
							ResultSet rs = sqlControl.executeStatementQuery(ec, mconn, compiledSQL, ps);{
								return ps.executeQuery();
							}
							ps.clearBatch();
							
						}
						
					}
				}
			}
		}
	}
	



