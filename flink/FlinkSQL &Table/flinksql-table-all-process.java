// xx模块: xx 进程: xx 线程: 
// jar/coreClass: 
// 处理逻辑: 
// nextTh: 




//关于 查询相关TableFactory的功能:


StreamTableEnvironment.create(env)
    => StreamTableEnvironment.lookupExecutor()
        => TableFactoryService.findAll(factoryClass, propertyMap);


// 1. 在StreamTable环境初始化时, 会查找所有的 TableFactory;
TableFactoryService.findAll(factoryClass, propertyMap);
    findAllInternal(factoryClass, propertyMap, Optional.empty());{
        List<TableFactory> tableFactories = discoverFactories(classLoader);
		return filter(tableFactories, factoryClass, properties);{
            List<T> contextFactories = filterByContext();
        }
    }

    TableFactoryService.findSingleInternal(){
        
    }
//# 核心: 查询并过滤合适TableFactory的核心代码:
    //注意,findAllInternal() 和 findSingleInternal() 都包括以下代码;
find(){
    
    List<TableFactory> tableFactories = discoverFactories(classLoader);
    
	List<T> filtered = filter(tableFactories, factoryClass, properties);{//TableFactoryService.
        //  过滤出 TableFactory的实现类: 如 HBase/CVS/ES/FS/Kafka等 Source/TableTableFactory;
        List<T> classFactories = filterByFactoryClass(factoryClass,properties,foundFactories);
        
        // 根据contect-type? 过滤出单个目标属性: CVS, Kafka 等;
        List<T> contextFactories = filterByContext(factoryClass,properties,classFactories);{//TableFactoryService.
            List<T> matchingFactories = new ArrayList<>();
            
            // 遍历所有 TableFactory的类: 是从哪里加载来的?
            // 这里由KafkaTableSourceSinkFactory, Kafka010Table..; Kafka09Table.., CsvBatchTable, CsvAppendTableSinkFactory;
            
            for (T factory : classFactories) {
                Map<String, String> requestedContext = normalizeContext(factory);{
                    factory.requiredContext();// 由不同factory实现类 返回其必填的 属性;
                    /* KafkaTable 必填的是: connector.type, connector.version, connector.property-version;
                    *
                    */
                }
                
                // 移除 xx.property-version 的属性;
                Map<String, String> plainContext = new HashMap<>(requestedContext);
                plainContext.remove(CONNECTOR_PROPERTY_VERSION);
                plainContext.remove(FORMAT_PROPERTY_VERSION);
                plainContext.remove(CATALOG_PROPERTY_VERSION);

                /* 遍历每个 tableFactory的 必填属性,若 with传进的属性没有该 key(如 connector.type),或key对应的value不对,就添加到 miss & mismatch 表中;
                *    例如: 对弈 KafkaTableFactory其必填的connector.type-> kafka, 如果这个sql with中定义的c.type= filesystem,则就不匹配,则加到 mismatch(错配表);
                *   
                */
                // check if required context is met
                Map<String, Tuple2<String, String>> mismatchedProperties = new HashMap<>();
                Map<String, String> missingProperties = new HashMap<>();
                for (Map.Entry<String, String> e : plainContext.entrySet()) {
                    if (properties.containsKey(e.getKey())) {
                        String fromProperties = properties.get(e.getKey());
                        if (!Objects.equals(fromProperties, e.getValue())) {
                            mismatchedProperties.put(e.getKey(), new Tuple2<>(e.getValue(), fromProperties));
                        }
                    } else {
                        missingProperties.put(e.getKey(), e.getValue());
                    }
                }
                // matchedSize: 该factory必填属性中, 扣除缺失(无key或value不对)后,正在成功的上的属性数量; 如必须匹配4个,结果with只有2个(key,value)完全匹配;
                int matchedSize = plainContext.size() - mismatchedProperties.size() - missingProperties.size();
                if (matchedSize == plainContext.size()) {
                    matchingFactories.add(factory);
                } else {
                    if (bestMatched == null || matchedSize > bestMatched.matchedSize) {
                        bestMatched = new ContextBestMatched<>(
                                factory, matchedSize, mismatchedProperties, missingProperties);
                    }
                }
            }

            if (matchingFactories.isEmpty()) {
                String bestMatchedMessage = null;
                if (bestMatched != null && bestMatched.matchedSize > 0) {
                    StringBuilder builder = new StringBuilder();
                    builder.append(bestMatched.factory.getClass().getName());

                    if (bestMatched.missingProperties.size() > 0) {
                        builder.append("\nMissing properties:");
                        bestMatched.missingProperties.forEach((k, v) ->
                                builder.append("\n").append(k).append("=").append(v));
                    }

                    if (bestMatched.mismatchedProperties.size() > 0) {
                        builder.append("\nMismatched properties:");
                        bestMatched.mismatchedProperties
                            .entrySet()
                            .stream()
                            .filter(e -> e.getValue().f1 != null)
                            .forEach(e -> builder.append(
                                String.format(
                                    "\n'%s' expects '%s', but is '%s'",
                                    e.getKey(),
                                    e.getValue().f0,
                                    e.getValue().f1)));
                    }

                    bestMatchedMessage = builder.toString();
                }
                //noinspection unchecked
                throw new NoMatchingTableFactoryException(
                    "Required context properties mismatch.",
                    bestMatchedMessage,
                    factoryClass,
                    (List<TableFactory>) classFactories,
                    properties);
            }

            return matchingFactories;
        }
        
        // 判断该TableFactory子类 是否支持相关参数
        return filterBySupportedProperties();
    }
        
}

tableSource = TableFactoryUtil.findAndCreateTableSource(table);{
    return findAndCreateTableSource(table.toProperties());{
        return TableFactoryService
				.find(TableSourceFactory.class, properties){//TableFactoryService.find()
                    return findSingleInternal(factoryClass, propertyMap, Optional.empty());{
                        List<TableFactory> tableFactories = discoverFactories(classLoader);
                        
                        List<T> filtered = filter(tableFactories, factoryClass, properties);{
                            //  1. 过滤出 TableFactory的实现类: 如 HBase/CVS/ES/FS/Kafka等 Source/TableTableFactory;
                            List<T> classFactories = filterByFactoryClass(factoryClass,properties,foundFactories);{
                                
                            }
                            
                            // 2. 根据contect-type? 过滤出单个目标属性: CVS, Kafka 等;
                            List<T> contextFactories = filterByContext(factoryClass,properties,classFactories);{//TableFactoryService.
                                List<T> matchingFactories = new ArrayList<>();
                                // 遍历所有 TableFactory的类: 是从哪里加载来的?这里由KafkaTableSourceSinkFactory, Kafka010Table..; Kafka09Table.., CsvBatchTable, CsvAppendTableSinkFactory;
                                for (T factory : classFactories) {
                                    // 1. factory的必填属性; 即TableFactory.requiredContext() 返回值.keySet();
                                    Map<String, String> requestedContext = normalizeContext(factory);
                                    // 所谓plainContext就是 必填属性中去掉 xx.property-version的属性; 正常就只 c.type,c.version这2个属性;
                                    Map<String, String> plainContext = new HashMap<>(requestedContext);
                                    plainContext.remove(CONNECTOR_PROPERTY_VERSION);//移除必填中的 connector.property-version
                                    
                                    //2. 遍历每个 tableFactory的 必填属性,若 with传进的属性没有该 key(如 connector.type),或key对应的value不对,就添加到 miss & mismatch 表中;
                                    Map<String, Tuple2<String, String>> mismatchedProperties = new HashMap<>();
                                    Map<String, String> missingProperties = new HashMap<>();
                                    for (Map.Entry<String, String> e : plainContext.entrySet()) {
                                        if (properties.containsKey(e.getKey())) {// factory.requestField 存在 useDef.pros中,
                                            String fromProperties = properties.get(e.getKey());
                                            // 2.1 比较factory对匹配的属性的值(如type是否都等于kafka, version是否等于0.10),是否相等
                                            if (!Objects.equals(fromProperties, e.getValue())) {
                                                // 将必填字段中 属性名称能匹配但属性值不相等的加到 mismatched, 用于后面报错提示?
                                                mismatchedProperties.put(e.getKey(), new Tuple2<>(e.getValue(), fromProperties));
                                            }
                                        } else {// 属于factory必填属性,但useDef.props中无此属性的; 加到missing中,该factory肯定不合格;
                                            missingProperties.put(e.getKey(), e.getValue());
                                        }
                                    }
                                    // 3. plainContext:必填属性中 key+value完全相等的 情况: matchedSize; 只要有必填中有任一缺失或value不对,都不算matchingFactory;
                                    int matchedSize = plainContext.size() - mismatchedProperties.size() - missingProperties.size();
                                    if (matchedSize == plainContext.size()) {
                                        matchingFactories.add(factory); // 必填属性中 key+value完全相等的factory, 才加到 matchingFactories集合;
                                    } else {
                                        if (bestMatched == null || matchedSize > bestMatched.matchedSize) {
                                            bestMatched = new ContextBestMatched<>(factory, matchedSize, mismatchedProperties, missingProperties);
                                        }
                                    }
                                }
                                if (matchingFactories.isEmpty()) { //一个匹配上的 tableFactory也没有,就抛 NoMatchingTableFactoryException 异常;
                                    String bestMatchedMessage = null;
                                    //noinspection unchecked
                                    throw new NoMatchingTableFactoryException("Required context properties mismatch.",
                                        bestMatchedMessage,factoryClass, (List<TableFactory>) classFactories, properties);
                                }
                                return matchingFactories;
                            }
                            
                            // 3. 将userDef.supportFields 和contextFacotry定义的Support字段一一匹配, 已校验用户的配置是否都支持; 
                            return filterBySupportedProperties(factoryClass, properties,classFactories,contextFactories);{//TableFactoryService.filterBySupportedProperties()
                                //3.1 将用户Table.properties(schema+ 用户编写)中schema.n.file中的数字替换成#,并生成 plainGivenKeys: Set<key>
                                final List<String> plainGivenKeys = new LinkedList<>();
                                properties.keySet().forEach(k -> {
                                    String key = k.replaceAll(".\\d+", ".#");
                                });
                                // 3.2 将(用户配置的)属性都能(在TableFactory.supported属性中)匹配的 factory ,加到 supportedFactories中输出; 
                                List<T> supportedFactories = new LinkedList<>();
                                for (T factory: contextFactories) {
                                    // 从contextFactory中解析 required必填字段; 
                                    Set<String> requiredContextKeys = normalizeContext(factory).keySet();
                                    // 从contextFactory中解析 supported 选填字段; tuple2.f0为所有选填字段; 
                                    Tuple2<List<String>, List<String>> tuple2 = normalizeSupportedProperties(factory);
                                    // givenFilteredKeys: 打平的用户定义(给)的(非必填) 选填属性; 用于过滤table.supported字段?
                                    List<String> givenFilteredKeys = filterSupportedPropertiesFactorySpecific(factory, givenContextFreeKeys);
                                    boolean allTrue = true;
                                    List<String> unsupportedKeys = new ArrayList<>();
                                    for (String k : givenFilteredKeys) {
                                        // 把userDef.supportKeys和 contextFactory.supportFields 进行匹配, 找出任何不能匹配(即不支持的属性)的属性name;
                                        if (!(tuple2.f0.contains(k) || tuple2.f1.stream().anyMatch(k::startsWith))) {
                                            allTrue = false; 
                                            unsupportedKeys.add(k);// 说明该userDef.prop 为非法属性, 不匹配(等于或通配)该factory的任意supported字段
                                        }
                                    }
                                    if(allTrue){
                                        supportedFactories.add(factory);// 该factory所有用户配置属性,都是被支持的;
                                    }
                                }
                                return supportedFactories;
                            }
                        }
                    }
                }
				.createTableSource(properties);
    }
}

// 2. 根据contect-type? 过滤出单个目标属性: CVS, Kafka 等;
List<T> contextFactories = filterByContext(factoryClass,properties,classFactories);{//TableFactoryService.
    List<T> matchingFactories = new ArrayList<>();
    // 遍历所有 TableFactory的类: 是从哪里加载来的?这里由KafkaTableSourceSinkFactory, Kafka010Table..; Kafka09Table.., CsvBatchTable, CsvAppendTableSinkFactory;
    for (T factory : classFactories) {
        // 1. factory的必填属性; 即TableFactory.requiredContext() 返回值.keySet();
        Map<String, String> requestedContext = normalizeContext(factory);
        // 所谓plainContext就是 必填属性中去掉 xx.property-version的属性; 正常就只 c.type,c.version这2个属性;
        Map<String, String> plainContext = new HashMap<>(requestedContext);
        plainContext.remove(CONNECTOR_PROPERTY_VERSION);//移除必填中的 connector.property-version
        
        //2. 遍历每个 tableFactory的 必填属性,若 with传进的属性没有该 key(如 connector.type),或key对应的value不对,就添加到 miss & mismatch 表中;
        Map<String, Tuple2<String, String>> mismatchedProperties = new HashMap<>();
        Map<String, String> missingProperties = new HashMap<>();
        for (Map.Entry<String, String> e : plainContext.entrySet()) {
            if (properties.containsKey(e.getKey())) {// factory.requestField 存在 useDef.pros中,
                String fromProperties = properties.get(e.getKey());
                // 2.1 比较factory对匹配的属性的值(如type是否都等于kafka, version是否等于0.10),是否相等
                if (!Objects.equals(fromProperties, e.getValue())) {
                    // 将必填字段中 属性名称能匹配但属性值不相等的加到 mismatched, 用于后面报错提示?
                    mismatchedProperties.put(e.getKey(), new Tuple2<>(e.getValue(), fromProperties));
                }
            } else {// 属于factory必填属性,但useDef.props中无此属性的; 加到missing中,该factory肯定不合格;
                missingProperties.put(e.getKey(), e.getValue());
            }
        }
        // 3. plainContext:必填属性中 key+value完全相等的 情况: matchedSize; 只要有必填中有任一缺失或value不对,都不算matchingFactory;
        int matchedSize = plainContext.size() - mismatchedProperties.size() - missingProperties.size();
        if (matchedSize == plainContext.size()) {
            matchingFactories.add(factory); // 必填属性中 key+value完全相等的factory, 才加到 matchingFactories集合;
        } else {
            if (bestMatched == null || matchedSize > bestMatched.matchedSize) {
                bestMatched = new ContextBestMatched<>(factory, matchedSize, mismatchedProperties, missingProperties);
            }
        }
    }
    if (matchingFactories.isEmpty()) { //一个匹配上的 tableFactory也没有,就抛 NoMatchingTableFactoryException 异常;
        String bestMatchedMessage = null;
        //noinspection unchecked
        throw new NoMatchingTableFactoryException("Required context properties mismatch.",
            bestMatchedMessage,factoryClass, (List<TableFactory>) classFactories, properties);
    }
    return matchingFactories;
}





// FlinkTable: xx 进程: xx 线程: 
// jar/coreClass: 
// 处理逻辑: 
// nextTh: 


Caused by: org.apache.calcite.sql.validate.SqlValidatorException: Object 'tb_user' not found
	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method) ~[?:1.8.0_261]
	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62) ~[?:1.8.0_261]
	at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45) ~[?:1.8.0_261]
	at java.lang.reflect.Constructor.newInstance(Constructor.java:423) ~[?:1.8.0_261]
	at org.apache.calcite.runtime.Resources$ExInstWithCause.ex(Resources.java:467) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.runtime.Resources$ExInst.ex(Resources.java:560) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.SqlUtil.newContextException(SqlUtil.java:883) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.SqlUtil.newContextException(SqlUtil.java:868) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.SqlValidatorImpl.newValidationError(SqlValidatorImpl.java:5043) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.IdentifierNamespace.resolveImpl(IdentifierNamespace.java:179) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.IdentifierNamespace.validateImpl(IdentifierNamespace.java:184) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.AbstractNamespace.validate(AbstractNamespace.java:84) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validateNamespace(SqlValidatorImpl.java:1067) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validateQuery(SqlValidatorImpl.java:1041) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validateFrom(SqlValidatorImpl.java:3205) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validateFrom(SqlValidatorImpl.java:3187) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validateSelect(SqlValidatorImpl.java:3461) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.SelectNamespace.validateImpl(SelectNamespace.java:60) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.AbstractNamespace.validate(AbstractNamespace.java:84) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validateNamespace(SqlValidatorImpl.java:1067) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validateQuery(SqlValidatorImpl.java:1041) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.SqlSelect.validate(SqlSelect.java:232) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validateScopedExpression(SqlValidatorImpl.java:1016) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validate(SqlValidatorImpl.java:724) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.table.planner.calcite.FlinkPlannerImpl.org$apache$flink$table$planner$calcite$FlinkPlannerImpl$$validate(FlinkPlannerImpl.scala:147) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.table.planner.calcite.FlinkPlannerImpl.validate(FlinkPlannerImpl.scala:111) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.table.planner.operations.SqlToOperationConverter.convert(SqlToOperationConverter.java:189) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.table.planner.delegation.ParserImpl.parse(ParserImpl.java:77) ~[flink-table-planner-blink_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.table.api.internal.TableEnvironmentImpl.sqlQuery(TableEnvironmentImpl.java:640) ~[flink-table-api-java-1.12.2.jar:1.12.2]
	at com.webank.wedatasphere.linkis.engineconnplugin.flink.client.sql.operation.impl.SelectOperation.lambda$createTable$4(SelectOperation.java:198) ~[linkis-engineconn-plugin-flink-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconnplugin.flink.client.context.ExecutionContext.wrapClassLoader(ExecutionContext.java:175) ~[linkis-engineconn-plugin-flink-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconnplugin.flink.client.sql.operation.impl.SelectOperation.createTable(SelectOperation.java:198) ~[linkis-engineconn-plugin-flink-1.0.2.jar:?]
	... 35 more





// SqlClient进程中 TableEnvInit初始化和 CatalogManager构建;
// client.start().openSession().build(): ExecutionContext.initializeTableEnvironment()初始化Table环境资源, initializeCatalogs()根据配置生成Catalogs和curdb;
// client.start().open().parseCommand(line).sqlParser.parse(stmt): PlannerContext.createCatalogReader() 将CatalogManager中curCatalog/DB作为defaultSchemas 封装进FlinkCatalogReader;
// client.start().open().callCommand().callSelect(cmdCall):executor.executeQuery():tableEnv.sqlQuery(selectQuery) 提交Table查询命令: TableEnvironmentImpl.sqlQuery()

SqlClient.main(){
	final SqlClient client = new SqlClient(true, options);
	client.start();{
		final Executor executor = new LocalExecutor(options.getDefaults(), jars, libDirs);
        executor.start();
		final Environment sessionEnv = readSessionEnvironment(options.getEnvironment());
        appendPythonConfig(sessionEnv, options.getPythonConfiguration());
		context = new SessionContext(options.getSessionId(), sessionEnv);
		// 创建 ModuleManager, CatalogManager, FunctionCatalog
		String sessionId = executor.openSession(context);{// LocalExecutor.
			String sessionId = sessionContext.getSessionId();// defaul;
			this.contextMap.put(sessionId, createExecutionContextBuilder(sessionContext).build());{//ExecutionContext$Builder.build()
				return new ExecutionContext<>(this.sessionContext,this.sessionState,this.dependencies,,,);{//ExecutionContext()构造函数, 生成一堆的执行环境;
					classLoader = ClientUtils.buildUserCodeClassLoader();
					// 重要的环境变量解析和 运行对象生成
					initializeTableEnvironment(sessionState);{//ExecutionContext.initializeTableEnvironment()
						EnvironmentSettings settings = environment.getExecution().getEnvironmentSettings();
						final TableConfig config = createTableConfig();
						if (sessionState == null) {
							// Step.1 Create environments
							final ModuleManager moduleManager = new ModuleManager();
							final CatalogManager catalogManager =CatalogManager.newBuilder()
										.classLoader(classLoader).config(config.getConfiguration())
										.defaultCatalog(settings.getBuiltInCatalogName(),
												new GenericInMemoryCatalog(settings.getBuiltInCatalogName(),settings.getBuiltInDatabaseName()))
										.build();{//CatalogManager.Builder.build()
											// default_catalog, default_database
											return new CatalogManager(defaultCatalogName,defaultCatalog,new DataTypeFactoryImpl(classLoader, config, executionConfig));
							}
							CommandLine commandLine =createCommandLine(environment.getDeployment(), commandLineOptions);
							clusterClientFactory = serviceLoader.getClusterClientFactory(flinkConfig);
							// Step 1.2 Initialize the FunctionCatalog if required.
							FunctionCatalog functionCatalog =new FunctionCatalog(config, catalogManager, moduleManager);
							// Step 1.3 Set up session state.
							this.sessionState = SessionState.of(catalogManager, moduleManager, functionCatalog);
							// Must initialize the table environment before actually the
							createTableEnvironment(settings, config, catalogManager, moduleManager, functionCatalog);
							// Step.2 Create modules and load them into the TableEnvironment.
							environment.getModules().forEach((name, entry) -> modules.put(name, createModule(entry.asMap(), classLoader)));
							// Step.3 create user-defined functions and temporal tables then register them.
							registerFunctions();
							// Step.4 Create catalogs and register them. 基于config配置文件,创建多个Catalog和 curCatalog,curDatabase;
							initializeCatalogs();{// ExecutionContext.initializeCatalogs
								// Step.1 Create catalogs and register them.
								environment.getCatalogs().forEach((name, entry) -> {
												Catalog catalog=createCatalog(name, entry.asMap(), classLoader);
												tableEnv.registerCatalog(name, catalog);
											});
								// Step.2 create table sources & sinks, and register them.
								environment.getTables().forEach((name, entry) -> {
												if (entry instanceof SourceTableEntry|| entry instanceof SourceSinkTableEntry) {
													tableSources.put(name, createTableSource(name, entry.asMap()));
												}
												if (entry instanceof SinkTableEntry|| entry instanceof SourceSinkTableEntry) {
													tableSinks.put(name, createTableSink(name, entry.asMap()));
												}
											});
								tableSources.forEach(((TableEnvironmentInternal) tableEnv)::registerTableSourceInternal);
								tableSinks.forEach(((TableEnvironmentInternal) tableEnv)::registerTableSinkInternal);
								// Step.4 Register temporal tables.
								environment.getTables().forEach((name, entry) -> {registerTemporalTable(temporalTableEntry);});
								// Step.5 Set current catalog and database. 从 
								Optional<String> catalog = environment.getExecution().getCurrentCatalog();// "current-catalog" 参数
								Optional<String> database = environment.getExecution().getCurrentDatabase();// current-database 参数
								database.ifPresent(tableEnv::useDatabase);
							}
						}
					}
				}
			}
		}
		
		openCli(sessionId, executor);{//SqlClient.openCli
			CliClient cli = new CliClient(sessionId, executor, historyFilePath)
			cli.open();{//CliClient.
				terminal.writer().append(CliStrings.MESSAGE_WELCOME);
				while (isRunning) {
					terminal.writer().append("\n");
					// 读取一行数据; 
					String line = lineReader.readLine(prompt, null, (MaskingCallback) null, null);
					// 解析用户查询语句生成 Calcite对象,并基于默认 curCatalog,curDB生成 FlinkCatalogReader;
					final Optional<SqlCommandCall> cmdCall = parseCommand(line);{//CliClient.
						parsedLine = SqlCommandParser.parse(executor.getSqlParser(sessionId), line);{
							Optional<SqlCommandCall> callOpt = parseByRegexMatching(stmt);
							if (callOpt.isPresent()) {//先用正则解析; 
								return callOpt.get();
							}else{// 没有正则, 进入这里; 
								return parseBySqlParser(sqlParser, stmt);{//SqlCommandParser.parseBySqlParser
									operations = sqlParser.parse(stmt);{//LocalExecutor.Parser匿名类.parse()
										return context.wrapClassLoader(() -> parser.parse(statement));{// ParserImpl.parse()
											CalciteParser parser = calciteParserSupplier.get();
											FlinkPlannerImpl planner = validatorSupplier.get();
											SqlNode parsed = parser.parse(statement);
											Operation operation =SqlToOperationConverter.convert(planner, catalogManager, parsed)
											.orElseThrow(() -> new TableException("Unsupported query: " + statement));{// SqlToOperationConverter.convert()
												final SqlNode validated = flinkPlanner.validate(sqlNode);{// FlinkPlannerImpl.validate()
													val validator = getOrCreateSqlValidator();{
														val catalogReader = catalogReaderSupplier.apply(false);{
															PlannerContext.createCatalogReader(){
																SqlParser.Config sqlParserConfig = getSqlParserConfig();
																SqlParser.Config newSqlParserConfig =SqlParser.configBuilder(sqlParserConfig).setCaseSensitive(caseSensitive).build();
																SchemaPlus rootSchema = getRootSchema(this.rootSchema.plus());
																// 这里的 currentDatabase,currentDatabase 来源与 CatalogManager.参数; 应该是加载 sql-client-defaults.yaml 后生成的; 
																// 把 currentCatalog("myhive"), currentDatabase("default") 作为默认的 SchemaPaths;
																List<List<String>> defaultSchemas = asList(asList(currentCatalog, currentDatabase), singletonList(currentCatalog));
																return new FlinkCalciteCatalogReader(CalciteSchema.from(rootSchema),defaultSchemas,typeFactory);
															}
														}
														validator = createSqlValidator(catalogReader)
													}
													validate(sqlNode, validator)
												}
												SqlToOperationConverter converter =new SqlToOperationConverter(flinkPlanner, catalogManager);
											}
											
										
										}
									}
									return new SqlCommandCall(cmd, operands);
								}
							}
						}
					}
					cmdCall.ifPresent(this::callCommand);{
						switch (cmdCall.command) {
							case SET:
								callSet(cmdCall);
								break;
							case SELECT:
								callSelect(cmdCall);{//CliClient.callSelect()
									resultDesc = executor.executeQuery(sessionId, cmdCall.operands[0]);{//LocalExecutor.executeQuery()
										final ExecutionContext<?> context = getExecutionContext(sessionId);
										return executeQueryInternal(sessionId, context, query);{//LocalExecutor.
											final Table table = createTable(context, context.getTableEnvironment(), query);{
												return context.wrapClassLoader(() -> tableEnv.sqlQuery(selectQuery));{
													//TableEnvironmentImpl.sqlQuery(selectQuery);
												}
											}
											final DynamicResult<C> result =resultStore.createResult();
											pipeline = context.createPipeline(jobName);
											final ProgramDeployer deployer =new ProgramDeployer(configuration, jobName, pipeline, context.getClassLoader());
											deployer.deploy().get();
											return new ResultDescriptor();
										}
									}
									if (resultDesc.isTableauMode()) {
										tableauResultView =new CliTableauResultView();
									}
								}
								break;
							case INSERT_INTO:
							case INSERT_OVERWRITE:
								callInsert(cmdCall);
								break;
							case CREATE_TABLE:
								callDdl(cmdCall.operands[0], CliStrings.MESSAGE_TABLE_CREATED);
								break;
						}
					}
				}
			}
		}
	}
}


TableEnvironmentImpl.sqlQuery(){
	ParserImpl.parse()
	SqlToOperationConverter.convert()
	FlinkPlannerImpl.validate(sqlNode: SqlNode, validator: FlinkCalciteSqlValidator){
		sqlNode.accept(new PreValidateReWriter(validator, typeFactory));
		sqlNode match { 
			case node: ExtendedSqlNode => node.validate()
			case _ =>
		}
		
		if (sqlNode.getKind.belongsTo(SqlKind.DDL) || sqlNode.getKind == SqlKind.INSERT ){
			return sqlNode
		}
		
		validator.validate(sqlNode);{//SqlValidatorImpl.validate()
			SqlValidatorImpl.validateScopedExpression()
			SqlSelect.validate()
			SqlValidatorImpl.validateQuery()
			SqlValidatorImpl.validateNamespace()
			AbstractNamespace.validate()
			IdentifierNamespace.validateImpl()
			IdentifierNamespace.resolveImpl()
			SqlValidatorImpl.newValidationError()
			SqlUtil.newContextException()
			
			
		}
		
	}
	
}



//flink-table-planner-blink_2.11-1.12.2.jar 依赖的 calcite-core-1.26.0-jar 
// calcite-core-1.26.0 源码

SqlValidatorImpl.validateNamespace(){
	namespace.validate();{//AbstractNamespace[IdentifierNamespace].validate()
		switch (status) {
			case UNVALIDATED:
				status = SqlValidatorImpl.Status.IN_PROGRESS;
				RelDataType type = validateImpl();{//IdentifierNamespace.validateImpl()
					resolvedNamespace = Objects.requireNonNull(resolveImpl(id));{//IdentifierNamespace.resolveImpl()
						final SqlNameMatcher nameMatcher = validator.catalogReader.nameMatcher();
						ResolvedImpl resolved =new SqlValidatorScope.ResolvedImpl();
						try {
							parentScope.resolveTable(names, nameMatcher,SqlValidatorScope.Path.EMPTY, resolved);{// DelegatingScope.
								this.parent.resolveTable(names, nameMatcher, path, resolved);{// EmptyScope.resolveTable()
									final List<Resolve> resolves = ((ResolvedImpl) resolved).resolves;
									Iterator var7 = this.validator.catalogReader.getSchemaPaths().iterator();
									// 关键是这里, 运行构建的 FlinkCalciteCatalogReader.schemaPaths 包含了 myhive.default等 配置的数据库; 
									List<List<String>> schemaPathList = validator.catalogReader.getSchemaPaths();{// 
										validator: FlinkCalciteSqlValidator ; 
										catalogReader: FlinkCalciteCatalogReader [extends CalciteCatalogReader]; {
											List<List<String>> schemaPaths;
											SqlNameMatcher nameMatcher;
										}
									}
									for (List<String> schemaPath : schemaPathList) {
										resolve_(validator.catalogReader.getRootSchema(), names, schemaPath,nameMatcher, path, resolved);{
											
										}
									}
								}
							}
						} catch (CyclicDefinitionException e) {
							if (e.depth == 1) { // 就是这里抛出 "SqlValidatorException: Object 'tb_user' not found"
								throw validator.newValidationError(id,);
							}else{throw new CyclicDefinitionException(e.depth - 1, e.path);}
						}
					}
					if (resolved.count() == 1) {
						resolve = previousResolve = resolved.only();
						if (resolve.remainingNames.isEmpty()) {
							return resolve.namespace;
						}
					}
					// 进到这里, 寿命 上面的resolved != 1, 可能是0,或者>=2; 
					if (nameMatcher.isCaseSensitive()) {// FlinkSqlNameMatcher.isCaseSensitive()
						return this.baseMatcher.isCaseSensitive();{//FlinkSqlNameMatcher.BaseMatcher.isCaseSensitive()
							this.caseSensitive = caseSensitive;// caseSensitive=true;
						}
						SqlNameMatcher liberalMatcher = SqlNameMatchers.liberal();
						this.parentScope.resolveTable(names, liberalMatcher, Path.EMPTY, resolved);
						
					}
					
					// Failed to match.  If we're matching case-sensitively, try a more lenient match. If we find something we can offer a helpful hint.
					// 就是这里抛出 Object 'tb_user' not found; 
					throw validator.newValidationError(id,RESOURCE.objectNotFound(id.getComponent(0).toString()));
				}
				setType(type);
				status = SqlValidatorImpl.Status.VALID;
				break;
			case IN_PROGRESS:
			  throw Util.newInternal("todo: Cycle detected during type-checking");
			case VALID:
			  break;
			default:
			  throw Util.unexpected(status);
		}
	}
	if (namespace.getNode() != null) {
		setValidatedNodeType(namespace.getNode(), namespace.getType());
    }
}



// select * from tb_user; 报 Object 'tb_user' not found
/*
	SqlValidatorImpl.validate() -> SqlValidatorImpl.validateNamespace()
	IdentifierNamespace.resolveImpl() 中, 当 parentScope.resolveTable() 无法解析该id:'tb_user' 并放入 resolved中,最终会
	代码走到最地下的 throw validator.newValidationError(id,RESOURCE.objectNotFound(id.getComponent(0).toString()));
	- 原因应该就是: 所有的 resolveTable需要 'catlog.database.table'格式, 但因为无法解析前面的 myhive.default,导致报错; 
	
*/



















