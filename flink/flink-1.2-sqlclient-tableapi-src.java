


/** 3	FlinkSqlCli 
*
*/



// SqlClient进程中 TableEnvInit初始化和 CatalogManager构建;
// client.start().openSession().build(): ExecutionContext.initializeTableEnvironment()初始化Table环境资源, initializeCatalogs()根据配置生成Catalogs和curdb;
// A. client.start().open().parseCommand(line).sqlParser.parse(stmt): PlannerContext.createCatalogReader() 将CatalogManager中curCatalog/DB作为defaultSchemas 封装进FlinkCatalogReader;
// B. client.start().open().callCommand().callSelect(cmdCall):executor.executeQuery():tableEnv.sqlQuery(selectQuery) 提交Table查询命令: TableEnvironmentImpl.sqlQuery()

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
				return new ExecutionContext<>(this.sessionContext,this.sessionState,this.dependencies,,,); // 源码详解如下 ExecutionContext.Builder.build();
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
					// 1. 解析用户查询语句生成 Calcite对象,并基于默认 curCatalog,curDB生成 FlinkCatalogReader;
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
					// 2. 提交执行sql Calcite命令; 
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

	// 定义环境配置和 执行变量的源码
	ExecutionContext$Builder.build(){
		Environment curEvn = this.currentEnv == null? Environment.merge(defaultEnv, sessionContext.getSessionEnv()): this.currentEnv
		return new ExecutionContext<>(curEvn,this.sessionContext,this.sessionState,this.dependencies,,,);{//ExecutionContext()构造函数, 生成一堆的执行环境;
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
										tableEnv.registerCatalog(name, catalog);{//TableEnvironmentImpl.registerCatalog
											catalogManager.registerCatalog(catalogName, catalog);{//CatalogManager.registerCatalog
												catalog.open();{// 不同的catalog,不同的实现;
													HiveCatalog.open();
												}
												catalogs.put(catalogName, catalog);
											}
										}
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
			
			final CommandLine commandLine = createCommandLine(environment.getDeployment(), commandLineOptions);
			
			flinkConfig.addAll(createExecutionConfig(commandLine, commandLineOptions, availableCommandLines, dependencies));{//createExecutionConfig()
				final CustomCommandLine activeCommandLine =findActiveCommandLine(availableCommandLines, commandLine);
				Configuration executionConfig = activeCommandLine.toConfiguration(commandLine);{
					// 不同环境, Cli的实现不一样
					FlinkYarnSessionCli.toConfiguration(commandLine){
						final Configuration effectiveConfiguration = new Configuration();
						if (applicationId != null) {
							effectiveConfiguration.setString(HA_CLUSTER_ID, zooKeeperNamespace);
							effectiveConfiguration.setString(YarnConfigOptions.APPLICATION_ID, ConverterUtils.toString(applicationId));
							effectiveConfiguration.setString(DeploymentOptions.TARGET, YarnSessionClusterExecutor.NAME);
						}
						if (commandLine.hasOption(slots.getOpt())) {// -s or --slots
							effectiveConfiguration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS,Integer.parseInt(commandLine.getOptionValue(slots.getOpt())));
						}
						if (isYarnPropertiesFileMode(commandLine)) {
							return applyYarnProperties(effectiveConfiguration);
						} else {
							return effectiveConfiguration;
						}
					}
					
				}
				LOG.info("Executor config: {}", executionConfig); // 打印的日志就是这个;
				return executionConfig;
			}
			
			clusterClientFactory = serviceLoader.getClusterClientFactory(flinkConfig);
			clusterSpec = clusterClientFactory.getClusterSpecification(flinkConfig);
			
		}
	}



// 3.2: CliClient.parseCommand() 解析lineStr成 SqlNode->Operation,并validate()校验; 

SqlCommandParser.parse(Parser sqlParser, String stmt){
	Optional<SqlCommandCall> callOpt = parseByRegexMatching(stmt);
	if (callOpt.isPresent()) {
		return callOpt.get();
	}else{ // 没有正则, 进入这里; 
		return parseBySqlParser(sqlParser, stmt);{//SqlCommandParser.parseBySqlParser
			operations = sqlParser.parse(stmt);{//LocalExecutor.Parser匿名类.parse()
				return context.wrapClassLoader(() -> parser.parse(statement));{// ParserImpl.parse()
					CalciteParser parser = calciteParserSupplier.get();
					FlinkPlannerImpl planner = validatorSupplier.get();
					// 1. 解析成 SqlNode: 依据谁? 主要靠 calcite框架
					SqlNode parsed = parser.parse(statement);
					
					// 2. 基于SqlNode生产Flink Operator算子? 
					Optional<Operation> operationOp = SqlToOperationConverter.convert(planner, catalogManager, parsed);{
						SqlNode validated = flinkPlanner.validate(sqlNode);{// FlinkPlannerImpl.validate()
							// 把 CatalogManager (基于sql-client.yaml构建)中 curCatalog,curDatabase 作为 validator校验器; 
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
							// 执行 sql验证; 
							validate(sqlNode, validator);{}
						}
						
						SqlToOperationConverter converter =new SqlToOperationConverter(flinkPlanner, catalogManager);
						
						if (validated instanceof SqlCreateCatalog) {
							return Optional.of(converter.convertCreateCatalog((SqlCreateCatalog) validated));
						} else if (validated instanceof RichSqlInsert) {
							// Insert的 转换入口; 
							return Optional.of(converter.convertSqlInsert((RichSqlInsert) validated));{
								// 构建 catalog.db.tableName 的 Id;
								ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
								
								PlannerQueryOperation query = SqlToOperationConverter.convert(flinkPlanner, insert.getSource()).orElseThrow();{
									// 又进入 SqlKind.QUERY 的query查询处理; 代码同下 converter.convertSqlQuery
									converter.convertSqlQuery(validated); 
								}
								return new CatalogSinkModifyOperation();
							}
						}else if (validated.getKind().belongsTo(SqlKind.QUERY)) {
							// 查询的 转换入口; 
							return Optional.of(converter.convertSqlQuery(validated));{//SqlToOperationConverter.convertSqlQuery
								return toQueryOperation(flinkPlanner, node);{
									RelRoot relational = planner.rel(validated);{// FlinkPlannerImpl.rel()
										val sqlToRelConverter: SqlToRelConverter = createSqlToRelConverter(sqlValidator);
										sqlToRelConverter.convertQuery(validatedSqlNode, false, true);{
											// calcite 解析器的一堆转换； 
											calcite.sql2rel.SqlToRelConverter.convertQuery()；
											// 一堆的 calcite convert 转换
											
											CatalogSourceTable.toRel(){
												final CatalogTable catalogTable = createFinalCatalogTable(context, hintedOptions);
												// 1. create and prepare table source
												DynamicTableSource tableSource = createDynamicTableSource(context, catalogTable);{
													return FactoryUtil.createTableSource();// table-common源码， 代码细节 如下； 
												}
												
												// 2. push table scan
												pushTableScan(relBuilder, cluster, catalogTable, tableSource, typeFactory, hints);
												
												// 4. push watermark assigner
												if (schemaTable.isStreamingMode() && !schema.getWatermarkSpecs().isEmpty()) {
													pushWatermarkAssigner(context, relBuilder, schema);
												}
												return relBuilder.build();
											}
										}
									}
									return new PlannerQueryOperation(relational.project());
								}
							}
						}
					}
					
					Operation operation =operationOp.orElseThrow(() -> new TableException("Unsupported query: " + statement));
					return Collections.singletonList(operation);
				}
			}
			return new SqlCommandCall(cmd, operands);
		}
	}
}






// 3.3 calCommand() : 执行sql命令/提交 Operator作业

CliClient.callCommand(){
	switch (cmdCall.command) {
		case SET:
			callSet(cmdCall);
			break;
		case SELECT:
			callSelect(cmdCall);{//CliClient.callSelect()
				// executeQuery()/executeInsert()/ executeDelete() 都是相似的流程: 
				// Executor有3个子类: LocalExecutor, TestingExeutor, MockExecutor; 
				executor.executeXXX(){// LocalExecutor.executeXXX()
					// 1. 获取或生产 执行上下文: ExecutionContext 
					final ExecutionContext<?> context = getExecutionContext(sessionId);
					return executeQueryInternal(sessionId, context, query);{//LocalExecutor.
						// 2. 准备相关的Table Api 
						{
							// query/ select 相关命令: 
							table = createTable(context, context.getTableEnvironment(), query);
							
							// insert / upsert相关运算
							applyUpdate(context, statement);
							//todo... 
						}
						
						// 3. 创建一条Pipeline; 
						pipeline = context.createPipeline(jobName);
						// 4. 创建 Deployer并提交发布 deployer.deploy(); 
						final ProgramDeployer deployer =new ProgramDeployer(configuration, jobName, pipeline, context.getClassLoader());
						deployer.deploy().get();
						return new ResultDescriptor();
					}
				}
			}
			break;
		case INSERT_INTO:
		case INSERT_OVERWRITE:
			callInsert(cmdCall);{CliClient.callInsert()
				// executeQuery()/executeInsert()/ executeDelete() 都是相似的流程: 
				// Executor有3个子类: LocalExecutor, TestingExeutor, MockExecutor; 
				executor.executeXXX(){// LocalExecutor.executeXXX()
					// 1. 获取或生产 执行上下文: ExecutionContext 
					final ExecutionContext<?> context = getExecutionContext(sessionId);
					return executeQueryInternal(sessionId, context, query);{//LocalExecutor.
						// 2. 准备相关的Table Api 
						{
							// query/ select 相关命令: 
							table = createTable(context, context.getTableEnvironment(), query);
							
							// insert / upsert相关运算
							applyUpdate(context, statement);
							//todo... 
						}
						
						// 3. 创建一条Pipeline; 
						pipeline = context.createPipeline(jobName);
						// 4. 创建 Deployer并提交发布 deployer.deploy(); 
						final ProgramDeployer deployer =new ProgramDeployer(configuration, jobName, pipeline, context.getClassLoader());
						deployer.deploy().get();
						return new ResultDescriptor();
					}
				}
			}
			break;
		case CREATE_TABLE:
			callDdl(cmdCall.operands[0], CliStrings.MESSAGE_TABLE_CREATED);
			break;
	}
}

	/** LocalExecutor.executeQuery()/ executeInsert() 等有者相似的执行流程: 
	*	1. 获取执行上下文:  getExecutionContext();
	*	2. 准备相关的Table Api : createTable(), applyUpdate(), 
	*	3. 创建一条Pipeline	:  context.createPipeline(jobName);
	*	4. 执行一次提交发布:  deployer.deploy(); 
	*/

	// 3.3.1 执行 select 类型查询
	CliClient.callSelect(cmdCall);{
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
	
	

	// 3.3.2 执行 insert 类型的sql语句; 
	CliClient.callInsert(){
		ProgramTargetDescriptor programTarget=executor.executeUpdate(sessionId, cmdCall.operands[0]);{//LocalExecutor.executeUpdate
			final ExecutionContext<?> context = getExecutionContext(sessionId);
			return executeUpdateInternal(sessionId, context, statement);{//LocalExecutor.executeUpdateInternal
				applyUpdate(context, statement);
				Pipeline pipeline = context.createPipeline(jobName);
				ProgramDeployer deployer =new ProgramDeployer(configuration, jobName, pipeline, context.getClassLoader());
				// 发布提交 
				context.wrapClassLoader(()->{JobClient jobClient = deployer.deploy().get();})
			}
		}
		terminal.writer().println(programTarget.toString());//打印sql结果?
		terminal.flush();
	}


	// 3.3.3 创建一条Pipeline	:  context.createPipeline(jobName);

	ExecutionContext.createPipeline(jobName);{
		if (streamExecEnv != null) {// 优先看是不是 Stream执行模式; 
			StreamTableEnvironmentImpl streamTableEnv =(StreamTableEnvironmentImpl) tableEnv;
			return streamTableEnv.getPipeline(name);{//StreamTableEnvironmentImpl.getPipeline()
				List<Transformation<?>> transformations = translateAndClearBuffer();{//TableEnvironmentImpl.
					transformations = translate(bufferedModifyOperations);{
						return planner.translate(modifyOperations);{// 把 Sql / Table 转换成 runtime stream Operator算子; 
							// Planner接口有2个实现类: StreamPlanner, BatchPlanner, 都继承子中间的 公共抽象类: PlannerBase
							
							StreamPlanner.translate(){// PlannerBase.translate() 调父类公共 转换方法; 
								// 核心: 创建一颗 逻辑tree; 
								val relNodes = modifyOperations.map(translateToRel);{// PlannerBase.translateToRel
									overrideEnvParallelism();
									modifyOperation match {
										case s: UnregisteredSinkModifyOperation[_] =>{}
										
										case s: SelectSinkOperation =>{}
										
										// 纯insert select  语句进入这里?
										case catalogSink: CatalogSinkModifyOperation =>{
											val input = getRelBuilder.queryOperation(modifyOperation.getChild).build();
											val sinkOption: Option[(CatalogTable, Any)] = getTableSink(identifier, dynamicOptions);{
												val lookupResult = catalogManager.getTable(objectIdentifier);
												lookupResult.map(_.getTable) match {
													case Some(table: ConnectorCatalogTable[_, _]) =>{}
													// 什么情况下, 是 CatalogTable? 
													case Some(table: CatalogTable) => {
														val catalog = catalogManager.getCatalog(objectIdentifier.getCatalogName)
														val tableToFind = table
														val isTemporary = lookupResult.get.isTemporary
														// Checks whether the [[CatalogTable]] uses legacy connector sink options
														val isGoodConnector = isLegacyConnectorOptions(objectIdentifier, table, isTemporary);{
															val properties = new DescriptorProperties(true);
															// 建表时如果指定 connector.type属性, 直接返回true 表示合法/Legacy的 Connector; 
															properties.putProperties(catalogTable.getOptions);
															if (properties.containsKey(ConnectorDescriptorValidator.CONNECTOR_TYPE)) { // 看该表 connector.type 属性是否存在; 
																true
															}else{
																val catalog = catalogManager.getCatalog(objectIdentifier.getCatalogName);
																try{
																	TableFactoryUtil.findAndCreateTableSink();
																	true
																}catch {
																	false
																}
															}
														}
														if ( isGoodConnector) {
															val tableSink = TableFactoryUtil.findAndCreateTableSink()
														}else{// 上临时表(dwd_orders_cate) 即无 connector-type,也没有 TableFactory 实现类,就属于非法, 进入这里;
															val tableSink = FactoryUtil.createTableSink();{
																
															}
														}
													}
												}
											}
											
										}
										
										case (table, sink: DynamicTableSink) =>{DynamicSinkUtils.toRel(getRelBuilder, input, catalogSink, sink, table);}
									}
								}
								// 核心: 执行优化;
								val optimizedRelNodes = optimize(relNodes)
								val execNodes = translateToExecNodePlan(optimizedRelNodes)
								// 核心: 转化成 执行计划; 
								translateToPlan(execNodes);{// StreamPlanner.translateToPlan()
									planner.overrideEnvParallelism();
									execNodes.map {case node: StreamExecNode[_] => node.translateToPlan(planner){ //ExecNode.translateToPlan
										if (transformation == null) {// 成员变量, 每个ExecNode只绑定1个 Transformation
											transformation = translateToPlanInternal(planner);{
												
												StreamExecLegacySink.translateToPlanInternal(){
													val resultTransformation = sink match {
														// streaming 模式的 算子;
														case streamTableSink: StreamTableSink[T] =>{
															val transformation = streamTableSink match {
																case _: RetractStreamTableSink[T] => translateToTransformation(withChangeFlag = true, planner);
																
																case upsertSink: UpsertStreamTableSink[T] =>{
																	val isAppendOnlyTable = ChangelogPlanUtils.inputInsertOnly(this)
																	upsertSink.setIsAppendOnly(isAppendOnlyTable);
																	UpdatingPlanChecker.getUniqueKeyForUpsertSink(this, planner, upsertSink){
																		val sinkFieldNames = sink.getTableSchema.getFieldNames;
																		 val fmq: FlinkRelMetadataQuery = FlinkRelMetadataQuery.reuseOrCreate(planner.getRelBuilder.getCluster.getMetadataQuery);
																		 // 查询主键, 没有主键; 
																		 val uniqueKeys = fmq.getUniqueKeys(sinkNode.getInput);
																		 if (uniqueKeys != null && uniqueKeys.size() > 0) {
																			 uniqueKeys
																				  .filter(_.nonEmpty).map(_.toArray.map(sinkFieldNames))
																				  .toSeq.sortBy(_.length).headOption
																		 }else {Nonde };
																	} match { 
																		case Some(keys) => upsertSink.setKeyFields(keys);
																		case None if isAppendOnlyTable => upsertSink.setKeyFields(null);
																		// 该异常是这里抛出, upsertSink必需要有主键; 
																		case None if !isAppendOnlyTable => throw new TableException("UpsertStreamTableSink requires that Table has a full primary keys if it is updated.")
																	}
																	translateToTransformation(withChangeFlag = true, planner);
																}
																
																case _: AppendStreamTableSink[T] =>{
																	
																}
															}
															val dataStream = new DataStream(planner.getExecEnv, transformation)
															val dsSink = streamTableSink.consumeDataStream(dataStream)
															dsSink.getTransformation
														}
														// batch 模式的算子? 
														case dsTableSink: DataStreamTableSink[_] =>{}
													}
													
													resultTransformation.asInstanceOf[Transformation[Any]]
												}
												
											}
										}
										transformation
									}}
								}
							}
							
							BatchPlanner.translate()
							
							
						}
					}
					bufferedModifyOperations.clear();
				}
				return execEnv.createPipeline(transformations, tableConfig, jobName);{// Executor.createPipeline()
					// execution.type=batch 模式setBatchProperties(), 会把 checkpoing置空;  
					BatchExecutor.createPipeline(){
						StreamExecutionEnvironment execEnv = getExecutionEnvironment();
						ExecutorUtils.setBatchProperties(execEnv, tableConfig);
						StreamGraph streamGraph = ExecutorUtils.generateStreamGraph(execEnv, transformations);
						
						ExecutorUtils.setBatchProperties(streamGraph, tableConfig);{
							streamGraph.getStreamNodes().forEach(sn -> sn.setResources(ResourceSpec.UNKNOWN, ResourceSpec.UNKNOWN));
							streamGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST);
							streamGraph.setStateBackend(null);
							if (streamGraph.getCheckpointConfig().isCheckpointingEnabled(){
								return checkpointInterval > 0;// execution.checkpointing.interval 是否>0 判断是否开启ckp; 
							}) {
								throw new IllegalArgumentException("Checkpoint is not supported for batch jobs.");
							}
							streamGraph.setGlobalDataExchangeMode(getGlobalDataExchangeMode(tableConfig));
						}
					}
					
					StreamExecutor.createPipeline(){
						StreamGraph streamGraph =ExecutorUtils.generateStreamGraph(getExecutionEnvironment(), transformations);
						return streamGraph;
					}
				}
			}
		}else{
			BatchTableEnvironmentImpl batchTableEnv=(BatchTableEnvironmentImpl) tableEnv;
			return batchTableEnv.getPipeline(name);
		}
	}


	// 3.3.4  set语法
	CliClient.callSet(){
		if (cmdCall.operands.length == 0) {
			Map<String, String> properties = executor.getSessionProperties(sessionId);
			terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
		}else{
			executor.setSessionProperty(sessionId, cmdCall.operands[0], cmdCall.operands[1].trim());{
				Environment newEnv = Environment.enrich(env, Collections.singletonMap(key, value));
				ExecutionContext<?> newContext =createExecutionContextBuilder(context.getOriginalSessionContext())
                        .env(newEnv)
                        .sessionState(context.getSessionState())
                        .build();// 源码参加上面的 ExecutionContext.Builder.build();
				//  LocalExecutor.contextMap: Map<String, ExecutionContext<?>> 存储了相应的环境变量; 
				this.contextMap.put(sessionId, newContext);
			}
		}
	}
	
createExecutionConfig:363, ExecutionContext (org.apache.flink.table.client.gateway.local)
<init>:194, ExecutionContext (org.apache.flink.table.client.gateway.local)
<init>:138, ExecutionContext (org.apache.flink.table.client.gateway.local)
build:961, ExecutionContext$Builder (org.apache.flink.table.client.gateway.local)
setSessionProperty:306, LocalExecutor (org.apache.flink.table.client.gateway.local)
callSet:433, CliClient (org.apache.flink.table.client.cli)



	
	// 基于 connector-type查相应 TableFactory并利用表工厂创建(Sink/Source)等算子? 
	TableFactoryUtil.findAndCreateTableSink(catalog,identifier,catalogTable,isStreamingMode,isTemporary);{
		TableSinkFactory.Context context =new TableSinkFactoryContextImpl();
		if (catalog == null) {
			return findAndCreateTableSink(context);
		} else {
			Optional<TableSink> sinkOp = createTableSinkForCatalogTable(catalog, context);{
				// GenericInMemoryCatalog
				TableFactory tableFactory = catalog.getTableFactory().orElse(null);{
					// default_catalog实现类 GenericInMemoryCatalog, 其提供的 TableFactory为空; 
					GenericInMemoryCatalog.getTableFactory(){
						return Optional.empty();
					}
					// Hive的Catalog
					HiveCatalog.getTableFactory();
					
					// JDBC
					JDBCCatalog.getTableFactory();
					
				}
				if (tableFactory instanceof TableSinkFactory) {
					return ((TableSinkFactory) tableFactory).createTableSink(context);{
						
					}
				}
				return Optional.empty();
			}
			return sinkOp.orElseGet(() -> findAndCreateTableSink(context));
		}
	}

	// 基于 catalog中"connector"字段查 TableFactory,并创建 Sink等; 
	TableUtil.createTableSink(){
		// 返回默认的实现类: DefaultDynamicTableContext
		DefaultDynamicTableContext context = new DefaultDynamicTableContext();
		DynamicTableSinkFactory factory = getDynamicTableFactory(DynamicTableSinkFactory.class, catalog, context);{
			if (catalog != null) {
				Factory factory =catalog.getFactory()
								.filter(f -> factoryClass.isAssignableFrom(f.getClass()))
								.orElse(null);
				if (factory != null) { return (T) factory;}
			}
			// 看 DynamicTableFactory.Context.CatalogTable.Properties 中, 获取"connector" 字段值: 如 hive, mysql-cdc,jdbc,kafka等; ; 
			final String connectorOption = context.getCatalogTable().getOptions().get(CONNECTOR.key());
			if (connectorOption == null) { 
				// 就是这个场景的错误; 
				throw new ValidationException( "Table options do not contain an option key '%s' for discovering a connector.");
			}
			return discoverFactory(context.getClassLoader(), factoryClass, connectorOption);
		}
		return factory.createDynamicTableSink(context);
	}

	FactoryUtil。createTableSource(){
		DefaultDynamicTableContext context = new DefaultDynamicTableContext();
		DynamicTableSourceFactory factory = getDynamicTableFactory(DynamicTableSourceFactory.class, catalog, context);{
			final String connectorOption = context.getCatalogTable().getOptions().get(CONNECTOR.key());
			if (connectorOption == null) { 
				// 就是这个场景的错误; 
				throw new ValidationException( "Table options do not contain an option key '%s' for discovering a connector.");
			}
			return discoverFactory(context.getClassLoader(), factoryClass, connectorOption);
		}
		return factory.createDynamicTableSource(context);
	}


// HiveCatalog.open()
HiveCatalog.open(){
	client = HiveMetastoreClientFactory.create(hiveConf, hiveVersion);{return new HiveMetastoreClientWrapper(hiveConf, hiveVersion);{
		this.hiveConf = Preconditions.checkNotNull(hiveConf, "HiveConf cannot be null");
		
		hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
		
		// 因为hive.metastore.uris非空,所以进入后面的 newSynchronizedClient(createMetastoreClient());
		boolean isEmbeddedMeta = HiveCatalog.isEmbeddedMetastore(hiveConf);{
			// 如果 hive.metastore.uris == null,empty,空白,返回true,表示 不使用外部Metastore Service,使用 嵌入式 Embedded Metastore;
			return isNullOrWhitespaceOnly(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));{ // hive.metastore.uris
				if (str == null || str.length() == 0) {
					return true;
				}
				return true;
			}
		}
		client = isEmbeddedMeta? createMetastoreClient(): HiveMetaStoreClient.newSynchronizedClient(createMetastoreClient());{
			HiveMetastoreClientWrapper.createMetastoreClient(){
				return hiveShim.getHiveMetastoreClient(hiveConf);{
					// 反射执行 RetryingMetaStoreClient.getProxy()
					return (IMetaStoreClient) method.invoke(null, (hiveConf));{// RetryingMetaStoreClient.getProxy()
						// org.apache.hadoop.hive.metastore.HiveMetaStoreClient
						Class<? extends IMetaStoreClient> baseClass = (Class<? extends IMetaStoreClient>) MetaStoreUtils.getClass(mscClassName);
						RetryingMetaStoreClient handler =new RetryingMetaStoreClient(hiveConf, constructorArgTypes, constructorArgs, metaCallTimeMap, baseClass);{
							String msUri = hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS);//读取metastore.uris值: thrift://bdnode102:9083
							this.base = (IMetaStoreClient) MetaStoreUtils.newInstance(msClientClass, constructorArgTypes, constructorArgs);{
								// 反射构建 HiveMetaStoreClient对象
								new HiveMetaStoreClient(){
									localMetaStore = HiveConfUtil.isEmbeddedMetaStore(msUri);
									retries = HiveConf.getIntVar(conf, HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES);
									metastoreUris = new URI[metastoreUrisString.length];
									for (String s : metastoreUrisString) {
										metastoreUris[i++] = tmpUri;
									}
									open();{//HiveMetaStoreClient.open
										for (URI store : metastoreUris) {
											transport = new TSocket(store.getHost(), store.getPort(), clientSocketTimeout);
											client = new ThriftHiveMetastore.Client(protocol);
											transport.open();
											UserGroupInformation ugi = Utils.getUGI();
											String[] groups = ugi.getGroupNames();{//Groups.getGroupNames() -> ShellBasedUnixGroupsMapping.getUnixGroups(user)
												String[] cmd= Shell.getGroupsForUserCommand(user);{//org.apache.hadoop.util.Shell getGroupsForUserCommand()
													//因为win没有 WINUTILS 命令, 导致这里=null; 
													return (WINDOWS)? new String[] { WINUTILS, "groups", "-F", "\"" + user + "\""}: new String [] {"bash", "-c", "id -Gn " + user};
												}
												result = Shell.execCommand(cmd);
											}
											client.set_ugi(ugi.getUserName(), Arrays.asList(groups));
										}
									}
								}
								
							}
						}
						return (IMetaStoreClient) Proxy.newProxyInstance(RetryingMetaStoreClient.class.getClassLoader(), baseClass.getInterfaces(), handler);
					}
				}
			}
		}
		
	}}
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


flink.table.api.internal.TableImpl.executeInsert(String tablePath, boolean overwrite){
	UnresolvedIdentifier unresolvedIdentifier =tableEnvironment.getParser().parseIdentifier(tablePath);
	ObjectIdentifier objectIdentifier =tableEnvironment.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
	ModifyOperation operation =new CatalogSinkModifyOperation();
	return tableEnvironment.executeInternal(Collections.singletonList(operation));{//TableEnvironmentImpl.executeInternal
		List<Transformation<?>> transformations = translate(operations);
		Pipeline pipeline = execEnv.createPipeline(transformations, tableConfig, jobName);
		JobClient jobClient = execEnv.executeAsync(pipeline);{//ExecutorBase.executeAsync()
			return executionEnvironment.executeAsync((StreamGraph) pipeline);{//StreamExecutionEnvironment.executeAsync()
				// 详细源码参考如下: 
				PipelineExecutorFactory executorFactory =executorServiceLoader.getExecutorFactory(configuration);
				jobClientFuture =executorFactory
					.getExecutor(configuration)
                    .execute(streamGraph, configuration, userClassloader);
				return jobClient;
			}
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
							if (e.depth == 1) { 
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



apache.calcite.rel.metadata.RelMetadataQuery.getUniqueKeys(RelNode rel){
	return getUniqueKeys(rel, false);{
		try {
			return uniqueKeysHandler.getUniqueKeys(rel, this, ignoreNulls);{
				{ // 构造函数 定义 uniqueKeysHandler
					this.uniqueKeysHandler = initialHandler(BuiltInMetadata.UniqueKeys.Handler.class);{
						return handlerClass.cast(Proxy.newProxyInstance(RelMetadataQuery.class.getClassLoader(),
								new Class[] {handlerClass}, (proxy, method, args) -> {
								  // r的示例: StreamExecCalc
								  final RelNode r = (RelNode) args[0];
								  throw new JaninoRelMetadataProvider.NoHandler(r.getClass());
								}));
					}
				}
			}
		} catch (JaninoRelMetadataProvider.NoHandler e) {
			uniqueKeysHandler =revise(e.relClass, BuiltInMetadata.UniqueKeys.DEF);
		}
	}
}

// 不同 sql操作的实现
ExecNode.translateToPlanInternal(){
	// select * from 操作;
	StreamExecTableSourceScan.translateToPlanInternal(){
		createSourceTransformation(planner.getExecEnv, getRelDetailedDescription);{//CommonPhysicalTableSourceScan
			val runtimeProvider = tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE)
			runtimeProvider match {
				case provider: SourceFunctionProvider =>{}
				
				case provider: InputFormatProvider => {}
				
				case provider: DataStreamScanProvider => {
					provider
						.produceDataStream(env){// provider: DataStreamScanProvider 多个实现类
							// hive table的实现类: HiveTableSource
							HiveTableSource.produceDataStream(){
								return getDataStream(execEnv); // 详解下面 HiveTableSource.produceDataStream() 源码;
							}
						}
						.getTransformation(){};
				}
			}
		}
	}
	
}


	// Hive Source Table的 环境初始和创建源码 
	HiveTableSource.produceDataStream(){
		return getDataStream(execEnv);{// HiveTableSource.getDataStream()
			validateScanConfigurations();
			// 获取所有 hive 的分区? 这里又92个 
			List<HiveTablePartition> allHivePartitions =getAllPartitions(jobConf, hiveVersion, remainingPartitions);
			HiveSource.HiveSourceBuilder sourceBuilder =new HiveSource.HiveSourceBuilder(allHivePartitions,limit,hiveVersion,
				flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER),// table.exec.hive.fallback-mapred-reader
				(RowType) getProducedDataType().getLogicalType());
			DataStreamSource<RowData> source =
			execEnv.fromSource(hiveSource, WatermarkStrategy.noWatermarks(), "HiveSource-" + tablePath.getFullName());
			boolean isStreamSource = isStreamingSource(){ // streaming-source.enable=true 表示流表;
				return Boolean.parseBoolean(catalogTable.getOptions().getOrDefault(
								STREAMING_SOURCE_ENABLE.key(),// streaming-source.enable 建表时指定,默认false
								STREAMING_SOURCE_ENABLE.defaultValue().toString())); //false 
			}
			if (isStreamSource) {
				return source;
			}else{
				HiveParallelismInference hiveInfer = new HiveParallelismInference(tablePath, flinkConf){ // inferred 推论,是否启用 splits分片数推断;
					this.infer = flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM);//table.exec.hive.infer-source-parallelism
					this.inferMaxParallelism = flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX);// table.exec.hive.infer-source-parallelism.max
					this.parallelism =flinkConf.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM); //table.exec.resource.default-parallelism
				};
				hiveInfer.infer(
					() ->HiveSourceFileEnumerator.getNumFiles(allHivePartitions, jobConf),
					() -> HiveSourceFileEnumerator.createInputSplits(0, allHivePartitions, jobConf).size());{//HiveParallelismInference.infer()
						if (!infer) {//(默认不进) 即上面的 hive.infer-source-parallelism=false时(默认true开启); 不infer推断
							return this;
						}
						int lowerBound = logRunningTime("getNumFiles", numFiles);// numFiles等于 hive的分片/文件数量;
						if (lowerBound >= inferMaxParallelism) {
							parallelism = inferMaxParallelism;// 并发度最大 不超过 table.exec.hive.infer-source-parallelism.max(默认1000)
							return this;
						}
						int splitNum = logRunningTime("createInputSplits", numSplits); // 733?
						parallelism = Math.min(splitNum, inferMaxParallelism);
				}
				hiveInfer.limit(limit);{//HiveParallelismInference.limit()
					if (limit != null) {// select * limit xxx 中的 limit 修饰数量;
						parallelism = Math.min(parallelism, (int) (limit / 1000));
					}
					return Math.max(1, parallelism);// 一般等于上面并发数 parallelism=> splitNum 
				}
				return source.setParallelism(parallelism);
			}
		}
	}


