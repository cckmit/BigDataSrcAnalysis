


// statement.executeQuery("select * from tb")

<init>:48, RowDataStatic (com.mysql.jdbc)
readSingleRowSet:3423, MysqlIO (com.mysql.jdbc)
getResultSet:471, MysqlIO (com.mysql.jdbc)
readResultsForQueryOrUpdate:3115, MysqlIO (com.mysql.jdbc)
readAllResults:2344, MysqlIO (com.mysql.jdbc)
sqlQueryDirect:2739, MysqlIO (com.mysql.jdbc)
execSQL:2491, ConnectionImpl (com.mysql.jdbc)
execSQL:2449, ConnectionImpl (com.mysql.jdbc)
executeQuery:1381, StatementImpl (com.mysql.jdbc)
testSelectApi_mysql:25, TestMySQLJdbcApi (com.bigdata.streaming.iotdb.api)


// mysql jdbc中: ConnectionImpl.execSQL() 执行一个sql 

ConnectionImpl.execSQL(){
	this.lastQueryFinishedTime = 0; 
	if (getHighAvailability() && (this.autoCommit || getAutoReconnectForPools()) && this.needsPing && !isBatch) {
		pingInternal(false, 0);
		this.needsPing = false;
	}
	
	if (packet == null) {
		return this.io.sqlQueryDirect(callingStatement, sql, encoding, null, maxRows, streamResults, catalog,cachedMetadata);{//MysqlIO.sqlQueryDirect()
			
			Buffer resultPacket = sendCommand(MysqlDefs.QUERY, null, queryPacket, false, null, 0);{//MysqlIO.sendCommand()
				send();
			}
			
			ResultSetInternalMethods rs = readAllResults(callingStatement, maxRows, streamResults, catalog, cachedMetadata);{//MysqlIO.readAllResults()
				resultPacket.setPosition(resultPacket.getPosition() - 1);
				ResultSetImpl topLevelResultSet = readResultsForQueryOrUpdate(callingStatement, maxRows);{//MysqlIO.readResultsForQueryOrUpdate()
					long columnCount = resultPacket.readFieldLength();//返回查询多少列,如4列;
					if (columnCount == 0) {
						return buildResultSetWithUpdates(callingStatement, resultPacket);
					}else if (columnCount == Buffer.NULL_LENGTH) {// == -1? 所有列? 还是?
						return sendFileToServer(callingStatement, fileName);
					}else {// query 正常进入这里; 
						ResultSetImpl results = getResultSet(callingStatement, columnCount, maxRows, streamResults, catalog, isBinaryEncoded, metadataFromCache);{
							
							if (!isEOFDeprecated() ||(this.connection.versionMeetsMinimum(5, 0, 2) && callingStatement != null && isBinaryEncoded && callingStatement.isCursorRequired())) {
								packet = reuseAndReadPacket(this.reusablePacket);
								readServerStatusForResultSets(packet);
							}
							
							if (!streamResults) {// 大部分进入这里;
								rowData = readSingleRowSet(columnCount, maxRows, (metadataFromCache == null) ? fields : metadataFromCache);{ //MysqlIO.readSingleRowSet()
									ArrayList<ResultSetRow> rows = new ArrayList<ResultSetRow>();
									boolean useBufferRowExplicit = useBufferRowExplicit(fields);
									
									ResultSetRow row = nextRow(fields, (int) columnCount, isBinaryEncoded, resultSetConcurrency, false, useBufferRowExplicit, false, null);{
										// 如果有数据,默认进入这里;
										if (this.useDirectRowUnpack && existingRowPacket == null && !isBinaryEncoded && !useBufferRowIfPossible && !useBufferRowExplicit) {
											return nextRowFast(fields, columnCount);{//MysqlIO.nextRowFast()
												int lengthRead = readFully(this.mysqlInput, this.packetHeaderBuf, 0, 4);
												int packetLength = (this.packetHeaderBuf[0] & 0xff) + ((this.packetHeaderBuf[1] & 0xff) << 8) + ((this.packetHeaderBuf[2] & 0xff) << 16);
												
												byte[][] rowData = null;
												for (int i = 0; i < columnCount; i++) {
													int sw = this.mysqlInput.read() & 0xff;
													if (firstTime) {
														rowData = new byte[columnCount][];
														firstTime = false;
													}
													
													if (len == NULL_LENGTH) {
														rowData[i] = null;
													}else if (len == 0) {
														rowData[i] = Constants.EMPTY_BYTE_ARRAY;
													}else{
														rowData[i] = new byte[len];
														int bytesRead = readFully(this.mysqlInput, rowData[i], 0, len);{//MysqlIO.readFully()
															int n = 0;
															while (n < len) {
																int count = in.read(b, off + n, len - n);
																n += count;
															}
															return n;
														}
														remaining -= bytesRead;
													}
													
												}
												
												if (remaining > 0) {
													skipFully(this.mysqlInput, remaining);
												}
												return new ByteArrayRow(rowData, getExceptionInterceptor());
											}
										}
										
										rowPacket.setPosition(rowPacket.getPosition() - 1);
										readServerStatusForResultSets(rowPacket);
										return null;
										
									}
									
									if (row != null) {
										rows.add(row);
										rowCount = 1;
									}
									
									while (row != null) {
										row = nextRow(fields, (int) columnCount, isBinaryEncoded, resultSetConcurrency, false, useBufferRowExplicit, false, null);
									}
									
									rowData = new RowDataStatic(rows);
									return rowData;
								}
								
							}else { //只有 FORWARD_ONLY && READ_ONLY && fetchSize == Int.Min 时,才 streamResults=true 进入这里; 
								rowData = new RowDataDynamic(this, (int) columnCount, (metadataFromCache == null) ? fields : metadataFromCache, isBinaryEncoded);
								this.streamingData = rowData;
							}
							ResultSetImpl rs = buildResultSetWithRows(callingStatement, catalog, (metadataFromCache == null) ? fields : metadataFromCache, rowData, resultSetType, resultSetConcurrency, isBinaryEncoded);
							return rs;
						}
						return results;
					}
				}
				return topLevelResultSet;
			}
			
		}
	}
	return this.io.sqlQueryDirect(callingStatement, null, null, packet, maxRows, resultSetType, resultSetConcurrency, streamResults, catalog,cachedMetadata);
}


// mysql jdbc的 select 查询

StatementImpl.executeQuery(String sql){
	checkNullOrEmptyQuery(sql);
	resetCancelledState();
	char firstStatementChar = StringUtils.firstAlphaCharUc(sql, findStartOfStatement(sql));
	
	statementBegins();
	boolean streamResults = createStreamingResultSet();{
		return ((this.resultSetType == java.sql.ResultSet.TYPE_FORWARD_ONLY) 		// FORWARD_ONLY type下
			&& (this.resultSetConcurrency == java.sql.ResultSet.CONCUR_READ_ONLY) 	// READ_ONLY 下
			&& (this.fetchSize == Integer.MIN_VALUE));								// fetchSize == Int.Min最小值时;
	}
	this.results = locallyScopedConn.execSQL(this, sql, this.maxRows, null, this.resultSetType, this.resultSetConcurrency, streamResults, this.currentCatalog, cachedFields);{
		return execSQL(callingStatement, sql, maxRows, packet, resultSetType, resultSetConcurrency, streamResults, catalog, cachedMetadata, false); // 源码如上 ConnectionImpl.execSQL()
	}
	this.lastInsertId = this.results.getUpdateID();
	if (cachedMetaData != null) {
		locallyScopedConn.initializeResultsMetadataFromCache(sql, cachedMetaData, this.results);
	}else{
		if (this.connection.getCacheResultSetMetadata()) {
			locallyScopedConn.initializeResultsMetadataFromCache(sql, null /* will be created */, this.results);
		}
	}
	return this.results;
}















