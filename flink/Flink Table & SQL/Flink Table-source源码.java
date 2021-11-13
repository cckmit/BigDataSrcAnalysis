

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





































