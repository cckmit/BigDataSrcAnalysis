
标签定义表: linkis_cg_manager_label
	id				自增主键,识别各Label标签的唯一ID;
	label_key		? 标签类型?	如: combined_userCreator_engineType, emInstance
	label_value		具体标签, 如*-IDE,spark-2.4.3, {"creator":"IDE","user":"bigdata","engineType":"spark","version":"2.4.3"}
	label_value_size




linkis_cg_manager_label_resource: 标签和资源容量 表
	id				自增主键?, 貌似和 resource_id 是一样的; 
	label_id		label标签的ID
	resource_id		资源情况 对应ID
	
	如spark的engine 相应的 资源ID关系是: 
	label_id = 26(combined_userCreator_engineType:{"creator":"LINKISCLI","user":"bigdata","engineType":"spark","version":"2.4.3"})
	resource_id = 2



linkis_cg_manager_linkis_resources: 各资源容量表
	id				资源ID, 在其他表中常名为: resource_id; 主键自增;
	max_resource	最大资源情况
	min_resource	最小资源情况
	used_resource	已使用资源情况
	left_resource	剩余容量; 
	
	如 spark engine相关的资源
	id = 2, 
	min_resource:  {"DriverAndYarnResource":{"loadInstanceResource":{"memory":0,"cores":0,"instances":0},"yarnResource":{"queueMemory":0,"queueCores":0,"queueInstances":0,"queueName":"default","applicationId":""}}}
	left_resource: {"DriverAndYarnResource":{"loadInstanceResource":{"memory":21474836480,"cores":10,"instances":10},"yarnResource":{"queueMemory":483183820800,"queueCores":150,"queueInstances":30,"queueName":"default","applicationId":""}}}






3.1 参数配置 定义表: linkis_ps_configuration_config_key
	id				自增主键, 每个Config的唯一ID
	key				参数名,如 spark.executor.instances
	description		参数解释
	name		
	default_value
	engine_conn_type 执行引擎的类型,如:hive,spark; 该自定只存在 config_key, engine_conn_plugin_bml_resources 中;往往对应 label表中 *-*,spark 标签Label
	level
	treeName		配置项所属 组名,如 [spark资源设置],[tidb设置]
	
	例如: id=9,key=spark.executor.instances
	key=spark.executor.instances
	description="取值范围：1-40，单位：个"
	name="spark执行器实例最大并发数"
	default_value=2
	engine_conn_type="spark"
	treeName="spark资源设置"
	
	

3.2 参数实际值的配置表: linkis_ps_configuration_config_value	各配置项的 value实际设置
	id				自增主键
	config_key_id	Config的唯一ID,对应 config_key中的id;
	config_value
	config_label_id
	
	例如: config_key_id(9,spark.executor.instances)
		config_key_id=9
		config_value=
		config_label_id=6 (对应label表中6:"*-*,spark-2.4.3"标签)
	
	
3.3 各配置参数 所属engine 的关系定义表: linkis_ps_configuration_key_engine_relation
	id
	config_key_id
	engine_type_label_id
	
	例如: config_key_id(9,spark.executor.instances)
		config_key_id=9
		engine_type_label_id=6	对应 label表中的 "*-*,spark-2.4.3"
	


spark变量定义
	linkis_ps_configuration_config_key		配置定义表	
	linkis_ps_configuration_config_value	实际值的配置表	
	linkis_ps_configuration_key_engine_relation	参数所属engine关系定义 
	linkis_ps_configuration_category


spark资源定义 linkis_configuration_dml.sql

SET @SPARK_LABEL="spark-2.4.3";
SET @SPARK_ALL=CONCAT('*-*,',@SPARK_LABEL);
SET @SPARK_IDE=CONCAT('*-IDE,',@SPARK_LABEL);
SET @SPARK_NODE=CONCAT('*-nodeexecution,',@SPARK_LABEL);
SET @SPARK_VISUALIS=CONCAT('*-Visualis,',@SPARK_LABEL);


-- 1. 插入 	linkis_cg_manager_label : *-*,spark-2.4.3
insert into `linkis_cg_manager_label` (`label_key`, `label_value`, `label_feature`, `label_value_size`, `update_time`, `create_time`) VALUES ('combined_userCreator_engineType',@FLINK_ALL, 'OPTIONAL', 2, now(), now());
insert into `linkis_cg_manager_label` (`label_key`, `label_value`, `label_feature`, `label_value_size`, `update_time`, `create_time`) VALUES ('combined_userCreator_engineType',@SPARK_IDE, 'OPTIONAL', 2, now(), now());
insert into `linkis_cg_manager_label` (`label_key`, `label_value`, `label_feature`, `label_value_size`, `update_time`, `create_time`) VALUES ('combined_userCreator_engineType',@SPARK_VISUALIS, 'OPTIONAL', 2, now(), now());
insert into `linkis_cg_manager_label` (`label_key`, `label_value`, `label_feature`, `label_value_size`, `update_time`, `create_time`) VALUES ('combined_userCreator_engineType',@SPARK_NODE, 'OPTIONAL', 2, now(), now());

-- 2. 插入  linkis_ps_configuration_config_key : spark.executor.instances, spark.executor.cores 等参数
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('spark.executor.instances', '取值范围：1-40，单位：个', 'spark执行器实例最大并发数', '2', 'NumInterval', '[1,40]', '0', '0', '2', 'spark资源设置', 'spark');

INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('wds.linkis.rm.instance', '范围：1-20，单位：个', 'spark引擎最大并发数', '3', 'NumInterval', '[1,20]', '0', '0', '1', '队列资源', 'spark');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('spark.executor.instances', '取值范围：1-40，单位：个', 'spark执行器实例最大并发数', '2', 'NumInterval', '[1,40]', '0', '0', '2', 'spark资源设置', 'spark');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('spark.executor.cores', '取值范围：1-8，单位：个', 'spark执行器核心个数',  '2', 'NumInterval', '[1,2]', '1', '0', '1','spark资源设置', 'spark');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('spark.executor.memory', '取值范围：3-15，单位：G', 'spark执行器内存大小', '3g', 'Regex', '^([3-9]|1[0-5])(G|g)$', '0', '0', '3', 'spark资源设置', 'spark');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('spark.driver.cores', '取值范围：只能取1，单位：个', 'spark驱动器核心个数', '1', 'NumInterval', '[1,1]', '1', '1', '1', 'spark资源设置','spark');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('spark.driver.memory', '取值范围：1-15，单位：G', 'spark驱动器内存大小','2g', 'Regex', '^([3-9]|1[0-5])(G|g)$', '0', '0', '1', 'spark资源设置', 'spark');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('spark.tispark.pd.addresses', NULL, NULL, 'pd0:2379', 'None', NULL, '0', '0', '1', 'tidb设置', 'spark');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('spark.tispark.tidb.addr', NULL, NULL, 'tidb', 'None', NULL, '0', '0', '1', 'tidb设置', 'spark');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('spark.tispark.tidb.password', NULL, NULL, NULL, 'None', NULL, '0', '0', '1', 'tidb设置', 'spark');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('spark.tispark.tidb.port', NULL, NULL, '4000', 'None', NULL, '0', '0', '1', 'tidb设置', 'spark');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('spark.tispark.tidb.user', NULL, NULL, 'root', 'None', NULL, '0', '0', '1', 'tidb设置', 'spark');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('spark.python.version', '取值范围：python2,python3', 'python版本','python2', 'OFT', '[\"python3\",\"python2\"]', '0', '0', '1', 'spark引擎设置', 'spark');

-- 3. 插入  linkis_ps_configuration_key_engine_relation : 9(spark.executor.instances),6("*-*,spark-2.4.3")
insert into `linkis_ps_configuration_key_engine_relation` (`config_key_id`, `engine_type_label_id`)
(select config.id as `config_key_id`, label.id AS `engine_type_label_id` FROM linkis_ps_configuration_config_key config
INNER JOIN linkis_cg_manager_label label ON config.engine_conn_type = 'spark' and label.label_value = @SPARK_ALL);

-- 4. 插入  linkis_ps_configuration_config_value :  如 9(spark.executor.instances),"", 6("*-*,spark-2.4.3")
insert into `linkis_ps_configuration_config_value` (`config_key_id`, `config_value`, `config_label_id`)
(select `relation`.`config_key_id` AS `config_key_id`, '' AS `config_value`, `relation`.`engine_type_label_id` AS `config_label_id` FROM linkis_ps_configuration_key_engine_relation relation
INNER JOIN linkis_cg_manager_label label ON relation.engine_type_label_id = label.id AND label.label_value = @SPARK_ALL);

-- 5. 插入 linkis_ps_configuration_category

insert into linkis_ps_configuration_category (`label_id`, `level`)(select label.id as `label_id`, 1 FROM linkis_cg_manager_label label where label.label_value = @SPARK_ALL);
insert into linkis_ps_configuration_category (`label_id`, `level`)(select label.id as `label_id`, 1 FROM linkis_cg_manager_label label where label.label_value = @SPARK_IDE);
insert into linkis_ps_configuration_category (`label_id`, `level`)(select label.id as `label_id`, 1 FROM linkis_cg_manager_label label where label.label_value = @SPARK_VISUALIS);
insert into linkis_ps_configuration_category (`label_id`, `level`)(select label.id as `label_id`, 1 FROM linkis_cg_manager_label label where label.label_value = @SPARK_NODE);





-- flink 配置定义;
SET @FLINK_LABEL="flink-1.12.2";
SET @FLINK_ALL=CONCAT('*-*,',@FLINK_LABEL);
SET @FLINK_IDE=CONCAT('*-IDE,',@FLINK_ALL);

-- 1. 插入 	linkis_cg_manager_label : *-*,spark-2.4.3
insert into `linkis_cg_manager_label` (`label_key`, `label_value`, `label_feature`, `label_value_size`, `update_time`, `create_time`) VALUES ('combined_userCreator_engineType',@FLINK_ALL, 'OPTIONAL', 2, now(), now());
insert into `linkis_cg_manager_label` (`label_key`, `label_value`, `label_feature`, `label_value_size`, `update_time`, `create_time`) VALUES ('combined_userCreator_engineType',@FLINK_IDE, 'OPTIONAL', 2, now(), now());

-- 2. 插入  linkis_ps_configuration_config_key :  等参数
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('flink.jobmanager.memory', '取值范围：1-15，单位：G', 'flink.JobManager内存大小','1g', 'Regex', '^([1-9]|1[0-5])(G|g)$', '0', '0', '1', 'flink资源设置', 'flink');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('flink.taskmanager.memory', '取值范围：1-15，单位：G', 'flink.TaskManager内存大小','1g', 'Regex', '^([1-9]|1[0-5])(G|g)$', '0', '0', '1', 'flink资源设置', 'flink');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('flink.taskmanager.numberOfTaskSlots', '取值范围：1-9，单位：个', 'flink每个TM中TaskSlot个数',  '2', 'NumInterval', '[1,9]', '0', '0', '1','flink资源设置', 'flink');
INSERT INTO `linkis_ps_configuration_config_key` (`key`, `description`, `name`, `default_value`, `validate_type`, `validate_range`, `is_hidden`, `is_advanced`, `level`, `treeName`, `engine_conn_type`) VALUES ('flink.container.num', '取值范围：1-100，单位：个', 'flinkTM数量',  '1', 'NumInterval', '[1,100]', '0', '0', '1','flink资源设置', 'flink');

-- 3. 插入  linkis_ps_configuration_key_engine_relation : 9(spark.executor.instances),6("*-*,spark-2.4.3")

insert into `linkis_ps_configuration_key_engine_relation` (`config_key_id`, `engine_type_label_id`)
(select config.id as `config_key_id`, label.id AS `engine_type_label_id` FROM linkis_ps_configuration_config_key config
INNER JOIN linkis_cg_manager_label label ON config.engine_conn_type = 'flink' and label.label_value = @FLINK_ALL);

-- 4. 插入  linkis_ps_configuration_config_value :  如 9(spark.executor.instances),"", 6("*-*,spark-2.4.3")
insert into `linkis_ps_configuration_config_value` (`config_key_id`, `config_value`, `config_label_id`)
(select `relation`.`config_key_id` AS `config_key_id`, '' AS `config_value`, `relation`.`engine_type_label_id` AS `config_label_id` FROM linkis_ps_configuration_key_engine_relation relation
INNER JOIN linkis_cg_manager_label label ON relation.engine_type_label_id = label.id AND label.label_value = @FLINK_ALL);

-- 5. 插入 linkis_ps_configuration_category
insert into linkis_ps_configuration_category (`label_id`, `level`)(select label.id as `label_id`, 1 FROM linkis_cg_manager_label label where label.label_value = @FLINK_ALL);
insert into linkis_ps_configuration_category (`label_id`, `level`)(select label.id as `label_id`, 1 FROM linkis_cg_manager_label label where label.label_value = @FLINK_IDE);


