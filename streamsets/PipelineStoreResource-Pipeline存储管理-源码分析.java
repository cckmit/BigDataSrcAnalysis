rest/v1/pipelines +queryParam		GET		PipelineStoreResource.getPipelines();




获取该用户下有哪些Pipelines的接口和源码
http://192.168.41.141:18630/rest/v1/pipelines?
	?	orderBy=LAST_MODIFIED
	&	order=DESC
	&	label=system:allPipelines
	&	offset=0
	&	len=50
	&	includeStatus=true

后台触发接口: 
PipelineStoreResource.getPipelines(orderBy,order,label,offset,len,includeStatus){
	
	// 构造对象时,若pipeline.access.control.enabled=true, store= AclPipelineStoreTask, 否则 store= CachePipelineStoreTask;
	store.getPipelines(){
		
		//1. 当pipeline.access.control.enabled=true时, store= AclPipelineStoreTask
		AclPipelineStoreTask.getPipelines(){
			filteredPipelines= AclPipelineStoreTask.filterPipelineBasedOnReadAcl(){
				return Collections2.filter(pipelineStore.getPipelines(),new Predicate<PipelineInfo>() {
					public boolean apply(PipelineInfo pipelineInfo) {
						return aclStore[CacheAclStoreTask].isPermissionGranted(pipelineInfo.getPipelineId(), EnumSet.of(Action.READ), currentUser){
							CacheAclStoreTask[extends AbstractAclStoreTask].isPermissionGranted(pipelineName,actions,currentUser){
								// 当user为Null, 或者 current.role == ("admin" or "datacollector:admin"), 表示是超级管理员;
								if (currentUser == null || (user.getRoles()!=null ) && (user.getRoles().contains("admin")||user.getRoles().contains("datacollector:admin"))  ) { 
									return true;
								}
								
								// 
								/*获取该pipeline-> acl.json, 若缓存有直接读取缓存; 其Acl的结构如下: Acl.permissions = List<Permission(subjectId,actions:List<String>)>
								*  Acl{
										"resourceOwner" : "admin",
										"permissions" : [ 
											{"subjectId" : "admin","actions" : [ "READ", "WRITE", "EXECUTE" ]}, 
											{"subjectId" : "wizard_manager","actions" : [ "READ", "WRITE", "EXECUTE" ]}
										]
									}
								*/
								Acl acl = getAcl(pipelineName){ // CacheAclStoreTask.getAcl(name)
									synchronized (lockCache.getLock(name)) {
										if (!pipelineAclMap[Map<String,Acl>].containsKey(name)) {
											Acl acl = aclStore[FileAclStoreTask].getAcl(name){
												FileAclStoreTask.getAcl(pipeline,checkExistence){
													if (checkExistence && !pipelineStore.hasPipeline(pipelineName)) {throw new PipelineStoreException();}
													
													Path aclFilePath = getPipelineAclFile(pipelineName);
													if (Files.exists(aclFilePath)) {
														try (InputStream aclFile = Files.newInputStream(getPipelineAclFile(pipelineName))) {
															AclJson aclJsonBean = json.readValue(aclFile, AclJson.class);
															return AclDtoJsonMapper.INSTANCE.asAclDto(aclJsonBean);
														}
													}
												}
											}
											
											if (acl != null) {
												pipelineAclMap.put(name, acl);
											}
										}
										return pipelineAclMap.get(name);
									}
								}
								if (acl == null) {
									PipelineInfo pipelineInfo = pipelineStore.getInfo(pipelineName);
									return pipelineInfo.getCreator().equals(currentUser.getName());
								}
								
								// 判断 currentUser的所申请的权限, 是否满足该pipeline的acl.json.permissions中任意赋权; 若匹配任一,则为true;
								return isPermissionGranted(acl, actions, currentUser){
									boolean permissionGranted = false;
									
									subjectIds.add(currentUser.getName());
									if (currentUser.getGroups() != null) {
										subjectIds.addAll(currentUser.getGroups());
									}
									
									// 取出该acl中所有的 赋权实体[Permission], 并过滤出当前用户相关的 赋权实体(currentUser.subjectIds[userName +groups])
									// permissions: 和currentUser相关的所有赋权实体;
									Collection<Permission> permissions = filterPermission(acl, subjectIds){
										return Collections2.filter(acl.getPermissions():List<Permission>, new Predicate<Permission>() {
											public boolean apply(Permission permission) {
												return subjectIds.contains(permission.getSubjectId());
											}
										});
									}
									
									// 当对currentUser所有赋权实体中, 有任意一个的 赋权项目与所申请的读写权向匹配, 则 permissionGranted=true;表示赋权成功.
									for (Permission permission : permissions) {
										permissionGranted = permission != null && permission.getActions().containsAll(actions);
										if (permissionGranted) { break;}
									}
									return permissionGranted;
								}
								
							}
						
						}
					}
				});
			}
			return new ArrayList<>(filteredPipelines);
		}
		
		// 2. 当未开启 acl(访问权限), store= CachePipelineStoreTask;
		CachePipelineStoreTask.getPipelines(){
			return Collections.unmodifiableList(new ArrayList<>(pipelineInfoMap.values()));
		}
		
	}
	
	// 过滤: 判断为true的才会输出; 即过滤出满足Predicate条件( 判断某pipeline是否是属于该label类型 )的元素;
	Collection<PipelineInfo> filteredCollection= Collections2.filter(pipelineInfoList, new Predicate<PipelineInfo>() {
		// 只有判断为 true的 才会被过滤出来(输出);
		@Override public boolean apply(PipelineInfo pipelineInfo) {
			// 当请求参数filterText不为null时, 对 pipeline的名称(title)按 filterText过滤;
			if (filterText != null && !title.toLowerCase().contains(filterText.toLowerCase())) {
				return false;
			}
			
			// 选择Pipeline的类型: All, Running, 
			if (label != null) {
				Map<String, Object> metadata = pipelineInfo.getMetadata();
				switch (label) {
					case SYSTEM_ALL_PIPELINES:
						return true;
					
					case SYSTEM_RUNNING_PIPELINES:// RunningPipelines: 就是每个 PipelineState.getStatus.isActive()==true的
						// 包括: [Starting, Running,Finishing,Stopping,DisConnecting]+ _error + ConnectError
						PipelineState state = manager.getPipelineState(pipelineInfo.getPipelineId(), pipelineInfo.getLastRev());
						pipelineStateCache.put(pipelineInfo.getPipelineId(), state);
						return state.getStatus().isActive();
						
					case SYSTEM_NON_RUNNING_PIPELINES: // Non Running Pipelines:
						return !state.getStatus().isActive();
						
					case SYSTEM_INVALID_PIPELINES: // 配置有误的Pipeline
						return ! pipelineInfo.isValid();
					
					case SYSTEM_ERROR_PIPELINES: // 
						boolean flag =( status == PipelineStatus.START_ERROR || status == PipelineStatus.RUNNING_ERROR 
							status == PipelineStatus.RUN_ERROR ||status == PipelineStatus.CONNECT_ERROR );
						return flat ;
					
						
				}
			}
		}
	});
	
}




# 向Spark-Executor上的一个Slave实例SDC发送 getPipeline(pid)请求,响应流程:
# 在DC上的原理相似

PipelineStoreResource.getPipelineInfo(String name){
	// 1. 先从磁盘加载其 pipelineId/info.json
	PipelineInfo pipelineInfo = store[PipelineStoreTask].getInfo(name){//SlavePipelineStoreTask.getInfo(String name)
		return pipelineStore[PipelineStoreTask].getInfo(name){//FilePipelineStoreTask.getInfo(String name)
			return getInfo(name, false){//FilePipelineStoreTask.getInfo(String name, boolean checkExistence)
				    synchronized (lockCache.getLock(name)) {
					  if (checkExistence && !hasPipeline(name)) {
						throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
					  }
					  // 获取该SlaveSDC对应的Data目录,一般是在该容器所在机器本地的Container目录: nm-local-dir 下 containerId位置;
					  Path filePath = getInfoFile(name){
						return getPipelineDir(name).resolve(INFO_FILE);{
							getPipelineDir(name){
								return storeDir[UnixPath].resolve(PipelineUtils.escapedPipelineName(name));{
									// storeDir = /home/app/appdata/hadoop/dataDir/tmpDir/nm-local-dir/usercache/app/appcache/application_1585661170516_0006/container_1585661170516_0006_01_000002/data/pipelines
								}
							}
							.resolve(INFO_FILE);
						}
					  }
					  try (InputStream infoFile = Files.newInputStream(filePath)) {
						PipelineInfoJson pipelineInfoJsonBean = json.readValue(infoFile, PipelineInfoJson.class);
						return pipelineInfoJsonBean.getPipelineInfo();
					  } catch (Exception ex) {
						throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, ex);
					  }
					}
				
			}
		}
	}
	String title = name;
	
	// 
	switch (get) {
      case "pipeline":
        PipelineConfiguration pipeline = store.load(name, rev);{//SlavePipelineStoreTask.load(String name, String tagOrRev)
			return pipelineStore.load(name, tagOrRev);{//FilePipelineStoreTask.load(String name, String tagOrRev)
				synchronized (lockCache.getLock(name)) {
				  if (!hasPipeline(name)) {
					throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
				  }
				  // getPipelineFile()是读取pipeline.json, 而getInfo()->getInfoFile()是读取 info.json文件;
				  /*
				  * info.json : 	464B 字节; createed, lastModified, uuid, sdcId, metadate信息;
				  * pipeline.json:	15.2 KB:  其info字段及为 info.json的内容, 此外还包括几个大字段:
						- configuration: Pipeline相关配置: exeMode, deliveryGuraantee等;
						- stages:	各Stages的配置情况;
						- startEventStages + stopEventStages; 
						- issues;
				  */
				  Path pipelineFile = getPipelineFile(name);
				  try (InputStream pipelineFile = Files.newInputStream(getPipelineFile(name))) {
					PipelineInfo info = getInfo(name);
					// 1. 先将 pipeline.json解析封装进 PipelineConfiguration对象;
					PipelineConfigurationJson pipelineConfigBean=json.readValue(pipelineFile, PipelineConfigurationJson.class);
					PipelineConfiguration pipeline = pipelineConfigBean.getPipelineConfiguration();
					// 2. 再将 刚从 info.json中获取的PipelineInfo对象更新到 PipelineConfiguration中;
					pipeline.setPipelineInfo(info);

					Map<String, Map> uiInfo;
					// 若uiinfo.json存在时(只有DC本地机器才存在);
					if (Files.exists(getPipelineUiInfoFile(name))) {
					  try (InputStream uiInfoFile = Files.newInputStream(getPipelineUiInfoFile(name))) {
						uiInfo = json.readValue(uiInfoFile, Map.class);
						// 将里面原先的uiInfo:Map<String,Object> 清空,
						/**
						* 1. 先将pipelineConf.uiInfo这个Map清空;
						* 2. 将uiinfo.json中的 :pipeline: putAll()进 uiInfo中;
						* 3. 将uiinfo.json中各stage.InstanceName的值,覆盖pipeline.stages.uiInfo中值;
						*/
						pipeline = injectUiInfo(uiInfo, pipeline);{// FilePipelineStoreTask.injectUiInfo()
							pipelineConf.getUiInfo().clear();
							if (uiInfo.containsKey(":pipeline:")) {
							  pipelineConf.getUiInfo().clear();
							  pipelineConf.getUiInfo().putAll(uiInfo.get(":pipeline:"));
							}
							// 遍历pipeline中每个Stage并将uiinfo.json中key等于InstanceName的信息,覆盖该Stage中uiInfo里的内容;
							// 现在ZhenYuOne账号报 缺少2个字段的问题, 有可能就是此处更新引起的.
							for (StageConfiguration stage : pipelineConf.getStages()) {
							  stage.getUiInfo().clear();
							  if (uiInfo.containsKey(stage.getInstanceName())) {
								stage.getUiInfo().clear();
								stage.getUiInfo().putAll(uiInfo.get(stage.getInstanceName()));
							  }
							}
							return pipelineConf;
							
						}
					  }
					}

					return pipeline;
				  }
				  catch (Exception ex) {
					throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, ex.toString(), ex);
				  }
				}
				
			}
		}
        PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipeline);
        // 对于从本地加载到的Pipeline.json, 还要validate()校验一下;否则可能显示不了;
		pipeline = validator.validate();
        data = BeanHelper.wrapPipelineConfiguration(pipeline);// 用PipelineConfigurationJson(pipelineConfiguration)对象包装输出;
        title = pipeline.getTitle() != null ? pipeline.getTitle() : pipeline.getInfo().getPipelineId();
        break;
      case "info":
        data = BeanHelper.wrapPipelineInfo(store.getInfo(name));
        break;
      case "history":// 返回的 PipelineRevInfo 对象仅简单封装了 date,user,rev字段; 可能以后版本再实现pipelineConfigHistory?
        data = BeanHelper.wrapPipelineRevInfo(store.getHistory(name));
        break;
      default:
        throw new IllegalArgumentException(Utils.format("Invalid value for parameter 'get': {}", get));
    }

    if (attachment) {
      Map<String, Object> envelope = new HashMap<String, Object>();
      envelope.put("pipelineConfig", data);

      RuleDefinitions ruleDefinitions = store.retrieveRules(name, rev);
      envelope.put("pipelineRules", BeanHelper.wrapRuleDefinitions(ruleDefinitions));

      return Response.ok().
          header("Content-Disposition", "attachment; filename=\"" + title + ".json\"").
          type(MediaType.APPLICATION_JSON).entity(envelope).build();
    } else {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(data).build();
    }

}


# 当Running状态下: 拖动算子后, 向PipelineStoreResource.saveUiInfo()发送 保存UI的请求到 uiinfo.json 文件中
PipelineStoreResource.saveUiInfo(String name,String rev,Map uiInfo){
	// 从CachePipelineStoreTask的内存(pipelineInfoMap:Map<String,PipelineInfo>)中 读取PipelineInfo对象;
	PipelineInfo pipelineInfo = store[PipelineStoreTask].getInfo(name){//CachePipelineStoreTask.getInfo(String name)
		PipelineInfo pipelineInfo = pipelineInfoMap.get(name);// pipelineInfoMap: Map<String, PipelineInfo>
		if (pipelineInfo == null) {
		  throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
		} else {
		  return pipelineInfo;
		}
	}
	
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    store.saveUiInfo(name, rev, uiInfo);{
		pipelineStore.saveUiInfo(name, rev, uiInfo);{//FilePipelineStoreTask.saveUiInfo()
			// /home/app/appdata/streamset/data/pipelines/ClusterOriginKafkaDemo255cbfd4-56ff-4c93-a94a-551721a9e80b/uiinfo.json
			try (OutputStream uiInfoFile = Files.newOutputStream(getPipelineUiInfoFile(name))){
			  json.writeValue(uiInfoFile, uiInfo);
			} catch (Exception ex) {
			  throw new PipelineStoreException(ContainerError.CONTAINER_0405, name, ex.toString(), ex);
			}
		}
	}
	
    return Response.ok().build();
}


# 当Retry 状态下 拖动算子?


# 当Stopped, StoppedError, Edit等非Running状态下时:拖拽算子触发 savePipeline(),更新pipeline.json/info.json/uiinfo.json, pipelineState.json4 个文件
PipelineStoreResource.savePipeline(String name,String rev,String description,PipelineConfigurationJson pipeline){
	if (store.isRemotePipeline(name, rev)) {
      throw new PipelineException(ContainerError.CONTAINER_01101, "SAVE_PIPELINE", name);
    }
    PipelineInfo pipelineInfo = store.getInfo(name);
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());
    PipelineConfiguration pipelineConfig = BeanHelper.unwrapPipelineConfiguration(pipeline);
    PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipelineConfig);
    pipelineConfig = validator.validate();
    pipelineConfig = store.save(user, name, rev, description, pipelineConfig);{//CachePipelineStoreTask.save()
		synchronized (lockCache.getLock(name)) {
		  PipelineConfiguration pipelineConf = pipelineStore.save(user, name, tag, tagDescription, pipeline);{
			  
			  synchronized (lockCache.getLock(name)) {
			  if (!hasPipeline(name)) {
				throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
			  }
			  PipelineInfo savedInfo = getInfo(name);
			  if (!savedInfo.getUuid().equals(pipeline.getUuid())) {
				throw new PipelineStoreException(ContainerError.CONTAINER_0205, name);
			  }
			  if (pipelineStateStore != null) {
				PipelineStatus pipelineStatus = pipelineStateStore.getState(name, tag).getStatus();
				if (pipelineStatus.isActive()) {
				  throw new PipelineStoreException(ContainerError.CONTAINER_0208, pipelineStatus);
				}
			  }
			  UUID uuid = UUID.randomUUID();
			  PipelineInfo info = new PipelineInfo();
			  try (
				  OutputStream infoFile = Files.newOutputStream(getInfoFile(name));
				  OutputStream pipelineFile = Files.newOutputStream(getPipelineFile(name));
				){
				pipeline.setUuid(uuid);
				// 更新 info.json文件;
				json.writeValue(infoFile, BeanHelper.wrapPipelineInfo(info));
				// 更新 pipeline.json 文件;
				json.writeValue(pipelineFile, BeanHelper.wrapPipelineConfiguration(pipeline));
				if (pipelineStateStore != null) {
				  List<Issue> errors = new ArrayList<>();
				  PipelineBeanCreator.get().create(pipeline, errors, null);
				  // 更新 runInfo下 pipelineState.json文件;
				  pipelineStateStore.edited(user, name, tag,  PipelineBeanCreator.get().getExecutionMode(pipeline, errors), false);
				  pipeline.getIssues().addAll(errors);
				}

				Map<String, Object> uiInfo = extractUiInfo(pipeline);
				
				// 更新 uiinfo.json文件;
				saveUiInfo(name, tag, uiInfo);

			  } catch (Exception ex) {
				throw new PipelineStoreException(ContainerError.CONTAINER_0204, name, ex.toString(), ex);
			  }
			  pipeline.setPipelineInfo(info);
			  return pipeline;
			}
			  
		  }
		  pipelineInfoMap.put(name, pipelineConf.getInfo());
		  return pipelineConf;
		}
	}
	
    return Response.ok().entity(BeanHelper.wrapPipelineConfiguration(pipelineConfig)).build();
	
}

