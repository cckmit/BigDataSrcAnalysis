


RMReceiver.receiveAndReply(message: Any, sender: Sender){
	case RequestResource(moduleInstance, user, creator, resource) =>
      rm.requestResource(moduleInstance, user, creator, resource)
	  
    case RequestResourceAndWait(moduleInstance, user, creator, resource, waitTime) =>
      rm.requestResource(moduleInstance, user, creator, resource, waitTime) {//DefaultResourceManager.requestResource()
		if (hasModuleInstanceEvent(moduleInstance)) {}
		if (hasUserEvent(user, moduleInstance.getApplicationName)) {}
		val reqService = getRequestResourceService(moduleInstance)
		var canRequest = reqService.canRequest(moduleInstance, user, creator, resource);{//DriverAndYarnReqResourceService.canRequest()
			var canSuperRequest = super.canRequest(moduleInstance, user, creator, requestResource);{//RequestResourceService
				if ((moduleLeftResource - requestResource) < protectedResource) {
					throw new RMWarnException(111005, s"${generateNotEnoughMessage(requestResource, moduleLeftResource)}")
				}
				val (moduleAvailableResource, creatorAvailableResource) = userMetaData.getUserAvailableResource(moduleInstance.getApplicationName, user, creator)
												instances	RAM	cpu	Queuememory	Queuecore	queueInstances
					moduleAvailableResource		10			20G	20	10G			10			3
					creatorAvailableResource	10			20G	20	10G			10			3
								
				val (moduleUsedResource, creatorUsedResource) = userResourceRecordService.getModuleAndCreatorResource(moduleInstance.getApplicationName, user, creator, requestResource)
					moduleUsedResource			1			2G	1	12G			6			0
					creatorUsedResource			1			2G	1	12G			6			0


				if (moduleAvailableResource.resource >= moduleUsedResource) if (creatorAvailableResource.resource >= creatorUsedResource)
				  true
				else {
				  info(s"creator:$creator for $user had used module resource:$creatorUsedResource > creatorAvailableResource:${creatorAvailableResource.resource} ")
				  throw new RMWarnException(111007, s"${generateNotEnoughMessage(creatorUsedResource, creatorAvailableResource.resource)}")
				} else {// module已使用资源 超过了 module可用资源数; 
				  info(s"$user had used module resource:$moduleUsedResource > moduleAvailableResource: $moduleAvailableResource")
				  throw new RMWarnException(111005, s"${generateNotEnoughMessage(moduleUsedResource, moduleAvailableResource.resource)}")
				}	
				
			}
			if (! canSuperRequest) return false
			val yarnResource = requestResource.asInstanceOf[DriverAndYarnResource].yarnResource
			// 向Hadoop-Yarn的8088端口获取 资源max和userd资源信息; 
			val (maxCapacity, usedCapacity) = YarnUtil.getQueueInfo(yarnResource.queueName);{
				YarnUtil.getResources(){
					val resp = getResponseByUrl("scheduler"){
						val httpGet = new HttpGet(rm_web_address + "/ws/v1/cluster/" + url)
						val response = httpClient.execute(httpGet) // 向 hadoop-yarn请求获取 scheduler信息:  http://bdnode101:8088/ws/v1/cluster/scheduler 发起Get请求; 
						parse(EntityUtils.toString(response.getEntity()))
					}
					val schedulerType = (resp \ "scheduler" \ "schedulerInfo" \ "type").asInstanceOf[JString].values
					
				}
			}
			info(s"This queue:${yarnResource.queueName} used resource:$usedCapacity and max resource：$maxCapacity")
			
			val queueLeftResource = maxCapacity - usedCapacity // 
			if (queueLeftResource < yarnResource) { // 请求的队列yarnResource 大于队列 剩余资源(queueLeftResource) 
										memory	cpu	
					queueLeftResource	8G		8	0
					yarnResource		12G		6	0
					
				info(s"User: $user request queue (${yarnResource.queueName}) resource $yarnResource is greater than queue (${yarnResource.queueName}) remaining resources $queueLeftResource(用户:$user 请求的队列（${yarnResource.queueName}）资源$yarnResource 大于队列（${yarnResource.queueName}）剩余资源$queueLeftResource)")
				throw new RMWarnException(111007, s"${generateNotEnoughMessage(yarnResource, queueLeftResource)}")
			}else{
				true
			}				
		}
		if (!canRequest){
			return NotEnoughResource(s"user：$user not enough resource")
		}
	  }
	  
    case moduleInstance: ServiceInstance => ResourceInfo(rm.getModuleResourceInfo(moduleInstance))
    case ResourceOverload(moduleInstance) => rm.instanceCanService(moduleInstance)
}





