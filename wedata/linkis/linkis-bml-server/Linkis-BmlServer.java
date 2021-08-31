


BML upload 上传文件到hdfs的实现:
	- BmlRestfulApi.uploadResource() ->  TaskServiceImpl.createUploadTask() 
		- ResourceServiceImpl.upload()
			- String path = resourceHelper.generatePath(user, resourceId, properties);
			- long size = resourceHelper.upload(path, user, inputStream, sb);
				* 只有1个 hdfs的实现类: HdfsResourceHelper.upload() : fileSystem.write(fsPath, false);


@Path("upload") // 上传资源到hdfs的接口 
BmlRestfulApi.uploadResource(HttpServletRequest req, String resourceHeader,String isExpire, String expireType, FormDataMultiPart form){
	String user = RestfulUtils.getUserName(req);
	String clientIp = HttpRequestHelper.getIp(req);
	ResourceTask resourceTask = taskService.createUploadTask(form, user, properties);{//TaskServiceImpl.createUploadTask()
		ResourceTask resourceTask = ResourceTask.createUploadTask(resourceId, user, properties);
		taskDao.updateState(resourceTask.getId(), TaskState.RUNNING.getValue(), new Date());
		ResourceServiceImpl.UploadResult result = resourceService.upload(form, user, properties).get(0);{//ResourceServiceImpl.upload()
			ResourceHelper resourceHelper = ResourceHelperFactory.getResourceHelper();
			List<FormDataBodyPart> files = formDataMultiPart.getFields("file");
			
			List<UploadResult> results = new ArrayList<>();
			for (FormDataBodyPart p : files) {
				InputStream inputStream = p.getValueAs(InputStream.class);
				String fileName = new String(fileDetail.getFileName().getBytes("ISO8859-1"), "UTF-8");
				String path = resourceHelper.generatePath(user, resourceId, properties);
				boolean isFileExists = resourceHelper.checkIfExists(path, user);
				long size = resourceHelper.upload(path, user, inputStream, sb);{// 唯一实现类 HdfsResourceHelper.upload()
					fileSystem = FSFactory.getFsByProxyUser(fsPath, user);
					if (!fileSystem.exists(fsPath)) FileSystemUtils.createNewFile(fsPath, user, true);
					outputStream = fileSystem.write(fsPath, false);
					while ((ch = inputStream.read(buffer)) != -1) {
						outputStream.write(buffer, 0, ch);
					}
				}
				
				// 插入一条记录到resource表
				Resource resource = Resource.createNewResource(resourceId, user, fileName, properties);
				logger.info("{} uploaded a resource and resourceId is {}", user, resource.getResourceId());
				ResourceVersion resourceVersion = ResourceVersion.createNewResourceVersion(resourceId, path, md5String, clientIp, size, Constant.FIRST_VERSION, 1);
				versionDao.insertNewVersion(resourceVersion); // 插入一条记录到resource version表
				results.add(new UploadResult(resourceId, FIRST_VERSION, isSuccess));
			}
			return results;
		}
		if (result.isSuccess()){
            taskDao.updateState(resourceTask.getId(), TaskState.SUCCESS.getValue(), new Date());
            LOGGER.info("上传资源成功.更新任务 taskId:{}-resourceId:{} 为 {} 状态.", resourceTask.getId(), resourceTask.getResourceId(), TaskState.SUCCESS.getValue());
        } else {
            taskDao.updateState(resourceTask.getId(), TaskState.FAILED.getValue(), new Date());
            LOGGER.info("上传资源失败.更新任务 taskId:{}-resourceId:{} 为 {} 状态.", resourceTask.getId(), resourceTask.getResourceId(), TaskState.FAILED.getValue());
        }
	}
	message = Message.ok("提交上传资源任务成功");
	return Message.messageToResponse(message);
}


// 上传过程中的日志记录
restful.BmlRestfulApi 428 uploadResource - 用户 bigdata 开始上传资源
TaskServiceImpl 74 createUploadTask - 成功保存上传任务信息.taskId:4,resourceTask:
	ResourceTask{id=4, resourceId='371096c3-0afd-40af-9991-a872f7e45030', version='v000001', operation='upload', state='scheduled', submitUser='bigdata', system='null', instance='192.168.51.111:9113', clientIp='127.0.0.1', errMsg='null', extraParams='null', startTime=Mon Aug 30 18:30:17 CST 2021, endTime=null, lastUpdateTime=Mon Aug 30 18:30:17 CST 2021}
TaskServiceImpl 76 createUploadTask - 成功更新任务 taskId:4-resourceId:371096c3-0afd-40af-9991-a872f7e45030 为 running 状态.
FileSystemUtils$ 42 info - doesn't need to call setOwner
ResourceServiceImpl 104 upload - bigdata uploaded a resource and resourceId is 371096c3-0afd-40af-9991-a872f7e45030
BmlRestfulApi 445 uploadResource - 用户 bigdata 提交上传资源任务成功, resourceId is 371096c3-0afd-40af-9991-a872f7e45030





