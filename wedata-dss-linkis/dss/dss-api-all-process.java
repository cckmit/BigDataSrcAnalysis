// cg-xx模块: xx 进程:  
// fromTh:  
// nextTh: 
// 处理逻辑: 
framework-project-server
framework-orchestrator-server
apiservice-server
datapipe-server
workflow-server
flow-execution-server




// framework-project 模块: xx 进程:  
// fromTh:  
// nextTh: 
// 处理逻辑: 



Error starting ApplicationContext. To display the conditions report re-run your application with 'debug' enabled.
2021-12-27 21:28:42.591 ERROR [main] org.springframework.boot.SpringApplication 837 reportFailure - Application run failed org.springframework.beans.factory.BeanCreationException: Error creating bean with name 'appConnManagerRestfulApi': Invocation of init method failed; nested exception is LinkisException{errCode=100000, desc='errCode: 10905 ,desc: URL /api/rest_j/v1/bml/updateVersion request failed! ResponseBody is {"method":null,"status":1,"message":"error code(错误码): 78361, error message(错误信息): resourceId: bc72008f-fadd-48ba-b63c-a07d33d22484is Null, illegal, or deleted!.","data":{"errorMsg":{"serviceKind":"linkis-ps-publicservice","level":2,"port":9105,"errCode":78361,"ip":"bdnode111.hjq.com","desc":"resourceId: bc72008f-fadd-48ba-b63c-a07d33d22484is Null, illegal, or deleted!"}}}. ,ip: bdnode111.hjq.com ,port: 9002 ,serviceKind: dss-framework-project-server', ip='bdnode111.hjq.com', port=9002, serviceKind='dss-framework-project-server'}
	at org.springframework.beans.factory.annotation.InitDestroyAnnotationBeanPostProcessor.postProcessBeforeInitialization(InitDestroyAnnotationBeanPostProcessor.java:160) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.applyBeanPostProcessorsBeforeInitialization(AbstractAutowireCapableBeanFactory.java:415) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.initializeBean(AbstractAutowireCapableBeanFactory.java:1786) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.doCreateBean(AbstractAutowireCapableBeanFactory.java:594) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBean(AbstractAutowireCapableBeanFactory.java:516) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.AbstractBeanFactory.lambda$doGetBean$0(AbstractBeanFactory.java:324) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.getSingleton(DefaultSingletonBeanRegistry.java:234) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.AbstractBeanFactory.doGetBean(AbstractBeanFactory.java:322) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.AbstractBeanFactory.getBean(AbstractBeanFactory.java:202) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.DefaultListableBeanFactory.preInstantiateSingletons(DefaultListableBeanFactory.java:897) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.context.support.AbstractApplicationContext.finishBeanFactoryInitialization(AbstractApplicationContext.java:879) ~[spring-context-5.2.8.RELEASE.jar:5.2.8.RELEASE]
	at org.springframework.context.support.AbstractApplicationContext.refresh(AbstractApplicationContext.java:551) ~[spring-context-5.2.8.RELEASE.jar:5.2.8.RELEASE]
	at org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext.refresh(ServletWebServerApplicationContext.java:143) ~[spring-boot-2.3.2.RELEASE.jar:2.3.2.RELEASE]
	at org.springframework.boot.SpringApplication.refresh(SpringApplication.java:758) ~[spring-boot-2.3.2.RELEASE.jar:2.3.2.RELEASE]
	at org.springframework.boot.SpringApplication.refresh(SpringApplication.java:750) [spring-boot-2.3.2.RELEASE.jar:2.3.2.RELEASE]
	at org.springframework.boot.SpringApplication.refreshContext(SpringApplication.java:397) [spring-boot-2.3.2.RELEASE.jar:2.3.2.RELEASE]
	at org.springframework.boot.SpringApplication.run(SpringApplication.java:315) [spring-boot-2.3.2.RELEASE.jar:2.3.2.RELEASE]
	at org.apache.linkis.DataWorkCloudApplication.main(DataWorkCloudApplication.java:111) [linkis-module-1.0.3.jar:1.0.3]
	at com.webank.wedatasphere.dss.framework.project.server.DSSProjectServerApplication$.main(DSSProjectServerApplication.scala:36) [dss-framework-project-server-1.0.1.jar:?]
	at com.webank.wedatasphere.dss.framework.project.server.DSSProjectServerApplication.main(DSSProjectServerApplication.scala) [dss-framework-project-server-1.0.1.jar:?]
Caused by: com.webank.wedatasphere.dss.common.exception.DSSRuntimeException:
 errCode: 100000 ,desc: errCode: 10905 ,desc: URL /api/rest_j/v1/bml/updateVersion request failed! ResponseBody is {"method":null,"status":1,"message":"error code(错误码): 78361, error message(错误信息): resourceId: bc72008f-fadd-48ba-b63c-a07d33d22484is Null, illegal, or deleted!.","data":{"errorMsg":{"serviceKind":"linkis-ps-publicservice","level":2,"port":9105,"errCode":78361,"ip":"bdnode111.hjq.com","desc":"resourceId: bc72008f-fadd-48ba-b63c-a07d33d22484is Null, illegal, or deleted!"}}}. ,ip: bdnode111.hjq.com ,port: 9002 ,serviceKind: dss-framework-project-server ,ip: bdnode111.hjq.com ,port: 9002 ,serviceKind: dss-framework-project-server
	at com.webank.wedatasphere.dss.common.utils.DSSExceptionUtils.lambda$handling$0(DSSExceptionUtils.java:43) ~[dss-common-1.0.1.jar:?]
	at java.util.ArrayList.forEach(ArrayList.java:1259) ~[?:1.8.0_261]
	at com.webank.wedatasphere.dss.framework.appconn.restful.AppConnManagerRestfulApi.init(AppConnManagerRestfulApi.java:51) ~[dss-appconn-framework-1.0.1.jar:?]
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[?:1.8.0_261]
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[?:1.8.0_261]
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_261]
	at java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_261]
	at org.springframework.beans.factory.annotation.InitDestroyAnnotationBeanPostProcessor$LifecycleElement.invoke(InitDestroyAnnotationBeanPostProcessor.java:389) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.annotation.InitDestroyAnnotationBeanPostProcessor$LifecycleMetadata.invokeInitMethods(InitDestroyAnnotationBeanPostProcessor.java:333) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.annotation.InitDestroyAnnotationBeanPostProcessor.postProcessBeforeInitialization(InitDestroyAnnotationBeanPostProcessor.java:157) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	... 19 more



ERROR [main] com.webank.wedatasphere.dss.common.utils.DSSExceptionUtils 42 
lambda$handling$0 - execute failed,reason: org.apache.linkis.httpclient.exception.HttpClientResultException: 
errCode: 10905 ,desc: URL /api/rest_j/v1/bml/updateVersion request failed! 
ResponseBody is {"method":null,"status":1,"message":"error code(错误码): 78361, 
error message(错误信息): resourceId: bc72008f-fadd-48ba-b63c-a07d33d22484is Null, illegal, or deleted!.",
"data":{"errorMsg":{"serviceKind":"linkis-ps-publicservice","level":2,"port":9105,
	"errCode":78361,"ip":"bdnode111.hjq.com","desc":"resourceId: bc72008f-fadd-48ba-b63c-a07d33d22484is Null, illegal, or deleted!"}}}. 
,ip: bdnode111.hjq.com ,port: 9002 ,serviceKind: dss-framework-project-server
	at org.apache.linkis.httpclient.dws.response.DWSResult$class.set(DWSResult.scala:54) ~[linkis-gateway-httpclient-support-1.0.3.jar:1.0.3]
	at org.apache.linkis.bml.response.BmlResult.set(BmlResult.scala:26) ~[linkis-bml-client-1.0.3.jar:1.0.3]
	at org.apache.linkis.httpclient.dws.DWSHttpClient$$anonfun$httpResponseToResult$2.apply(DWSHttpClient.scala:67) ~[linkis-gateway-httpclient-support-1.0.3.jar:1.0.3]
	at org.apache.linkis.httpclient.dws.DWSHttpClient$$anonfun$httpResponseToResult$2.apply(DWSHttpClient.scala:63) ~[linkis-gateway-httpclient-support-1.0.3.jar:1.0.3]
	at scala.Option.map(Option.scala:146) ~[scala-library-2.11.12.jar:?]
	at org.apache.linkis.httpclient.dws.DWSHttpClient.httpResponseToResult(DWSHttpClient.scala:63) ~[linkis-gateway-httpclient-support-1.0.3.jar:1.0.3]
	at org.apache.linkis.httpclient.AbstractHttpClient.responseToResult(AbstractHttpClient.scala:331) ~[linkis-httpclient-1.0.3.jar:1.0.3]
	at org.apache.linkis.httpclient.AbstractHttpClient.execute(AbstractHttpClient.scala:107) ~[linkis-httpclient-1.0.3.jar:1.0.3]
	at org.apache.linkis.httpclient.AbstractHttpClient.execute(AbstractHttpClient.scala:87) ~[linkis-httpclient-1.0.3.jar:1.0.3]
	at org.apache.linkis.bml.client.impl.HttpBmlClient.updateResource(HttpBmlClient.scala:210) ~[linkis-bml-client-1.0.3.jar:1.0.3]
	at com.webank.wedatasphere.dss.framework.appconn.service.impl.AppConnResourceServiceImpl.upload(AppConnResourceServiceImpl.java:118) ~[dss-appconn-framework-1.0.1.jar:?]
	at com.webank.wedatasphere.dss.framework.appconn.restful.AppConnManagerRestfulApi.lambda$init$0(AppConnManagerRestfulApi.java:53) ~[dss-appconn-framework-1.0.1.jar:?]
	at com.webank.wedatasphere.dss.common.utils.DSSExceptionUtils.lambda$handling$0(DSSExceptionUtils.java:40) ~[dss-common-1.0.1.jar:?]
	at java.util.ArrayList.forEach(ArrayList.java:1259) ~[?:1.8.0_261]
	at com.webank.wedatasphere.dss.framework.appconn.restful.AppConnManagerRestfulApi.init(AppConnManagerRestfulApi.java:51) ~[dss-appconn-framework-1.0.1.jar:?]
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[?:1.8.0_261]
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[?:1.8.0_261]
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_261]
	at java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_261]
	at org.springframework.beans.factory.annotation.InitDestroyAnnotationBeanPostProcessor$LifecycleElement.invoke(InitDestroyAnnotationBeanPostProcessor.java:389) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.annotation.InitDestroyAnnotationBeanPostProcessor$LifecycleMetadata.invokeInitMethods(InitDestroyAnnotationBeanPostProcessor.java:333) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.annotation.InitDestroyAnnotationBeanPostProcessor.postProcessBeforeInitialization(InitDestroyAnnotationBeanPostProcessor.java:157) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.applyBeanPostProcessorsBeforeInitialization(AbstractAutowireCapableBeanFactory.java:415) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.initializeBean(AbstractAutowireCapableBeanFactory.java:1786) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.doCreateBean(AbstractAutowireCapableBeanFactory.java:594) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBean(AbstractAutowireCapableBeanFactory.java:516) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.AbstractBeanFactory.lambda$doGetBean$0(AbstractBeanFactory.java:324) ~[spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.getSingleton(DefaultSingletonBeanRegistry.java:234) [spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.AbstractBeanFactory.doGetBean(AbstractBeanFactory.java:322) [spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.AbstractBeanFactory.getBean(AbstractBeanFactory.java:202) [spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.beans.factory.support.DefaultListableBeanFactory.preInstantiateSingletons(DefaultListableBeanFactory.java:897) [spring-beans-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.context.support.AbstractApplicationContext.finishBeanFactoryInitialization(AbstractApplicationContext.java:879) [spring-context-5.2.8.RELEASE.jar:5.2.8.RELEASE]
	at org.springframework.context.support.AbstractApplicationContext.refresh(AbstractApplicationContext.java:551) [spring-context-5.2.8.RELEASE.jar:5.2.8.RELEASE]
	at org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext.refresh(ServletWebServerApplicationContext.java:143) [spring-boot-2.3.2.RELEASE.jar:2.3.2.RELEASE]
	at org.springframework.boot.SpringApplication.refresh(SpringApplication.java:758) [spring-boot-2.3.2.RELEASE.jar:2.3.2.RELEASE]
	at org.springframework.boot.SpringApplication.refresh(SpringApplication.java:750) [spring-boot-2.3.2.RELEASE.jar:2.3.2.RELEASE]
	at org.springframework.boot.SpringApplication.refreshContext(SpringApplication.java:397) [spring-boot-2.3.2.RELEASE.jar:2.3.2.RELEASE]
	at org.springframework.boot.SpringApplication.run(SpringApplication.java:315) [spring-boot-2.3.2.RELEASE.jar:2.3.2.RELEASE]
	at org.apache.linkis.DataWorkCloudApplication.main(DataWorkCloudApplication.java:111) [linkis-module-1.0.3.jar:1.0.3]
	at com.webank.wedatasphere.dss.framework.project.server.DSSProjectServerApplication$.main(DSSProjectServerApplication.scala:36) [dss-framework-project-server-1.0.1.jar:?]
	at com.webank.wedatasphere.dss.framework.project.server.DSSProjectServerApplication.main(DSSProjectServerApplication.scala) [dss-framework-project-server-1.0.1.jar:?]





// framework-orchestrator 模块: xx 进程:  
// fromTh:  
// nextTh: 
// 处理逻辑: 



// apiservice 模块: xx 进程:  
// fromTh:  
// nextTh: 
// 处理逻辑: 



// datapipe 模块: xx 进程:  
// fromTh:  
// nextTh: 
// 处理逻辑: 


// workflow 模块: xx 进程:  
// fromTh:  
// nextTh: 
// 处理逻辑: 


// flow-execution 模块: xx 进程:  
// fromTh:  
// nextTh: 
// 处理逻辑: 

