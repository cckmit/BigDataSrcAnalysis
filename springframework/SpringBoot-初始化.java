
ConfigFileApplicationListener.onApplicationEvent(){
    onApplicationEnvironmentPreparedEvent();
        postProcessEnvironment();
            addPropertySources();{//ConfigFileApplicationListener
                RandomValuePropertySource.addToEnvironment(environment);
                new Loader(environment, resourceLoader).load();{
                    initializeProfiles();
                    while (!this.profiles.isEmpty()) {
                        Profile profile = this.profiles.poll();
                            
                    }
                }
            }
}


load:330, ConfigFileApplicationListener$Loader (org.springframework.boot.context.config)
addPropertySources:214, ConfigFileApplicationListener (org.springframework.boot.context.config)
postProcessEnvironment:197, ConfigFileApplicationListener (org.springframework.boot.context.config)
onApplicationEnvironmentPreparedEvent:184, ConfigFileApplicationListener (org.springframework.boot.context.config)
onApplicationEvent:170, ConfigFileApplicationListener (org.springframework.boot.context.config)
doInvokeListener:172, SimpleApplicationEventMulticaster (org.springframework.context.event)
invokeListener:165, SimpleApplicationEventMulticaster (org.springframework.context.event)
multicastEvent:139, SimpleApplicationEventMulticaster (org.springframework.context.event)
multicastEvent:127, SimpleApplicationEventMulticaster (org.springframework.context.event)
environmentPrepared:74, EventPublishingRunListener (org.springframework.boot.context.event)
environmentPrepared:54, SpringApplicationRunListeners (org.springframework.boot)
prepareEnvironment:358, SpringApplication (org.springframework.boot)
run:317, SpringApplication (org.springframework.boot)
run:137, SpringApplicationBuilder (org.springframework.boot.builder)
bootstrapServiceContext:197, BootstrapApplicationListener (org.springframework.cloud.bootstrap)
onApplicationEvent:104, BootstrapApplicationListener (org.springframework.cloud.bootstrap)
onApplicationEvent:70, BootstrapApplicationListener (org.springframework.cloud.bootstrap)
doInvokeListener:172, SimpleApplicationEventMulticaster (org.springframework.context.event)
invokeListener:165, SimpleApplicationEventMulticaster (org.springframework.context.event)
multicastEvent:139, SimpleApplicationEventMulticaster (org.springframework.context.event)
multicastEvent:127, SimpleApplicationEventMulticaster (org.springframework.context.event)
environmentPrepared:74, EventPublishingRunListener (org.springframework.boot.context.event)
environmentPrepared:54, SpringApplicationRunListeners (org.springframework.boot)
prepareEnvironment:358, SpringApplication (org.springframework.boot)
run:317, SpringApplication (org.springframework.boot)
run:1255, SpringApplication (org.springframework.boot)
run:1243, SpringApplication (org.springframework.boot)
main:31, SpringCloudEurekaApplication (com.webank.wedatasphere.linkis.eureka)


