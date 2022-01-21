2022-01-19 20:06:56,741 INFO org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet: bigdata is accessing unchecked http://bdnode102.hjq.com:45764/13.53d6719faba0b4c2707a.js which is the app master GUI of application_1642592462740_0003 owned by bigdata
2022-01-19 20:06:56,901 INFO org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet: 
bigdata is accessing unchecked http://bdnode102.hjq.com:45764/jobs/overview which is the app master GUI of application_1642592462740_0003 owned by bigdata

2022-01-19 20:06:28,997 INFO org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl: container_1642592462740_0003_01_000001 Container Transitioned from ACQUIRED to RUNNING
2022-01-19 20:06:39,463 INFO SecurityLogger.org.apache.hadoop.ipc.Server: Auth successful for appattempt_1642592462740_0003_000001 (auth:SIMPLE)
2022-01-19 20:06:39,484 INFO org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService: AM registration appattempt_1642592462740_0003_000001

2022-01-19 20:06:39,484 INFO org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger: 
	USER=bigdata	IP=192.168.51.102	OPERATION=Register App Master	TARGET=ApplicationMasterService	RESULT=SUCCESS	APPID=application_1642592462740_0003	APPATTEMPTID=appattempt_1642592462740_0003_000001
2022-01-19 20:06:39,485 INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl: 
	appattempt_1642592462740_0003_000001 State change from LAUNCHED to RUNNING
2022-01-19 20:06:39,485 INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl: application_1642592462740_0003 State change from ACCEPTED to RUNNING


WebAppProxyServlet

// 虽然有这个报错, 但这并不 影响查询结构, 没问题; 



WebAppProxyServlet.doGet(){
	boolean userApproved = Boolean.valueOf(userApprovedParamS);
	
}




doGet:370, WebAppProxyServlet (org.apache.hadoop.yarn.server.webproxy)
service:707, HttpServlet (javax.servlet.http)
service:820, HttpServlet (javax.servlet.http)
handle:511, ServletHolder (org.mortbay.jetty.servlet)
doFilter:1221, ServletHandler$CachedChain (org.mortbay.jetty.servlet)
doFilter:66, FilterChainInvocation (com.google.inject.servlet)
doFilter:900, ServletContainer (com.sun.jersey.spi.container.servlet)
doFilter:834, ServletContainer (com.sun.jersey.spi.container.servlet)
doFilter:142, RMWebAppFilter (org.apache.hadoop.yarn.server.resourcemanager.webapp)
doFilter:795, ServletContainer (com.sun.jersey.spi.container.servlet)
doFilter:163, FilterDefinition (com.google.inject.servlet)
doFilter:58, FilterChainInvocation (com.google.inject.servlet)
dispatch:118, ManagedFilterPipeline (com.google.inject.servlet)
doFilter:113, GuiceFilter (com.google.inject.servlet)
doFilter:1212, ServletHandler$CachedChain (org.mortbay.jetty.servlet)
doFilter:109, StaticUserWebFilter$StaticUserFilter (org.apache.hadoop.http.lib)
doFilter:1212, ServletHandler$CachedChain (org.mortbay.jetty.servlet)
doFilter:595, AuthenticationFilter (org.apache.hadoop.security.authentication.server)
doFilter:291, DelegationTokenAuthenticationFilter (org.apache.hadoop.security.token.delegation.web)
doFilter:554, AuthenticationFilter (org.apache.hadoop.security.authentication.server)
doFilter:82, RMAuthenticationFilter (org.apache.hadoop.yarn.server.security.http)
doFilter:1212, ServletHandler$CachedChain (org.mortbay.jetty.servlet)
doFilter:1243, HttpServer2$QuotingInputFilter (org.apache.hadoop.http)
doFilter:1212, ServletHandler$CachedChain (org.mortbay.jetty.servlet)
doFilter:45, NoCacheFilter (org.apache.hadoop.http)
doFilter:1212, ServletHandler$CachedChain (org.mortbay.jetty.servlet)
doFilter:45, NoCacheFilter (org.apache.hadoop.http)
doFilter:1212, ServletHandler$CachedChain (org.mortbay.jetty.servlet)
handle:399, ServletHandler (org.mortbay.jetty.servlet)
handle:216, SecurityHandler (org.mortbay.jetty.security)
handle:182, SessionHandler (org.mortbay.jetty.servlet)
handle:766, ContextHandler (org.mortbay.jetty.handler)
handle:450, WebAppContext (org.mortbay.jetty.webapp)
handle:230, ContextHandlerCollection (org.mortbay.jetty.handler)
handle:152, HandlerWrapper (org.mortbay.jetty.handler)
handle:326, Server (org.mortbay.jetty)
handleRequest:542, HttpConnection (org.mortbay.jetty)
headerComplete:928, HttpConnection$RequestHandler (org.mortbay.jetty)
parseNext:549, HttpParser (org.mortbay.jetty)
parseAvailable:212, HttpParser (org.mortbay.jetty)
handle:404, HttpConnection (org.mortbay.jetty)
run:410, SelectChannelEndPoint (org.mortbay.io.nio)
run:582, QueuedThreadPool$PoolThread (org.mortbay.thread)

