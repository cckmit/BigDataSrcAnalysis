


WebServerTask.getLoginService(Configuration conf, String mode){
	switch (loginModule) {
		case FILE:
				String realm = conf.get(DIGEST_REALM_KEY, mode + REALM_POSIX_DEFAULT);
				File realmFile = new File(runtimeInfo.getConfigDir(), realm + ".properties").getAbsoluteFile();
				validateRealmFile(realmFile);//拼接成 $SDC_HOME/etc/form-realm.properties文件路径
				loginService = new SdcHashLoginService(realm, realmFile.getAbsolutePath());{//SdcHashLoginService的构造函数,是否可以在这里做文章;
					super(name, config);{//new HashLoginService()
						super()://创建父类AbstractLoginService的无参构造函数;
						setName(name);
						setConfig(config);
					}
				}
				break;
	}
}


loadUserInfo:162, HashLoginService (org.eclipse.jetty.security)
login:168, AbstractLoginService (org.eclipse.jetty.security)



SecurityHandler.handle(){
	if (authentication==null || authentication==Authentication.NOT_CHECKED)
        authentication=authenticator==null?Authentication.UNAUTHENTICATED:authenticator.validateRequest(request, response, isAuthMandatory);{//ActivationAuthenticator.validateRequest()
			Authentication authentication = authenticator.validateRequest(request, response, mandatory);{//ProxyAuthenticator.validateRequest()
				return authenticator.validateRequest(req, res, mandatory);{//FormAuthenticator.validateRequest()
					mandatory|=isJSecurityCheck(uri);
					if (isJSecurityCheck(uri)){
						final String username = request.getParameter(__J_USERNAME);
						final String password = request.getParameter(__J_PASSWORD);
						UserIdentity user = login(username, password, request);{//FormAuthenticator.login
							UserIdentity user = super.login(username,password,request);{//LoginAuthenticator.login()
								UserIdentity user = _loginService.login(username,password, request);{//AbstractLoginService.login()
									if (username == null) return null;
									UserPrincipal userPrincipal = loadUserInfo(username);{//HashLoginService.loadUserInfo()
										UserIdentity id = _propertyUserStore.getUserIdentity(userName);
										if (id != null){
											return (UserPrincipal)id.getUserPrincipal();
										}
									}
									boolean canLogin = userPrincipal != null && userPrincipal.authenticate(credentials);{//AbstractLoginService.authenticate(Object credentials)
										return _credential!=null && _credential.check(credentials);{//Credential.$.MD5.check(credentials)
											byte[] digest = null;
											if (credentials instanceof char[])//若是字节数组,先转换String;
												credentials = new String((char[])credentials);
											
											if (credentials instanceof Password || credentials instanceof String){//若为Password或字符串; 前端传入即为字符串,进入这里;
												synchronized (__md5Lock){
													if (__md == null)//若_md为空,则新建一个MD5
														__md = MessageDigest.getInstance("MD5");//若要获取SHA加密算法,输入"SHA"即可;
													__md.reset();{MessageDigest.reset()//更新引擎,可能之前是其他算法引擎,如MD2,MD5,SHA等;
														engineReset();
														state = INITIAL;
													}
													
													__md.update(credentials.toString().getBytes(StandardCharsets.ISO_8859_1));{//MessageDigest.update()
														engineUpdate(input, 0, input.length);{//MessageDigest.$.Delegate.engineUpdate()
															digestSpi.engineUpdate(input, offset, len);{//sun.security.MD5 -> 父类DigestBase.engineUpdate()
																
															}
														}
														state = IN_PROGRESS;
													}
													digest = __md.digest();{//MessageDigest.$Delegate.
														return digestSpi.engineDigest();{//DigestBase.engineDigest()
															this.engineDigest(var1, 0, var1.length);{//DigestBase.engineDigest()
																else if (var2 >= 0 && var3 >= 0 && var2 <= var1.length - var3) {
																	this.implDigest(var1, var2);{//由子类实现: MD5.implDigest()
																		MD5.implDigest();
																	}
																}
															}
														}
													}
												}
												if (digest == null || digest.length != _digest.length)
													return false;
												boolean digestMismatch = false;
												for (int i = 0; i < digest.length; i++)
													digestMismatch |= (digest[i] != _digest[i]);
												return !digestMismatch;
											} else if (credentials instanceof MD5){
												return equals((MD5)credentials);
											} else if (credentials instanceof Credential){
												return ((Credential)credentials).check(this);
											} else {
												LOG.warn("Can't check " + credentials.getClass() + " against MD5");
												return false;
											}
										}
									}
									if (canLogin){
										String[] roles = loadRoleInfo(userPrincipal);
										return _identityService.newUserIdentity(subject,userPrincipal,roles);
									}
								}
							}
							if (user!=null){
								HttpSession session = ((HttpServletRequest)request).getSession(true);
								Authentication cached=new SessionAuthentication(getAuthMethod(),user,password);
							}
						}
						LOG.debug("jsecuritycheck {} {}",username,user);
					}
				}
			}
		}

}








