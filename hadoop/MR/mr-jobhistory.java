

webapp.Dispatcher.service(){//重写了HttpServlet.service()方法
    Router.Dest dest = router.resolve(method, pathInfo);
    Controller controller = injector.getInstance(dest.controllerClass);{
        return new HsController(){ //反射方式创建HSController的实例;
            super(app, conf, ctx, "History");{//new AppController()
                super(ctx);
                this.app = app;
                set(APP_ID, app.context.getApplicationID().toString());//设置AppId;
                //设置rm.web
                set(RM_WEB, JOINER.join(MRWebAppUtil.getYARNWebappScheme(),
                        WebAppUtils.getResolvedRemoteRMWebAppURLWithoutScheme(conf,MRWebAppUtil.getYARNHttpPolicy())));
            }
        }
    }
    
    dest.action.invoke(controller, (Object[]) null);{//HsController.job() -> AppController.job()
        requireJob();
        render(jobPage());
    }
}