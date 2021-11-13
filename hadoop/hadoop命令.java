


RunJar.main(String[] args) {
    
    new RunJar().run(args);{//参数第一个是 JarFile ;
        String usage = "RunJar jarFile [mainClass] args...";
        if (args.length < 1) {
          System.exit(-1);
        }
        int firstArg = 0;
        String fileName = args[firstArg++];
        File file = new File(fileName);
        if (!file.exists() || !file.isFile()) {
          System.err.println("Not a valid JAR: " + file.getCanonicalPath());
          System.exit(-1);
        }
        JarFile jarFile =new JarFile(fileName);
        
        // 确定mainClass, 优先从Jar中的Mainifest获取mainClass,其从从第二个参数
        String mainClassName = null;
        Manifest manifest = jarFile.getManifest();
        if (manifest != null) {
          mainClassName = manifest.getMainAttributes().getValue("Main-Class");
        }
        jarFile.close();

        if (mainClassName == null) { //当Jar.Mainifest 没有时,尝试从第二个args参数解析;
          if (args.length < 2) {
            System.err.println(usage);
            System.exit(-1);
          }
          mainClassName = args[firstArg++];
        }
        mainClassName = mainClassName.replaceAll("/", ".");

        File workDir = File.createTempFile("hadoop-unjar", "", new File(System.getProperty("java.io.tmpdir"))); //Java应用临时目录;
        if (!workDir.delete()) {
          System.err.println("Delete failed for " + workDir);
          System.exit(-1);
        }
        ensureDirectory(workDir);
        ShutdownHookManager.get().addShutdownHook( //钩子函数,保证wordDir能被完成删除;
          new Runnable() {
            @Override
            public void run() {FileUtil.fullyDelete(workDir);}
          }, SHUTDOWN_HOOK_PRIORITY);
        
        
        unJar(file, workDir);
        
        ClassLoader loader = createClassLoader(file, workDir);
        Thread.currentThread().setContextClassLoader(loader);
        
        // 定义mainClass 和 Dirver的main()方法, args参数;
        Class<?> mainClass = Class.forName(mainClassName, true, loader);
        Method main = mainClass.getMethod("main", new Class[] {
          Array.newInstance(String.class, 0).getClass()
        });
        String[] newArgs = Arrays.asList(args).subList(firstArg, args.length).toArray(new String[0]);
        // 执行用户定义的UserDriver.main()方法
        main.invoke(null, new Object[] { newArgs });
        
    }
}

