

// 创建本地或cluster的FileSystem ?
FileSystem.get(conf){
    URI url=getDefaultUri(conf);{
        return URI.create(fixName(conf.get(FS_DEFAULT_NAME_KEY, DEFAULT_FS)));{
            conf.get(FS_DEFAULT_NAME_KEY, DEFAULT_FS);{
                String[] names = handleDeprecation(deprecationContext.get(), name);
                for(String n : names) {
                    //n =="fs.defaultFS",对应getProps()中的props中的该key的值,也是默认值: file:/// ; defaultValue也是file:///
                    result = substituteVars(getProps().getProperty(n, defaultValue));
                }
                return result;//所以,当Configuration为空时,返回的fs.defaultFS即为: file:///
            }
        }
    }
        
    return get(url, conf);{
        String scheme = uri.getScheme();
        return CACHE.get(uri, conf);{//FileSystem.CACHE.get
            Key key = new Key(uri, conf);
            return getInternal(uri, conf, key);{
                FileSystem fs = map.get(key);
                if (fs != null) return fs; // 若该Key(Url对应的Key)有缓存的FS实例,直接返回;单例模式;
                
                fs = createFileSystem(uri, conf);{
                    Tracer tracer = FsTracer.get(conf);
                    Class<?> clazz = getFileSystemClass(uri.getScheme(), conf);{
                        if (!FILE_SYSTEMS_LOADED) {
                            /* 加载了各种FileSystem:
                            *   "file" -> "class org.apache.hadoop.fs.LocalFileSystem"
                            *   "hdfs" -> "class org.apache.hadoop.hdfs.DistributedFileSystem"
                            *   "s3" -> "class org.apache.hadoop.fs.s3.S3FileSystem"
                            *   "ftp" -> "class org.apache.hadoop.fs.ftp.FTPFileSystem"
                            *   "webhdfs" -> "class org.apache.hadoop.hdfs.web.WebHdfsFileSystem"
                            */
                            loadFileSystems();{ //将所有FileSystem的实现类,加载到SERVICE_FILE_SYSTEMS: Map<String,Class> 
                                ServiceLoader<FileSystem> serviceLoader = ServiceLoader.load(FileSystem.class);
                                Iterator<FileSystem> it = serviceLoader.iterator();
                                while (it.hasNext()) { //将所有FileSystem的实现类,加载到SERVICE_FILE_SYSTEMS: Map<String,Class> 
                                    fs = it.next();
                                    SERVICE_FILE_SYSTEMS.put(fs.getScheme(), fs.getClass());
                                }
                            }
                        }
                        if (clazz == null) {
                            clazz = SERVICE_FILE_SYSTEMS.get(scheme);//对于file这个scheme, 即返回 class org.apache.hadoop.fs.LocalFileSystem 对象;
                        }
                        return clazz;
                    }
                }
                
            }
        }
    }
}


