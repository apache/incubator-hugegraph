## 启用无PD模式

- hgstore设置yml文件 **app.fake-pd: true**
- hugegraph设置properties文件 **pd.fake=true**，设置**hstore.peers=** 指向hgstore的grpc地址，多个地址逗号分割

````
    backend=hstore
    serializer=binary
    pd.peers=localhost:9000
    pd.fake=true
    hstore.peers=127.0.0.1:9081,127.0.0.1:9082,127.0.0.1:9083
    store=hugegraph
````

###FakePD集群配置
如果启用fakePD模式的集群部署，需要在hgstore的yml文件增加fake-pd配置

+ **store-list:** store集群列表
+ **peers-list:** raft集群列表

````
    fake-pd:
            store-list: 127.0.0.1:9080,127.0.0.1:9081,127.0.0.1:9082
            peers-list: 127.0.0.1:8080,127.0.0.1:8081,127.0.0.1:8082
````

###本地打包

项目根目录下执行：

````
./mvnw -Dmaven.test.skip=true package
````

文件保存在项目根目录下的 dist 文件夹中。

###本地单测

- 在跑单测之前需要启动一个pd和store服务
- 所以的单测统一在hg-store-test模块中撰写，client、common、core等与项目的模块一一对应，相应模块下的单测写到对应的文件夹下
- 保持目录结构与项目模块对应，文件命名必须包含"Test"字样，把编写好的类加到对应的SuiteTest类中，由Suite调度执行
- 单测可以用编译工具单个执行，也可以到代码的根目录下执行：mvn verify -Pjacoco
- 代码覆盖率查看，需要先执行：mvn verify -Pjacoco 后，在到根目录的target->site->jacoco->index.html