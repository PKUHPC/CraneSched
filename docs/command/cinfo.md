
# cinfo 查看节点与分区状态 #

**cinfo可查询各分区节点的队列资源信息。**

查看分区节点状态：
~~~bash
cinfo
~~~

**cinfo运行结果展示**

![cinfo](../images/cinfo.png)


#### 主要输出项 ####

- **PARTITION**：分区名
- **AVAIL**： 分区状态
  - **idel**： 空闲
  - **mix**： 节点部分核心可以使用
  - **alloc**： 节点已被占用
  - **down**： 节点不可用
- **NODES**：节点数
- **NODELIST**： 节点列表



#### 主要参数 ####

- **--help**: 显示帮助
