# ccancel 取消作业 #

**ccancel可以终止正在运行或者在排队中的作业。**

取消作业号为280的作业：

~~~bash
ccancel 280
~~~

**ccancel运行结果展示**

![ccancel](../images/ccancel.png)


取消作业之后，如果被分配节点上没有用户的其他作业，作业调度系统会终止用户在所分配节点的所有进程，并取消用户在所分配节点上的ssh权限。