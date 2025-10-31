# cinfo 查看节点与分区状态

**cinfo可查询各分区节点的队列资源信息。**

查看分区节点状态：
~~~bash
cinfo
~~~

**cinfo运行结果展示**

![cinfo](../../images/cinfo/cinfo_running.png)

## 主要输出项

- **PARTITION**：分区名
- **AVAIL**： 分区状态
  - up: 可用
  - down: 不可用
- **NODES**：节点数
- **STATE**： 节点状态
  - **idle**： 空闲
  - mix： 节点部分核心可以使用
  - alloc： 节点已被占用
  - down： 节点不可用
- **NODELIST**： 节点列表

## 主要参数

- **-h/--help**: 显示帮助
- **-C/--config string**：配置文件路径（默认为"/etc/crane/config.yaml"）
- **-d/--dead**：只显示无响应节点
- **-i/--iterate uint**：指定间隔秒数刷新查询结果。如 `-i=3` 表示每隔三秒输出一次查询结果
- **--json**：JSON格式输出命令执行结果
- **-n/--nodes string**：显示指定节点信息，多个节点用逗号隔开。例：`cinfo -n crane01,crane02`
- **-N/--noheader**：输出隐藏表头
- **-p/--partition string**：显示指定分区信息，多个分区用逗号隔开。例：`cinfo -p CPU,GPU`
- **-r/--responding**：只显示有响应节点
- **-t/--states string**：仅显示具有指定状态的节点信息。状态可以为（不区分大小写）: IDLE, MIX, ALLOC和DOWN。示例：
  - `-t idle,mix`
  - `-t=alloc`
- **-v/--version**：查询版本号

### 格式说明符 (-o/--format)

`--format` 选项允许自定义输出格式。字段由百分号（%）后接一个字符或字符串标识。在 % 和格式字符或字符串之间使用点（.）和数字可以指定字段的最小宽度。

**支持的格式标识符**（不区分大小写）：

| 标识符 | 完整名称 | 描述 |
|--------|----------|------|
| %p | Partition | 显示当前环境中的所有分区 |
| %a | Avail | 显示分区的可用状态 |
| %n | Nodes | 显示分区节点的数量 |
| %s | State | 显示分区节点的状态 |
| %l | NodeList | 显示分区中的所有节点列表 |

每个格式标识符或字符串都可以用宽度说明符修改（例如，"%.5j"）。如果指定了宽度，字段将被格式化为至少达到该宽度。如果格式无效或无法识别，程序将报错并终止。

**格式示例：**
```bash
# 显示分区名（最小宽度5）、状态（最小宽度6）和节点状态
cinfo --format "%.5partition %.6a %s"
```

## 使用示例

**显示所有分区和节点：**
```bash
cinfo
```
![cinfo](../../images/cinfo/cinfo_running.png)

**显示帮助：**
```bash
cinfo -h
```
![cinfo](../../images/cinfo/cinfo_h.png)

**隐藏表头：**
```bash
cinfo -N
```
![cinfo](../../images/cinfo/cinfo_n.png)

**仅显示无响应节点：**
```bash
cinfo -d
```
![cinfo](../../images/cinfo/cinfo_d.png)

**每3秒自动刷新：**
```bash
cinfo -i 3
```
![cinfo](../../images/cinfo/cinfo_i3.png)

**显示特定节点：**
```bash
cinfo -n crane01,crane02,crane03
```
![cinfo](../../images/cinfo/cinfo_n123.png)

**显示特定分区：**
```bash
cinfo -p GPU,CPU
```
![cinfo](../../images/cinfo/cinfo_p.png)

**仅显示有响应节点：**
```bash
cinfo -r
```
![cinfo](../../images/cinfo/cinfo_r.png)

**按节点状态过滤：**
```bash
cinfo -t IDLE
```
![cinfo](../../images/cinfo/cinfo_t.png)

**显示版本：**
```bash
cinfo -v
```
![cinfo](../../images/cinfo/cinfo_v.png)

**JSON输出：**
```bash
cinfo --json
```

## 节点状态过滤

`-t/--states` 选项允许按节点状态过滤。可以将多个状态指定为逗号分隔的列表：

```bash
# 显示空闲和混合状态的节点
cinfo -t idle,mix

# 仅显示已分配的节点
cinfo -t alloc

# 仅显示宕机的节点
cinfo -t down
```

**注意：** `-t/--states`、`-r/--responding` 和 `-d/--dead` 选项是互斥的。一次只能指定一个。

## 相关命令

- [cqueue](cqueue.zh.md) - 查看作业队列
- [ccontrol](ccontrol.zh.md) - 控制集群资源
- [cacctmgr](cacctmgr.zh.md) - 管理账户和分区
