# ccancel - 取消作业

**ccancel 终止队列中正在运行或挂起的作业，或作业内的单个步骤。**

您可以通过作业ID取消作业，使用`jobid.stepid`格式取消单个步骤，或使用过滤条件一次取消多个作业。

## 命令语法

```bash
ccancel [job_id[.step_id][,job_id[.step_id]...]] [选项]
```

## 命令行选项

### 作业选择
- **job_id[.step_id][,job_id[.step_id]...]**: 要取消的作业ID或作业步ID（逗号分隔的列表）。
  - 作业格式：`<job_id>` 或 `<job_id>,<job_id>,<job_id>...`
  - 作业步格式：`<job_id>.<step_id>` 或 `<job_id>.<step_id>,<job_id>.<step_id>...`
  - 混合格式：`<job_id>,<job_id>.<step_id>,<job_id>.<step_id>...`

### 过滤选项
- **-n, --name string**: 取消具有指定作业名的作业
- **-u, --user string**: 取消指定用户提交的作业
- **-A, --account string**: 取消指定账户下的作业
- **-p, --partition string**: 取消指定分区中的作业
- **-t, --state string**: 取消指定状态的作业。有效状态：`PENDING`（P）、`RUNNING`（R）、`ALL`（不区分大小写）
- **-w, --nodes strings**: 取消在指定节点上运行的作业（逗号分隔的列表）

### 输出选项
- **--json**: 以 JSON 格式输出

### 其他选项
- **-C, --config string**: 配置文件路径（默认：`/etc/crane/config.yaml`）
- **-h, --help**: 显示帮助信息
- **-v, --version**: 显示版本号

## 过滤规则

!!! important
    必须提供至少一个条件：作业 ID 或至少一个过滤选项。

使用多个过滤器时，将取消匹配**所有**指定条件的作业（AND 逻辑）。

## 使用示例

### 按作业 ID 取消

取消单个作业：
```bash
ccancel 30686
```

**运行结果：**

```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME     USER        ACCOUNT    STATUS  TYPE   TIME     TIMELIMIT  NODES  NODELIST/REASON
30686  CPU        Test_Job cranetest   CraneTest  Running Batch  00:00:19  00:30:01   2      crane[02-03]
30685  CPU        Test_Job cranetest   CraneTest  Running Batch  00:00:19  00:25:25   2      crane[02-03]
```
```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME     USER        ACCOUNT    STATUS  TYPE   TIME     TIMELIMIT  NODES  NODELIST/REASON
30685  CPU        Test_Job cranetest   CraneTest  Running Batch  00:00:37  00:25:25   2      crane[02-03]
```
取消多个作业：
```bash
ccancel 30686,30687,30688
```

### 按作业名取消

取消所有名为 "test1" 的作业：
```bash
ccancel -n test1
```

**运行结果：**
```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME   USER        ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT  NODES  NODELIST/REASON
30743  CPU        test5  cranetest   CraneTest  Pending  Batch  -         00:30:01   1      Priority
30739  CPU        test1  cranetest   CraneTest  Running  Batch  00:05:38  00:30:01   1      crane03
30742  CPU        test4  cranetest   CraneTest  Running  Batch  00:02:28  00:30:01   1      crane02
30740  CPU        test2  cranetest   CraneTest  Running  Batch  00:05:34  00:30:01   1      crane02
30741  CPU        test3  cranetest   CraneTest  Running  Batch  00:05:33  00:30:01   1      crane03
```
```text
[cranetest@crane01 ~]$ ccancel -n test1
Job 30739 cancelled successfully.
```
```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME   USER        ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT  NODES  NODELIST/REASON
30742  CPU        test4  cranetest   CraneTest  Running  Batch  00:03:26  00:30:01   1      crane02
30743  CPU        test5  cranetest   CraneTest  Running  Batch  00:00:04  00:30:01   1      crane03
30740  CPU        test2  cranetest   CraneTest  Running  Batch  00:06:32  00:30:01   1      crane02
30741  CPU        test3  cranetest   CraneTest  Running  Batch  00:06:31  00:30:01   1      crane03
```
### 按分区取消

取消 GPU 分区中的所有作业：
```bash
ccancel -p GPU
```

**运行结果：**

```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME     USER        ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT  NODES  NODELIST/REASON
30766  GPU        Test_Job cranetest   CraneTest  Pending  Batch  -         00:30:01   1      Priority
30767  GPU        Test_Job cranetest   CraneTest  Pending  Batch  -         00:30:01   1      Priority
30764  CPU        Test_Job cranetest   CraneTest  Running  Batch  00:00:12  00:30:01   1      crane03
30765  GPU        Test_Job cranetest   CraneTest  Running  Batch  00:00:03  00:30:01   1      crane03
30763  CPU        Test_Job cranetest   CraneTest  Running  Batch  00:00:13  00:30:01   1      crane02
```
```text
[cranetest@crane01 ~]$ ccancel -p GPU
Job 30766,30767,30765 cancelled successfully.
```
```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME     USER        ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT  NODES  NODELIST/REASON
30764  CPU        Test_Job cranetest   CraneTest  Running  Batch  00:00:50  00:30:01   1      crane03
30763  CPU        Test_Job cranetest   CraneTest  Running  Batch  00:00:51  00:30:01   1      crane02
```
### 按节点取消

取消在 crane02 上运行的所有作业：
```bash
ccancel -w crane02
```

```text
[cranetest@crane01 ~]$ ccancel -w crane02
Job 30683,30684 cancelled successfully.
```

### 按状态取消

取消所有挂起的作业：
```bash
ccancel -t Pending
```

```text
[cranetest@crane01 ~]$ ccancel -t Pending
Job 30687 cancelled successfully.
```

取消所有正在运行的作业：
```bash
ccancel -t Running
```

取消所有作业（挂起和运行）：
```bash
ccancel -t All
```

### 按账户取消

取消 PKU 账户下的所有作业：
```bash
ccancel -A PKU
```
```text
[root@cranetest-rocky01 zhouhao]# ccancel -A ROOT --json
{"cancelled_tasks":[128088,128089,128090,128091,128092,128093,128094,128095,128096,128097,128098,128099,128100,128101,128102,128103,128104,128105,128106,128107,128108,128109,128110,128111,128112,128113,128114,128115,128116,128117,128118,128119,128120,128121,128122,128123,128124,128125,128126,128127,128128,128129,128130,128131,128132,128133,128134,128135,128136,128137,128138,128139,128140,128141,128142,128143,128144,128145,128146,128147,128148,128149,128150,128151,128152,128153,128154,128155,128156,128157,128158,128159,128160,128161,128162,128163,128164,128165,128166,128167,128168,128169,128170,128171,128172,128173,128174,128175,128176,128177],"not_cancelled_tasks":[],"not_cancelled_reasons":{}}
```

### 按用户取消

取消用户 ROOT 提交的所有作业：
```bash
ccancel -u ROOT
```

```text
[root@cranetest-rocky01 zhouhao]# ccancel -u root
1279986 1279987 1279988 1279989 1279990 1279991 1279992 1279993 1279994 1279995 1279996 1279997 1279998 1279999 1280000 1280001 1280002 1280003 1280004 1280005
1280006 1280007 1280008 1280009 1280010 1280011 1280012 1280013 1280014 1280015 1280016 1280017 1280018 1280019 1280020 1280021 1280022 1280023 1280024 1280025
1280026 1280027 1280028 1280029 1280030 1280031 1280032 1280033 1280034 1280035 1280036 1280037 1280038 1280039 1280040 1280041 1280042 1280043 1280044 1280045
1280046 1280047 1280048 1280049 1280050 1280051 1280052 1280053 1280054 1280055 1280056 1280057 1280058 1280059 1280060 1280061 1280062 1280063 1280064 1280065
1280066 1280067 1280068 1280069 1280070 1280071 1280072 1280073 1280074 1280075 1280076 1280077 1280078 1280079 1280080 1280081 1280082 1280083 1280084 1280085
1280086 1280087 1280088 1280089 1280090 1280091 1280092 1280093 1280094 1280095 1280096 1280097 1280098 1280099 1280100 1280101 1280102 1280103 1280104 1280105
1280106 1280107 1280108 1280109 1280110 1280111 1280112 1280113 1280114 1280115 1280116 1280117 1280118 1280119 1280120 1280121 1280122 1280123 1280124 1280125
1280126 1280127 1280128 1280129 1280130 1280131 1280132 1280133 1280134 1280135 1280136 1280137 1280138 1280139 1280140 1280141 1280142 1280143 1280144 1280145
1280146 1280147 1280148 1280149 1280150 1280151 1280152 1280153 1280154 1280155 1280156 1280157 1280158 1280159 1280160 1280161 1280162 1280163 1280164 1280165
1280166 1280167 1280168 1280169 1280170 1280171 1280172 1280173 1280174 1280175 1280176 1280177 1280178 1280179 1280180 1280181 1280182 1280183 1280184 1280185
1280186 1280187 1280188 1280189 1280190 1280191 1280192 1280193 1280194 1280195 1280196 1280197 1280198 1280199 1280200 1280201 1280202 1280203 1280204 1280205
1280206 1280207 1280208 1280209 1280210 1280211 1280212 1280213 1280214 1280215 1280216 1280217 1280218 1280219 1280220 1280221 1280222 1280223 1280224 1280225
1280226 1280227 1280228 1280229 1280230 1280231 1280232 1280233 1280234 1280235 1280236 1280237 1280238 1280239 1280240 1280241 1280242 1280243 1280244 1280245
1280246 1280247 1280248 1280249 1280250 1280251 1280252 1280253 1280254 1280255 1280256 1280257 1280258 1280259 1280260 1280261 1280262 1280263 1280264 1280265
1280266 1280267 1280268 1280269 1280270 1280271 1280272 1280273 1280274 1280275 1280276 1280277 1280278 1280279 1280280 1280281 1280282 1280283 1280284 1280285
1280286 1280287 1280288 1280289 1280290 1280291 1280292 1280293 1280294 1280295 1280296 1280297 1280298 1280299 1280300 1280301 1280302 1280303 1280304 1280305
1280306 1280307 1280308 1280309 1280310 1280311 1280312 1280313 1280314 1280315 1280316 1280317 1280318 1280319 1280320 1280321 1280322 1280323 1280324 1280325
1280326 1280327 1280328 1280329 1280330 1280331 1280332 1280333 1280334 1280335 1280336 1280337 1280338 1280339 1280340 1280341 1280342 1280343 1280344 1280345
1280346 1280347 1280348 1280349 1280350 1280351 1280352 1280353
```

### 组合过滤器

取消 CPU 分区中所有挂起的作业：
```bash
ccancel -t Pending -p CPU
```

取消用户 alice 在 GPU 分区中的所有作业：
```bash
ccancel -u alice -p GPU
```

取消特定节点上正在运行的作业：
```bash
ccancel -t Running -w crane01,crane02
```

### JSON 输出

以 JSON 格式获取取消结果：
```bash
ccancel 30686 --json
```

使用过滤器和 JSON 输出取消：
```bash
ccancel -p GPU -t Pending --json
```

## 示例概览

```text
[cranetest@crane01 ~]$ cqueue
JOBID  PARTITION  NAME     USER        ACCOUNT    STATUS   TYPE   TIME      TIMELIMIT  NODES  NODELIST/REASON
30685  CPU        Test_Job cranetest   CraneTest  Pending  Batch  -         00:25:25   2      Priority
30686  CPU        Test_Job cranetest   CraneTest  Pending  Batch  -         00:30:01   2      Priority
30687  CPU        Test_Job cranetest   CraneTest  Pending  Batch  -         00:30:01   2      Priority
30683  CPU        Test_Job cranetest   CraneTest  Running  Batch  00:07:43  00:30:01   2      crane[02-03]
30684  CPU        Test_Job cranetest   CraneTest  Running  Batch  00:07:41  00:30:01   2      crane[02-03]
```

## 取消作业步骤

CraneSched支持使用步骤ID格式`jobid.stepid`取消作业内的单个步骤。这允许您终止特定步骤，同时保持父作业和其他步骤继续运行。

### 步骤取消语法

**取消特定步骤：**
```bash
ccancel 123.1      # 取消作业123的步骤1
```

**取消多个步骤：**
```bash
ccancel 123.1,123.2,456.3
```

**取消整个作业（所有步骤）：**
```bash
ccancel 123        # 取消作业123及其所有步骤
```

### 步骤取消行为

取消步骤时：

- **单个步骤取消**：取消特定步骤（`jobid.stepid`）仅影响该步骤
  - 父作业继续运行
  - 同一作业中的其他步骤不受影响
  - 分配给步骤的资源释放回父作业

- **完全作业取消**：取消作业而不指定步骤ID（`jobid`）会取消：
  - 作业内的所有步骤
  - 父作业本身
  - 所有分配的资源被释放

### 步骤取消示例

**仅取消作业100的步骤2：**
```bash
ccancel 100.2
```

**取消作业100的步骤1和2（步骤3如果存在则继续）：**
```bash
ccancel 100.1,100.2
```

**混合取消 - 整个作业100和作业200的步骤3：**
```bash
ccancel 100,200.3
```

**取消前查询步骤：**
```bash
# 查看作业的所有步骤
cqueue --step -j 123

# 根据状态取消特定步骤
ccancel 123.2,123.4
```

### 步骤取消权限

- **普通用户**：只能取消属于自己作业的步骤
- **协调员**：可以取消其账户下作业的步骤
- **操作员/管理员**：可以取消系统中的任何步骤

## 取消后的行为

作业取消后：

1. **进程终止**：如果分配的节点上没有该用户的其他作业，作业调度系统将终止这些节点上的所有用户进程

2. **撤销 SSH 访问**：将撤销用户对分配节点的 SSH 访问权限

3. **资源释放**：所有分配的资源（CPU、内存、GPU）立即释放，可供其他作业使用

4. **作业状态更新**：作业状态在作业历史记录中变为 `CANCELLED`

## 权限要求

- **普通用户**：只能取消自己的作业
- **调度员**：可以取消其账户内的作业
- **操作员/管理员**：可以取消系统中的任何作业

## 重要说明

1. **立即生效**：作业取消立即生效。默认情况下，运行中的作业会立即终止，没有宽限期

2. **多个作业**：您可以使用逗号分隔的作业 ID 或过滤条件一次取消多个作业

3. **无确认提示**：没有确认提示。命令执行后立即取消作业

4. **状态过滤**：使用 `-t` 针对特定作业状态，以避免意外取消处于非预期状态的作业

5. **作业/步骤 ID 格式**：ID必须遵循以下格式，不能有空格：
   - 作业：`<job_id>` 或 `<job_id>,<job_id>,<job_id>...`
   - 步骤：`<job_id>.<step_id>` 或 `<job_id>.<step_id>,<job_id>.<step_id>...`
   - 混合：`<job_id>,<job_id>.<step_id>...`

## 错误处理

常见错误：

- **无效的作业 ID**：如果作业 ID 不存在或您没有取消权限，将返回错误
- **无匹配作业**：如果过滤条件不匹配任何作业，返回成功但取消 0 个作业
- **无效状态**：状态必须是以下之一：PENDING、RUNNING、ALL（不区分大小写）

## 另请参阅

- [cbatch](cbatch.md) - 提交批处理作业
- [crun](crun.md) - 运行交互式任务
- [calloc](calloc.md) - 分配交互式资源
- [cqueue](cqueue.md) - 查看作业队列
- [cacct](cacct.md) - 查看作业统计信息
- [ccontrol](ccontrol.md) - 控制作业和系统资源
