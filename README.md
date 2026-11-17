# 制品元数据熔断网关

部署系统读取制品元数据时，上游故障会引发密集重试。本项目是一个 **FastAPI + SQLite
的双 worker 熔断网关**：按制品名把 `GET` 查询转发给本地目录服务，慢查询不会阻塞其他
查询，两个 worker 进程共享同一份熔断状态，重启后状态仍然保留。

此外，网关与示例目录都带有**共享容量保护**：普通部署请求挤满目录时，值班工程师的
紧急回滚查询仍能拿到保留席位；容量不足的请求立即得到独立的忙碌响应，不访问目录、
不影响熔断。

打开 <http://localhost:8000/> 即可使用网页：选择**普通 / 紧急**级别并发查询、查看逐条
结果、网关与目录的当前/峰值在途数、熔断状态，并切换示例上游行为（正常 / 延迟 / 503）。

## 快速启动

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000/
```

需要 Docker 与 Docker Compose v2。Compose 会构建两个服务：

| 服务 | 说明 | 端口 |
| --- | --- | --- |
| `upstream` | 示例本地目录服务，提供 `/artifacts/{name}`、模式控制与访问计数 | 仅集群内 9000 |
| `gateway` | 熔断网关，`uvicorn --workers 2`，SQLite 位于命名卷 `breaker-data` | 宿主机 8000 |

`gateway` 使用命名卷挂载 `/data`，容器重启或重建后熔断状态仍在。

### 不使用 Docker 的本地运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export UPSTREAM_URL=http://127.0.0.1:9000 GATEWAY_DB=./data/breaker.db
python -m uvicorn upstream:app --port 9000 &
python -m uvicorn app:app --port 8000 --workers 2
```

## 熔断语义

状态按**制品名**独立维护，存放在 SQLite（WAL、写事务 `BEGIN IMMEDIATE`），因此对两个
worker 进程共享，并对重启持久。

* **CLOSED**：转发请求。失败（连接失败 / 超时 / 5xx）按**完成顺序**连续计数；
  其它响应（2xx/3xx/4xx）把计数清零。连续第 3 次失败立即熔断。
* **OPEN**：所有请求直接返回 **503**（带 `Retry-After`），**不访问上游**。
  冷却 **2 秒**后，恰好一个请求通过原子 CAS 成为唯一的**恢复探测**；其余请求继续 503。
* **HALF_OPEN**：只有一个探测在飞。探测成功 → CLOSED 并清零；探测失败或超时 →
  重新 OPEN 并重新计算 2 秒冷却。探测进程崩溃后，探测槽位在租约到期 / 重启时释放，
  可以再次探测。
* 每次调用总时长上限 **1 秒**（`asyncio.wait_for`），连接、读取、解析合计计算。

### 旧结果不会覆盖新结论

* 准入时记录该次调用所属的 **epoch**。若调用期间熔断已打开（epoch 变化），这次结果
  被视为过期：**熔断前发出、熔断后才成功的慢请求既不会清零失败数，也不会让熔断恢复**
  （响应中标记 `stale_success_ignored`）。
* 探测结果携带 **probe generation**。旧代次探测迟到时被丢弃（`stale_probe_ignored`），
  不会覆盖新代次的判定。
* 探测者进程退出后，`HALF_OPEN` 记录被识别为无主，冷却结束后可再次探测。

### 并发与阻塞

* 每个 worker 是独立的 asyncio 事件循环，上游调用全程异步；慢查询不会阻塞同 worker
  内的其他查询。
* 上游 HTTP 连接在进程内复用连接池。
* SQLite 写事务放在线程池执行；常态放行与熔断拒绝都是只读快路径，不占用写锁。

## 共享容量保护

容量状态与熔断状态在同一个 SQLite 库中（`capacity_seats` / `capacity_stats` 表），
因此两个 worker 共享同一套席位，写事务 `BEGIN IMMEDIATE` 保证“检查 + 占席”原子。

### 两级席位（网关侧）

* 网关全局最多 **2 个在途查询**（`GATEWAY_MAX_INFLIGHT`）。
* 其中**普通查询（`level=normal`）最多占 1 席**（`GATEWAY_NORMAL_MAX_INFLIGHT`）；
  另 1 席是紧急查询（`level=emergency`）的**保留席**，普通流量永远占不到它。
* 普通席已占时，再来普通查询立即拒绝；紧急查询仍可使用保留席。两席都满时，连紧急
  查询也会被立即拒绝（`reason=total_capacity`）。
* 容量不足的响应是独立的 **503 `capacity_busy`**，与熔断的 503 `circuit_open` 区分：
  * **不访问目录**（不发起任何上游 HTTP 调用）；
  * **不计入熔断**：不写失败计数、不触发探测选举、不计网关转发次数；
  * 因此**容量拒绝不可能占住恢复探测槽位**——探测选举发生在拿到席位之后的写事务中，
    而容量拒绝在只读预判阶段就返回了。
* 每次占席返回一个不透明 **token**，释放时按 token 删除；迟到的旧结果释放一个已不存在
  的 token 是空操作，**不可能释放新请求正在使用的席位**。
* 请求在 `finally` 中释放席位，**1 秒超时、客户端断开都会回收**；worker 被 kill 后，
  遗留在库里的孤儿席位会在下一次占席时按“进程已死 / 租约（`SEAT_LEASE`，默认 2s）
  过期”回收，网关启动时也会清理一次。
* 已放行请求保持原有语义：**1 秒总超时**、成功/失败照旧计入熔断。

### 目录侧的并发上限（纵深防御）

示例目录服务自身也只同时处理 **2 个查询**（`UPSTREAM_MAX_INFLIGHT`）。超出的请求在
入口处立即收到 **503 `directory_busy`**，不读取任何制品文件；控制面
（`/control/stats`）暴露目录的当前在途、峰值在途与累计忙碌拒绝数。正常情况下网关席位
已把在途限制到 2，目录侧不会触发；它是绕过网关直连目录时的第二道防线。

### 查询级别

`GET /api/meta/{artifact}?level=normal|emergency`（默认 `normal`，非法值返回 400）。
网页上的“查询级别”下拉框对应此参数，批量并发时所有请求使用同一级别。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/meta/{artifact}?level=normal\|emergency` | 经容量闸门与熔断器查询元数据。成功 200，上游失败 502，熔断 503 `circuit_open`，容量不足 503 `capacity_busy` |
| GET | `/api/capacity` | 网关共享席位快照：当前/峰值在途、普通占用、累计放行与忙碌拒绝、各席位明细 |
| GET | `/api/breakers` | 所有制品的熔断状态、最近事件、转发计数，响应中同时包含 `capacity` 快照 |
| POST | `/api/reset` | 清空熔断状态、容量席位与计数（演示用） |
| GET/POST | `/api/upstream-mode` | 读取 / 设置示例上游模式：`normal`、`delay`、`503`，可带 `delay` 秒数 |
| GET | `/api/upstream-stats` | 上游实际收到的 `/artifacts` 请求数、当前/峰值在途、目录忙碌拒绝数（地面真值） |
| POST | `/api/upstream-stats/reset` | 清零上游计数（峰值按当前在途重置） |
| GET | `/healthz` | 健康检查 |

说明：`/api/meta` 的 4xx（例如制品不存在）视为上游**健康**响应，原样返回并清零计数；
仅连接失败、超时和 5xx 计入熔断失败。

示例制品：`example-app.json`、`shared-lib.json`、`reports-tool.json`
（位于 `artifacts/`；请求任意不存在的名字会得到 404，可用来观察“健康 4xx”行为）。

## 建议的手动验证

1. 将上游切到 **延迟**（如 `0.8s`），网页选“普通”、并发 4 个查询：仅 1 个占席执行，
   其余 3 个立刻显示紫色“503 容量忙碌”（毫秒级返回），目录访问次数只增加 1。
2. 保持一个普通查询在途，级别切到“紧急（回滚）”再查询：紧急请求使用保留席成功，
   网关峰值在途变为 2；此时再发普通查询仍是容量忙碌。
3. 将上游切到 **503**，连续查询同一制品 3 次：前两次 502，第三次之后变 503（熔断）。
   再查几次，观察「目录收到的请求」不再增长；OPEN 期间的 503 是 `circuit_open`，
   网关当前在途始终为 0。
4. 把上游切回**正常**，等待 2 秒后查询：该请求作为“恢复探测”成功，熔断恢复；
   期间的容量忙碌请求不会变成探测（探测始终只有 1 个）。
5. 熔断后在浏览器刷新页面，或 `docker compose restart gateway`：熔断状态仍为 open。
6. 直连目录（容器网络内 `http://upstream:9000/artifacts/...`）并发 4 个慢查询：
   2 个处理、2 个立即 `directory_busy`，目录峰值在途为 2。

## 自动化测试

```bash
pip install -r requirements-dev.txt
# 需要先启动 gateway 与 upstream（Compose 或本机进程）
GATEWAY=http://127.0.0.1:8000 python -m pytest -q tests/
```

* `tests/test_unit.py`：直接驱动状态机，覆盖失败计数/清零、熔断、单探测选举、
  探测失败重冷却、慢成功不能恢复、旧探测代次不能覆盖、无主探测释放、超时与错误分类。
* `tests/test_capacity.py`：直接驱动容量席位子系统，覆盖普通席/总席上限、紧急保留席、
  token 防迟到释放、死亡进程与租约回收、峰值计数、只读预判不触发探测。
* `tests/test_breaker.py`：通过 HTTP 覆盖真实双 worker 行为，包括两个 worker 都被用到、
  超时上限、并发探测只放行一个、容量忙碌立即返回且不访问目录/不计熔断、紧急保留席、
  容量拒绝不占恢复探测、慢查询并发，以及（设置 `GATEWAY_RESTART_CMD` 时）重启后
  状态保留。

## 可调环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `UPSTREAM_URL` | `http://upstream:9000` | 上游目录服务地址 |
| `GATEWAY_DB` | `./data/breaker.db` | SQLite 路径（熔断与容量席位共用） |
| `REQUEST_TIMEOUT` | `1.0` | 单次上游调用总超时（秒） |
| `COOLDOWN` | `2.0` | 熔断冷却（秒） |
| `FAILURE_THRESHOLD` | `3` | 连续失败阈值 |
| `GATEWAY_MAX_INFLIGHT` | `2` | 网关共享的在途查询总席位数（两 worker 共享） |
| `GATEWAY_NORMAL_MAX_INFLIGHT` | `1` | 普通查询最多可占席位数；其余席位保留给紧急查询 |
| `UPSTREAM_MAX_INFLIGHT` | `2` | 示例目录自身同时处理的查询上限（仅 upstream 服务） |
