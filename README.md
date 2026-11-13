# 临床试验数据核查服务（FastAPI + SQLite）

访视明细入仓前的 SQL 核查平台：上传含 `subjects(id, enrolled_on)`、
`visits(id, subject_id, visited_on)` 的 SQLite 副本及一组**具名只读查询**，
后台逐条执行；每条查询返回违规记录，**零行才通过**。仅当批次完成且全部规则
零违规时，报告才给出 `can_load=true` 的可入仓结论。

## 1. 需求对照

| 需求 | 实现 |
| --- | --- |
| 上传 SQLite 副本 + 具名只读查询 | `POST /batches`（multipart：`db_file` + `rules` JSON），提交前做 SQLite 头/完整性/表结构校验与 `EXPLAIN` 静态解析 |
| 零行才通过 | 规则包装为 `SELECT COUNT(*) FROM (<原查询>)` 计数；0 行 → `passed`，否则 `violations` 并保留**前 20 行**明细 |
| 后台运行、报告规则结论/违规总数/前 20 行 | 线程池后台执行；`GET /batches/{id}` 返回每条规则 `status`、`violation_count`、`sample_rows` 及批次汇总 |
| 仅全部通过才可入仓 | `can_load=true` 当且仅当批次 `completed` 且所有规则 `passed` |
| 不修改上传库 | 上传库始终以 `file:...?mode=ro&immutable=1` 只读 URI 打开，并设 `PRAGMA query_only=ON`；测试断言文件字节不变 |
| 每条查询限 5 秒，超时**停止实际计算**且继续其他规则，不当通过 | 看门狗线程到点调用 `sqlite3.Connection.interrupt()`（官方异步取消 API）+ 进度回调协作中止；规则状态 `timeout`，其余规则继续，`timeout` 永不放行 |
| 运行期间其他批次可提交/查询 | 批次间线程池并行、批次内规则顺序执行；路由不被单批阻塞（测试验证快批次不被慢批次阻塞） |
| 取消整批，停止实际计算，迟到结果不得改成完成 | `POST /batches/{id}/cancel` 置 `threading.Event`，经同一套 interrupt 中止；落库前二次复核批次终态，迟到结果丢弃并标 `cancelled` |
| 取消/超时/中断均不可放行 | 四种非通过状态 `timeout` / `error` / `cancelled` / `interrupted` 均使 `can_load=false` |
| 重启保留完整报告，未完成批次标中断 | 报告写独立控制库（WAL）并挂载 Docker volume；启动 lifespan 将 `queued/running` 批次及未完成规则置 `interrupted`，已完成结论保留 |
| 可实际启动的 Docker Compose | `docker compose up --build` 即起；volume 持久化 `/data`，含 healthcheck |
| 交互式 API 文档演示 | Swagger UI：`http://localhost:8000/docs`；落地页 `/docs-demo` |

## 2. 目录结构

```
app/
  config.py       # 环境变量配置（5 秒时限、20 行明细、上传上限等）
  storage.py      # 控制库：批次/规则报告持久化、重启恢复
  engine.py       # 执行引擎：只读连接、5 秒 interrupt 中断、取消、计数/取样
  validation.py   # 上传校验：SQLite 头、完整性、必需表、SQL 只读白名单
  runner.py       # 后台线程池编排、取消注册表、终态裁决、迟到结果保护
  schemas.py      # Pydantic 请求/响应模型
  main.py         # FastAPI 路由与交互式文档
demo/
  make_sample_db.py       # 生成含 5 类预置违规的样例库
  rules.sample.json       # 5 条核查规则 + 1 条超时演示慢查询
  rules.clean-demo.json   # 干净库上的通过示例
  run_demo.sh             # curl 端到端演示（上传/轮询/取消）
tests/
  test_engine.py         # 引擎语义测试（29 项，仅标准库）
  test_orchestration.py  # 端到端编排测试（24 项，含并发/取消/重启）
Dockerfile
docker-compose.yml
requirements.txt
```

## 3. 启动（Docker Compose）

```bash
docker compose up --build
# 服务： http://localhost:8000
# 文档： http://localhost:8000/docs   （自动重定向 / -> /docs）
# 演示指引：http://localhost:8000/docs-demo
# 健康检查：http://localhost:8000/health
```

数据落在命名 volume `ctms-data`（容器内 `/data`）：
- `/data/control.db`：服务自身的报告控制库；
- `/data/uploads/<batch_id>/source.db`：上传库副本，全程只读使用。

本地无 Docker 时（需自行准备 Python 3.11 与 `requirements.txt` 依赖）：

```bash
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

> 注意：取消注册表是**进程内**状态，因此 Compose/Dockerfile 固定单 uvicorn worker；
> 批次间并行由进程内线程池提供。水平扩容需要把取消信号迁出进程（如 Redis pub/sub）。

## 4. 交互式文档演示步骤（Swagger UI）

1. 准备样例数据（容器内已附带 `demo/`，本地可执行）：
   ```bash
   python demo/make_sample_db.py demo/sample_ctms.db
   ```
2. 浏览器打开 `http://localhost:8000/docs`，找到 `POST /batches`，点 **Try it out**：
   - `db_file` 选择 `demo/sample_ctms.db`；
   - `rules` 粘贴 `demo/rules.sample.json` 的内容；
   - `name` 随意，如 `方案A-首次核查`；执行后响应中复制 `id`。
3. 打开 `GET /batches/{batch_id}`，粘贴 id 执行，轮询直到 `status=completed`，
   查看每条规则结论、`total_violations` 与 `sample_rows`（前 20 行）。
   样例库的预期：前 5 条规则均命中违规，第 6 条慢查询为 `timeout`，
   因此 `can_load=false`、`total_violations_complete=false`。
4. 取消演示：再建一个含慢查询的批次，1 秒内调用
   `POST /batches/{batch_id}/cancel`，随后查报告：批次 `cancelled`、
   未完成规则 `cancelled`、`can_load=false`。
5. 重启演示：`docker compose restart` 后再次 `GET /batches`，
   报告完整保留；重启时仍在运行的批次状态为 `interrupted`，
   `/health` 的 `interrupted_on_startup` 可查对应批次 id。

也可以一键跑脚本演示（需要 curl，jq 可选）：

```bash
bash demo/run_demo.sh
```

## 5. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/batches` | 上传副本与规则，创建后台批次，201 返回初始报告 |
| GET | `/batches` | 批次列表与汇总（违规总数、通过/未通过规则数、`can_load`） |
| GET | `/batches/{id}` | 完整报告：各规则结论 + 违规总数 + 前 20 行明细 |
| POST | `/batches/{id}/cancel` | 取消整批（运行中返回信号已送达；终态返回 `cancelled_signal_sent=false`） |
| GET | `/health` | 健康检查与重启中断批次 id |
| GET | `/docs` | Swagger UI 交互式文档 |

规则状态机：

```
queued ──> passed        （零行，唯一通过态）
        ├> violations    （有违规行，附计数与前 20 行）
        ├> timeout       （5 秒内被 interrupt 中止，无计数，不放行）
        ├> error         （SQL 执行失败/只读库写尝试，不放行）
        ├> cancelled     （批次取消，不放行）
        └> interrupted   （进程重启未完成，不放行）
```

批次状态：`queued / running / completed / cancelled / interrupted`。
注意 `completed` 只表示“规则全部执行完毕”，是否可入仓以 `can_load` 为准。

## 6. 5 秒时限与取消的实现要点

- 每条规则在独立工作线程中跑两条 SQL（计数 + `LIMIT 20` 取样），**共享同一
  deadline**，总预算 `QUERY_TIMEOUT_SECONDS`（默认 5 秒）。
- 看门狗线程到点调用 `sqlite3.Connection.interrupt()`，SQLite 虚拟机在下一条
  字节码指令中止当前语句并抛 `OperationalError(interrupted)`——计算被真正停止，
  不是客户端放弃读取。
- 同时注册 `set_progress_handler`（每 1000 条指令回调）：取消事件置位时回调
  返回非零立即中止，取消响应通常在几十毫秒内。
- 临界竞态由共享锁 + `reason` 标记裁决：即使语句刚好跑完，只要时限/取消已
  触发，结果按 `timeout`/`cancelled` 处理，绝不在超时后放行。
- 规则级中断不杀线程：工作线程随 interrupt 退出后，编排线程继续下一条规则。

## 7. 测试

测试仅依赖 Python 标准库（CI 无网络也可跑），共 53 项断言：

```bash
python -I tests/test_engine.py          # 29 项：通过/违规、超时中断、取消、只读、重启、迟到结果
python -I tests/test_orchestration.py   # 24 项：完整批次、并发、取消、重启恢复、汇总
```

关键验证点：慢查询在 1 秒预算下实测 ~1.00s 中止（自然完成需数小时）；
只读连接拒绝 INSERT/UPDATE/DELETE/CREATE 且上传库文件字节不变；
取消后工作线程真实退出、结论为 `cancelled` 且批次终态不被迟到结果改写。
