# Notification Service MVP

企业内部 API 通知服务。业务系统提交通知后即可返回，后台 Worker 负责异步 HTTP 投递、自动重试和失败记录。使用 Go + PostgreSQL，任务表同时作为持久化队列，不依赖 Redis 或消息中间件。

包含持久化接收、并发幂等去重、三种 vendor 映射、当前配置动态更新、指数退避、租约回收、失败查询、日志查询、人工重试、健康检查和队列指标。Header/Body 映射直接使用 Go `text/template`。MVP 不包含鉴权、密钥引用、配置版本或单 vendor 限流。

## 目录

```text
cmd/server/                   API + Worker 入口
cmd/mockvendor/               本地模拟供应商入口
internal/api/                 HTTP 接口和输入校验
internal/task/                任务、配置、日志和统计的数据库操作
internal/vendor/              当前配置模型和 text/template 渲染
internal/worker/              领取、投递、重试、租约回收
internal/mockvendor/          仅演示使用的模拟供应商
tests/integration/            真实 PostgreSQL + httptest.Server 集成测试
migrations/                   旧数据库升级脚本
scripts/acceptance.py         独立运行服务的黑盒验收
scripts/package_submission.py 会话导出和提交包生成
scripts/test_package_submission.py  导出保真及隐私边界测试
docs/ACCEPTANCE.md            本次验收结果与限制
artifacts/                    黑盒结果、测试日志、覆盖率
schema.sql                    三张表、索引及广告/CRM/库存测试配置
Dockerfile                    多阶段构建，非 root 运行
compose.yaml                  数据库与可选 app/demo profiles
.github/workflows/ci.yml       PostgreSQL 集成测试与 race 检查
AI-conversation.zip           完整可见 AI 会话与工具记录
```

## 本地启动

需要 Go 1.26+ 和 PostgreSQL 17+。Python 3 仅用于验收与打包，不参与服务运行。

有 Docker 时，可在项目根目录先启动数据库：

```powershell
docker compose up -d --wait db
```

新数据库会自动执行 `schema.sql`。使用自己的数据库时，先创建数据库，再显式初始化：

```powershell
$env:DATABASE_URL = 'postgres://notification:notification@127.0.0.1:5432/notifications?sslmode=disable'
psql $env:DATABASE_URL -v ON_ERROR_STOP=1 -f schema.sql
```

一个终端启动模拟供应商；`MOCK_FAIL_FIRST=1` 让每个新通知首次返回 503，后续成功，以便观察重试：

```powershell
$env:MOCK_FAIL_FIRST = '1'
go run ./cmd/mockvendor
```

另一个终端启动通知服务：

```powershell
$env:DATABASE_URL = 'postgres://notification:notification@127.0.0.1:5432/notifications?sslmode=disable'
go run ./cmd/server
```

`DATABASE_URL` 必填。API 默认监听 `127.0.0.1:8080`，可用 `HTTP_ADDR` 修改；mock 默认监听 `127.0.0.1:9090`，可用 `MOCK_ADDR` 修改。Worker 随 API 一起启动。

`schema.sql` 用于初始化，不迁移旧表结构；重复执行不会覆盖已修改的 vendor。已有 Step 1/2 数据库升级前执行：

```powershell
psql $env:DATABASE_URL -v ON_ERROR_STOP=1 -f migrations/003_manual_retry.sql
```

迁移只增加人工重试的预算起点，保留已有任务和日志，可重复执行。启动会检查该字段，防止遗漏迁移后仍报告就绪。

## 容器启动

数据库默认启动；API 和 mock 使用可选 profile：

```powershell
# 一键演示：数据库、API、首次失败再成功的 mock
docker compose --profile demo up --build -d --wait

# 只运行数据库和 API；通过 vendor 配置接口设置实际目标地址
docker compose --profile app up --build -d --wait
```

demo mock 共享 app 的网络命名空间，因此种子配置的 `127.0.0.1:9090` 地址无需替换。API 只发布到宿主机 `127.0.0.1:8080`。镜像以非 root 用户运行，包含 HTTPS 所需 CA。

数据库保存在 Compose 持久卷中，`docker compose down` 不清空数据。保留 PostgreSQL 默认持久化设置。已有数据卷不会重新执行初始化脚本，升级时需显式应用迁移。

## 提交通知

`POST /v1/notifications`，Header 为 `Content-Type: application/json` 和 `Idempotency-Key`：

```json
{
  "source_system": "shop",
  "vendor_code": "inventory",
  "event_type": "order.paid",
  "payload": {"sku": "SKU-001", "delta": -2, "order_id": "order-20261101-001"}
}
```

PowerShell 示例：

```powershell
$body = @{
    source_system = 'shop'
    vendor_code = 'inventory'
    event_type = 'order.paid'
    payload = @{ sku = 'SKU-001'; delta = -2; order_id = 'order-20261101-001' }
} | ConvertTo-Json -Depth 10

$result = Invoke-RestMethod -Method Post `
    -Uri 'http://127.0.0.1:8080/v1/notifications' `
    -ContentType 'application/json' `
    -Headers @{ 'Idempotency-Key' = 'order-20261101-001:inventory' } `
    -Body $body
$result
```

数据库确认提交后返回 **202 Accepted**，附带 `Location: /v1/notifications/<task_id>`：

```json
{"task_id":"6be0c53b-e880-4d3c-9299-905cf4895cd1","status":"PENDING","duplicate":false}
```

202 表示已持久化，不表示已送达。同来源、同幂等键、同内容的重复请求返回原任务 ID、当前状态和 `duplicate: true`；同键不同内容返回 409。JSON 对象字段顺序和空白不影响摘要；数字保留文本表示，`1` 与 `1.0` 视为不同内容。解码使用 `json.Number`，不会把大整数转成 float64。

去重范围为 `(source_system, idempotency_key)`。同一事件通知三个 vendor 时使用不同键，例如 `order-001:ads`、`order-001:crm`、`order-001:inventory`。数据库唯一约束处理并发冲突，独立后续查询读取原任务，避免单语句快照看不到刚提交的并发记录。

响应丢失、连接中断或收到 503 时，调用方使用原键和原内容重提。业务提交成功到调用本服务前的事件丢失，需要业务方可靠重提，或在业务数据库采用 Transactional Outbox。

## 查询、修复与人工重试

| 接口 | 用途 |
| --- | --- |
| `GET /v1/notifications/{id}` | 状态、累计尝试次数、预算起点、调度时间、最近错误 |
| `GET /v1/notifications?status=DEAD` | 失败列表，支持 vendor_code、source_system、limit、offset |
| `GET /v1/notifications/{id}/logs` | 每次投递结果、错误和当时配置快照 |
| `POST /v1/notifications/{id}/retry` | 重新入队 DEAD 任务，请求体为空 |
| `GET /v1/vendors` | 所有当前配置 |
| `GET /v1/vendors/{code}` | 单个 vendor 当前配置 |
| `PUT /v1/vendors/{code}` | 新增或完整替换配置，不创建版本 |
| `GET /healthz` | 数据库健康检查 |
| `GET /metrics` | Prometheus 文本格式的各状态任务数与最老等待任务年龄 |

列表默认 limit=50，最大 100，offset 默认 0，按创建时间倒序。日志最多 100 条，按尝试号升序；使用 `after_attempt=<上一页最后尝试号>` 获取后续记录。

修复配置并重新入队示例：

```powershell
$config = Invoke-RestMethod 'http://127.0.0.1:8080/v1/vendors/inventory'
$config.endpoint_url = 'http://127.0.0.1:9090/inventory/adjustments'
Invoke-RestMethod -Method Put `
    -Uri 'http://127.0.0.1:8080/v1/vendors/inventory' `
    -ContentType 'application/json' -Body ($config | ConvertTo-Json -Depth 10)

$failed = Invoke-RestMethod 'http://127.0.0.1:8080/v1/notifications?status=DEAD'
# 从 $failed.items 中选择任务，再替换下面的编号。
Invoke-RestMethod -Method Post 'http://127.0.0.1:8080/v1/notifications/<task_id>/retry'
```

PUT 必须提供完整配置；Body 中 code 可省略或与 URL 一致。检查 HTTP(S) 地址、方法、超时、次数、退避参数和模板语法，不使用示例数据预先渲染。缺少业务字段的错误在实际投递时记录。

人工重试保留任务 ID、对外幂等键、历史日志和累计 `attempt_count`。`retry_start_attempt` 是本轮预算起点，每轮最多执行当前配置的 max_attempts 次，退避重新开始。非 DEAD 任务返回 409，并发调用只有一个可以重新入队。修改配置不会自动复活 DEAD 任务。管理操作写入结构化服务日志，MVP 没有操作人认证。

## vendor 与模板

| code | 默认地址 | 方法 | payload 示例 |
| --- | --- | --- | --- |
| ads | http://127.0.0.1:9090/ads/conversions | POST | `{"user_id":"u1","click_id":"click1"}` |
| crm | http://127.0.0.1:9090/crm/contacts | PATCH | `{"contact_id":"c1","status":"paid"}` |
| inventory | http://127.0.0.1:9090/inventory/adjustments | POST | `{"sku":"SKU-001","delta":-2,"order_id":"order1"}` |

Header 模板输出字符串值组成的 JSON 对象，Body 模板当前输出 JSON。上下文为 `.TaskID`、`.SourceSystem`、`.EventType`、`.Payload`：

```gotemplate
{"Content-Type":"application/json","Idempotency-Key":{{json .TaskID}}}
```

```gotemplate
{"sku":{{json .Payload.sku}},"quantity_delta":{{json .Payload.delta}}}
```

`json` 使用 json.Marshal 输出正确转义的 JSON 值，外层不要再加引号；模板启用 missingkey=error。种子配置以任务 UUID 作为供应商幂等键，自动和人工重试均保持相同身份。mock 校验同键同内容，仅在内存中维护演示状态，不代替真实供应商的幂等协议。

## 可靠性机制

三张表分别保存当前 vendor 配置、任务队列和投递日志。任务只关联 vendor_code。Worker 每次尝试读取当前配置，将同一份配置写入 vendor_snapshot 后再发送；下一次重试使用修复后的配置，旧日志不变。

默认 4 个投递 goroutine，空闲轮询 500 ms，回收间隔 5 秒。每个空闲槽只领取一条任务，不在内存中排队占用租约；多个服务实例可以共享队列。

1. 短事务用 `FOR UPDATE OF t SKIP LOCKED` 领取到期任务，增加累计尝试次数，生成租约，写 STARTED 日志，提交后释放连接和锁。
2. 事务外渲染模板并执行 HTTP，vendor 超时覆盖连接、响应头及响应体读取，不跟随重定向。
3. 新事务原子更新任务与日志。必须令牌相同、仍为 PROCESSING、租约有效；日志失败时任务更新也回滚。

| 结果 | 处理 |
| --- | --- |
| HTTP 2xx 且响应读取未出错 | SUCCEEDED，不解析 Body 业务码 |
| HTTP 408、429、5xx | 自动重试，额度耗尽后 DEAD |
| 网络错误、连接/读取超时、响应读取异常 | 自动重试，额度耗尽后 DEAD |
| 模板错误或非法 URL | 自动重试，允许修复配置恢复 |
| 其他状态，包括 3xx、其他 4xx | DEAD，保留错误及响应摘要 |

退避上界为 `min(base × 2^(本轮尝试号-1), max)`，实际取该值的 50%–100%。有效 Retry-After 为最小等待时间，可以超过本地上限。本轮首次计入 max_attempts。

响应使用 `io.ReadAll(io.LimitReader(body, 64*1024+1))`，额外 1 字节判断截断，不无限排空。日志最多保留 4 KiB 文本加截断标记，清理非法 UTF-8 和 NUL；Body 始终关闭。响应头限制 64 KiB。响应过大本身不触发重试。

租约为 HTTP 超时加 30 秒余量。回收器用 SKIP LOCKED，每次最多回收 100 条：旧 STARTED 记为 UNKNOWN，清除租约，按当前配置退避重试；额度耗尽则 DEAD。令牌和期限检查防止旧 Worker 迟到结果覆盖新状态。

HTTP 已执行但结果未写库时，回收可能再次发送同一操作，因此提供至少一次投递，不能单方面保证外部副作用只发生一次。库存扣减等接口需要供应商幂等配合。HTTP 202 仅表示受理；SUCCEEDED 不代表供应商异步业务最终完成。不保证任务间顺序，不把广告、CRM、库存当作可互换备用目标。

退出时停止领取并取消在途 HTTP，再给结果写库最多 5 秒；Worker 退出后关闭数据库连接池。任务和失败日志不自动删除。

## 输入与错误

请求最多 1 MiB，拒绝未知顶层字段和多个 JSON 文档。source_system、vendor_code 为 1–64 位字母、数字、点、下划线或连字符，首位为字母或数字。事件类型和幂等键最多 128 字符，不得空白、首尾留空或含控制字符。payload 必须为对象。JSONB 不支持的 NUL 和超范围数字返回 400。

| HTTP 状态 | 含义 |
| --- | --- |
| 202 | 持久化接收、命中原任务或人工重试入队 |
| 400 | 字段、JSON、配置或查询参数非法 |
| 404 | 任务或 vendor 不存在 |
| 409 | 幂等冲突或人工重试的任务不是 DEAD |
| 413 / 415 | 请求过大 / Content-Type 不是 application/json |
| 422 | 提交指定了未配置的 vendor |
| 503 | 存储失败，提交可用原幂等键重提 |

```json
{"error":{"code":"idempotency_conflict","message":"Idempotency-Key was already used with different request content"}}
```

## 测试与验收

完整验收必须设置 TEST_DATABASE_URL；不设置时数据库集成测试会明确跳过。测试只在随机临时 schema 创建数据，结束时仅删除该 schema，不修改业务表；数据库用户需要创建 schema 权限。

```powershell
$env:TEST_DATABASE_URL = $env:DATABASE_URL
go test -count=1 ./...
go vet ./...
go mod verify
go build -o bin/notification.exe ./cmd/server
go build -o bin/mockvendor.exe ./cmd/mockvendor
python -m unittest discover -s scripts -p 'test_*.py'
```

API 和 mock 启动后执行独立黑盒验收：

```powershell
python scripts/acceptance.py
```

验收临时修改三个测试 vendor 的地址和重试参数，创建独立 acceptance 事件，结束后恢复原配置；用于本地演示环境。结果保存在 artifacts/acceptance.json，覆盖三种 vendor、自动重试、去重冲突、配置修复、人工重试、历史快照、失败列表和指标。

集成测试还覆盖事务外发送、24 个并发接收去重、并发 Worker 不重复领取、锁定任务跳过、超时、超大及二进制响应、租约回收、迟到结果隔离、日志失败回滚、人工重试预算及并发控制、旧表迁移。单元测试以无限响应流确认读取有界。

GitHub Actions 使用 PostgreSQL service 并执行 go test -race。本次实际执行环境、覆盖率、进程重启结果及未执行的检查见 docs/ACCEPTANCE.md。

## 本地提交材料

项目仓库：[jokerD888/RightCapital_AI_Coding](https://github.com/jokerD888/RightCapital_AI_Coding)。本地 submission/notification-service.zip 包含代码、README、部署文件、测试、验收证据和 AI 会话包；根目录 AI-conversation.zip 随源代码一并提交。

AI-conversation.zip 包含会话正文 Markdown、原始可见事件 JSONL 和 SHA-256 清单。所有可见用户消息、助手回复、工具调用及返回保持原文；系统/开发者指令、内部推理和运行时元数据不属于导出对话。清单记录截止时间；继续对话或修改提交材料后，可重新运行打包脚本更新。

```powershell
python scripts/package_submission.py
```

打包脚本优先使用当前会话环境，否则沿用现有会话包清单中的源文件。也可通过 --session-file <本次原始JSONL路径> 显式指定，避免混入其他聊天。Python 仅参与验收和打包。
