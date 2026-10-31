# 本地验收记录

验收日期：2026-11-01（Asia/Shanghai）。以下记录为代码验收时的本地执行结果，GitHub 后续检查见[仓库 Actions](https://github.com/jokerD888/RightCapital_AI_Coding/actions)。

## 结论

**核心 MVP 验收通过。** 全部 Go 单元及真实 PostgreSQL 集成测试通过，独立编译后的 API、mock 与真实数据库的黑盒验收通过。四个验收任务最终全部 SUCCEEDED，API 和 PostgreSQL 重启后仍能读取原状态及完整日志。

## 执行环境

- Windows amd64，Go 1.26.0，PostgreSQL 17.6。
- 使用临时本机 PostgreSQL 实例，验收数据库与测试随机 schema 均独立创建，结束后删除。
- Go 集成测试显式设置 TEST_DATABASE_URL，没有跳过数据库测试。
- mock 设置 MOCK_FAIL_FIRST=1，三种通知的首次调用均返回 503。

## 检查结果

| 检查 | 结果 |
| --- | --- |
| Go 测试（含数据库、HTTP、管理接口、迁移） | PASS，28 个顶层测试，另含参数化子测试 |
| internal 包合并语句覆盖率 | 85.0% |
| go vet ./... | PASS |
| go mod verify | PASS |
| API 与 mock 的 Windows 编译和实际运行 | PASS |
| API 与 mock 的 Linux amd64 交叉编译 | PASS |
| Python 会话导出保真与隐私边界测试 | PASS |
| Compose 和 CI YAML 结构检查 | PASS |
| 独立黑盒验收 | PASS，11 项检查 |
| API 与 PostgreSQL 重启持久化 | PASS，任务状态、尝试次数和投递日志保持一致 |

覆盖率命令使用显式参数列表，确保 ./... 和 -coverpkg=./internal/... 均完整传入。早先终端拆分参数的临时结果没有作为本记录依据。

## 独立黑盒任务

| 场景 | 最终状态 | 累计尝试 | 任务 ID |
| --- | --- | --- | --- |
| ads | SUCCEEDED | 2 | `14556379-ce3a-49f7-87fb-6156ed7b5a0c` |
| crm | SUCCEEDED | 2 | `0795bf28-f0cc-4486-bc89-18ba1eb50959` |
| inventory | SUCCEEDED | 2 | `fb008e29-8d70-4b12-95b9-2c5d0d0572a7` |
| inventory-repair | SUCCEEDED | 3 | `0ca87290-7841-426a-8ee4-35717ea0f507` |

前三个任务均为首次 503、第二次成功。修复场景先以错误 URL 返回 404 进入 DEAD，再修改当前配置、用原任务 ID 手工重试；新一轮首次 503，随后成功，累计 3 次。检查了历史日志保留旧 URL、后续日志使用修复后的 URL，以及非 DEAD 任务不能人工重试。

## 可靠性覆盖

- 24 个并发重复请求只生成一个任务；同键不同内容返回 409。
- 三个 vendor 的方法、Header、Body 映射和大整数精度正确。
- HTTP 期间另一事务能锁定任务行，并能看见已提交的 STARTED 日志，验证发送在事务外。
- 503 自动轮询、指数退避、随机抖动、Retry-After、永久错误和额度耗尽。
- 网络超时后重试；超大及二进制响应有界读取并生成数据库可存储的摘要。
- 无限响应流单元测试验证实际最多读取 64 KiB + 1 字节，且关闭 Body。
- 锁定任务跳过、两个 Worker 实例不重复领取同一任务。
- 定期回收超时租约，UNKNOWN 日志保留，旧 Worker 迟到结果不能覆盖新状态。
- 投递日志更新失败时任务状态一并回滚，后续租约恢复可继续投递。
- 失败列表、日志分页、配置修复、人工重试新预算和并发控制。
- 旧表迁移可重复执行，已有任务保留，预算字段约束生效。

## 复跑

```powershell
$env:TEST_DATABASE_URL = $env:DATABASE_URL
go test -count=1 ./...
go vet ./...
go mod verify
python -m unittest discover -s scripts -p 'test_*.py'
# API 和本地 mock 已启动后：
python scripts/acceptance.py
```

覆盖率使用完整参数引号，避免终端拆分：

```powershell
go test -count=1 "-coverpkg=./internal/..." "-coverprofile=artifacts/coverage.out" ./...
go tool cover "-func=artifacts/coverage.out"
```

## 执行限制

本机未安装 Docker/Compose，未执行容器镜像构建或 compose up；已检查配置结构并交叉编译 Linux 程序。Windows 环境 CGO_ENABLED=0、没有 C 编译器，未在本机执行 race 检查；已提供 Ubuntu + PostgreSQL 的 GitHub Actions 流程执行 go test -race。本记录生成时该流程尚未推送运行，后续状态以仓库 Actions 为准。

以上限制不影响已实际完成的本机 PostgreSQL、HTTP、故障恢复与进程重启验收，不把静态检查或待运行的 CI 当作已执行结果。

## 证据

- artifacts/test-results.txt：完整 Go 测试输出。
- artifacts/coverage.out 与 artifacts/coverage.txt：合并覆盖率。
- artifacts/acceptance.json：机器可读黑盒结果与四个任务编号。
- AI-conversation.zip：截至清单记录时间的完整可见会话，可通过打包脚本重新生成。
