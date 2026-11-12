# Pointwise Delivery

商品、库存、销售三张表在**同一个只读事务、同一条连接**里导出成 Parquet。演示业务库使用
SQLite WAL，独立的写入进程持续扣库存、登记销售、更新累计销量（三者一次提交）；网页可以
在写入进行中发起导出，得到的三张表始终来自同一个事务快照。

## 快速开始

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
cd web && npm install && npm run build && cd ..
./run.sh          # http://127.0.0.1:8765
```

`run.sh` 默认同时启动一个独立的写入进程（`python -m app.writer`）。也可以分两个终端：

```bash
POINTWISE_START_WRITER=0 ./run.sh          # 终端 1：API + 导出
python -m app.writer                       # 终端 2：持续写入
```

前端开发模式：`cd web && npm run dev`（Vite 把 `/api` 代理到 8765）。

环境变量见 `.env.example`；数据默认落在 `./.data`。

## 一致性的做法

- 快照：导出线程用 `mode=ro` 连接执行 `BEGIN DEFERRED`，第一条 SELECT 固定 WAL end mark。
  三张表的所有读取都在这个事务里完成，因此写入进程后续提交的批次不会被读到。
- 批次：业务每次变更在 **一个 `BEGIN IMMEDIATE` 事务**里写入 `write_batches`、`sales`、
  `stock`、`products` 并一起提交，批次号自增。导出事务读到的第一个批次号就是快照的锚点，
  写进清单（`business.batch_id` / `committed_at`）。
- 勾稽：清单文件写好后，导出器重新从落盘的 Parquet 读取并核对：
  库存 = 初始库存 − 累计销量；每个商品销售明细合计 = 累计销量；明细总量 = 累计销量之和；
  行数、主键唯一性、销售行不晚于快照批次等。
- 发布：先写 `staging/<id>/`，核对通过后写 `manifest.json`、`manifest.sha256`、`preview.json`、
  `bundle.zip`，再以一次 `os.replace` 把整个目录移入 `exports/<id>/`。失败或取消只删除
  staging，并保留任务记录；下载接口只对 `succeeded` 的任务提供文件。
- 重复下载：发布后的文件原样保存，下载直接返回这些字节（同一批次重复下载得到相同内容）。
  若在写入完全停顿时对同一批次再发起导出，会复用已发布批次的原文件并在任务上记 `reused_from`。

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/overview` | 业务概况、写入进程状态、控制值 |
| GET/PUT | `/api/controls` | 写入批次频率、每批销售行数、暂停/继续、导出读取速度 |
| POST | `/api/exports` | 发起导出（队列满返回 429） |
| GET | `/api/exports` | 最近任务（进度、阶段、速度、快照批次） |
| GET | `/api/exports/{id}` | 单个任务（含清单、勾稽结果、预览） |
| POST | `/api/exports/{id}/cancel` | 取消；不会留下半份文件 |
| GET | `/api/exports/{id}/download` | 下载整批 zip |
| GET | `/api/exports/{id}/files/{name}` | 下载单个文件 |
| POST | `/api/verifications` | `export_id` 复核已发布批次，或上传 zip 复核 |
| GET | `/api/verifications` | 最近复核报告 |

## 测试

```bash
python -m pytest            # 后端：快照、导出、取消、篡改检测、并发写
cd web && npm test          # 前端：进度、勾稽展示、控制、复核
```
