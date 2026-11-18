# 可靠性验证记录（TESTING）

运行方式：`cd backend && python -m pytest tests/ -q`（SQLite + 内存版 FakeRedis，无需外部服务）。
最近一轮：**13 passed**（2026-11-18，Python 3.11）。

## 受理：幂等与冻结输入
- `test_retry_same_request_returns_same_run` 同键同输入重试返回原运行，不新增记录。
- `test_retry_keeps_frozen_snapshot` 源观测事后被修改，重试仍返回首次冻结的快照与 SHA256。
- `test_same_key_different_inputs_rejected` 同键换输入（参数变更）返回 409，且不产生新运行。
- `test_new_key_creates_new_run` 新键正常创建新运行。

## 投递：断网与消息丢失
- `test_delivery_outage_auto_redelivery` 受理时 Redis 断网（RPUSH 失败），运行保持 queued；补投器 sweep 自动重投，随后执行完成。
- `test_queue_flush_recovery` 队列被整体清空（flushall）后，补投器从数据库重建消息并完成执行。

## 执行：长计算、并发与接手
- `test_long_computation_holds_lease_then_expiry_allows_takeover` 心跳持续续租期间他人无法认领；心跳停止且租约过期后可被接手（claim_seq 递增）。
- `test_takeover_then_recovered_worker_cannot_overwrite` 接手者发布结果后，旧执行者恢复：其围栏化发布被拒绝（行数 0），篡改归档被拒绝（IOError），归档字节与数据库结果保持接手者版本。
- `test_concurrent_claims_have_single_winner` 双线程同时认领同一运行，恰有一个赢家。
- `test_duplicate_message_is_not_recomputed` 重复消息第二次执行返回 skipped，结果与 claim_seq 不变。

## 发布：原子切换与中断恢复
- `test_crash_after_archive_recovers_without_partial_publish` 落档后、发布前崩溃：数据库保持 running 且无结果（无半份发布）；租约过期后由新执行者重算，归档字节一致（确定性算法），最终单事务切换为 completed，无残留临时文件。
- `test_full_cycle_over_http` HTTP 全链路：登录→建批次→录观测→幂等受理（重试/换输入 409）→消息清空后补投→执行→结果可见→导出 CSV。

## 接续：现存排队项与历史结论
- `test_legacy_rows_continue_and_history_untouched` 旧格式 queued 行（无幂等键、claim_seq=0）被补投并执行完成；历史 completed 行不被重投、不被执行，结果与 claim_seq 保持原样。

## 并发浸泡（一次性脚本，未入库）
- 3 个 worker 并发消费 6 个运行：全部恰好完成一次（claim #1）。
- 崩溃接管：认领后“死亡”留下残留 processing 项，补投器重投，幸存 worker 以 claim #2 完成，processing 列表清空。
