# VERIFY_REQ Phase 1 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** D 节点在发起 KV RDMA 读之前，通过 ZMQ 向 P 校验该请求的 blocks 是否仍被持有；未持有（或校验失败）则跳过读、复用现有 failed-recv → 重算路径，杜绝 PD 分离脏读（issue #15420）。

**架构：** 在 `MooncakeConnectorV1` 的控制面（ZMQ REQ/ROUTER）上新增 `VERIFY_REQ/VERIFY_RESP` 消息对。D 侧在 `_transfer_kv_cache_all_groups` 的零块早退之后、发起读之前调用 `_verify_remote_blocks_held`；P 侧 `run_busy_loop` 新增分支，把判定与续命逻辑下沉为 `KVCacheTaskTracker.check_and_extend`（max 下限续命：剩余窗口 < G=10s 时顶到 G，只延不缩）。失败走现有 `_mark_failed_recv_request` → `invalid_block_ids` → scheduler 重算链路，scheduler 侧零改动。受 env 开关 `VLLM_ASCEND_VERIFY_KV_BEFORE_PULL`（默认关闭）控制。

**技术栈：** Python 3.12 / vllm-ascend v0.23.0 / msgspec msgpack / ZMQ（REQ-ROUTER，socket 池）/ unittest + pytest

**规格：** `docs/superpowers/specs/2026-09-03-verify-req-phase1-design.md`（已批准，commit `35c73233e`）

---

## 环境前提（执行前检查）

1. 测试命令统一为：
   ```bash
   cd /home/gao/code/python/vllm-ascend
   env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest <测试路径> -x -q
   ```
   `TORCH_DEVICE_BACKEND_AUTOLOAD=0` 必须带（本机 WSL2 无 NPU，禁 torch_npu 自动加载）。
2. **torchvision 必须是 CPU 构建**（版本号带 `+cpu` 后缀）。若运行测试报 `operator torchvision::nms does not exist`，执行：
   ```bash
   uv pip install --python /home/gao/code/python/vllm-ascend-venv/.venv/bin/python --reinstall-package torchvision "torchvision==0.25.0" --index-url https://download.pytorch.org/whl/cpu
   ```
3. 测试文件 `tests/ut/kv_offload/test_mooncake_connector.py` 在导入连接器前注入了 fake `mooncake.engine` 模块并 patch 了 group 函数——新测试直接追加到该文件，沿用其基建（`MockVllmConfig`、`make_agent_metadata`），不要另建文件。

## 文件结构

| 文件 | 职责 | 变更类型 |
|------|------|---------|
| `vllm_ascend/envs.py` | 新增 `VLLM_ASCEND_VERIFY_KV_BEFORE_PULL`（bool，默认 False） | 修改 |
| `vllm_ascend/distributed/kv_transfer/kv_p2p/mooncake_connector.py` | 常量/异常类（模块顶部）；`KVCacheTaskTracker.check_and_extend` + `recently_force_freed` 降噪；`run_busy_loop` VERIFY 分支；`KVCacheRecvingThread` 开关字段/计数器/`_verify_remote_blocks_held`；`_transfer_kv_cache_all_groups` 插入校验 | 修改 |
| `tests/ut/kv_offload/test_mooncake_connector.py` | 新增 `TestVerifyReq` 测试类 + 顶部 import 扩充 | 修改 |

不新建任何文件。所有改动都在既有文件内，遵循现有代码组织。

## 关键代码事实（已核实，写实现时直接引用）

- 连接器常量区：`GET_META_MSG = b"get_meta_msg"`、`DONE_RECVING_MSG = b"done_recving_msg"` 定义在模块顶部（`KVCacheTaskTracker` 类之前，约 L80）。
- `KVCacheTaskTracker`（L166-242）：`done_task_lock`、`finished_requests`、`delayed_free_requests: OrderedDict[str, float]`、`reqs_to_process: set[str]`；`update_done_task_count`（L187）、`add_delayed_request`（L215，仅在 `reqs_to_process` 中才加入）、`_retrieve_expired_requests`（L221，插入序扫描、首条未过期即 break）。
- `run_busy_loop`（L322-406）：函数顶部局部 `encoder = msgspec.msgpack.Encoder()`；if-elif 链 `GET_META_MSG`（L360）→ `DONE_RECVING_MSG`（L362）→ `else` 记错误日志（L387）；外层 try/except 兜底。
- `KVCacheRecvingThread.__init__`（L409-560）：L497-498 `self.encoder = msgspec.msgpack.Encoder()`、`self.decoder = msgspec.msgpack.Decoder(MooncakeAgentMetadata)`（verify 应答不能用这个 decoder）；L505 `self.timeout = 1.0`；L533-537 附近是 `failed_recv_requests`/`invalid_block_ids` 及其锁。
- `_get_remote_socket`（L1437）/`_return_remote_socket`（L1461）：池化 REQ socket，`SNDTIMEO/RCVTIMEO=1s`；`_send_done_recv_signal`（L1401-1435）的出错纪律：`except RuntimeError` → `sock.close()` 不归还池。
- `ensure_zmq_send`（L3709）/`ensure_zmq_recv`（L3730）：内部重试 3 次 × 0.1s sleep，最终 raise `RuntimeError`。
- `_transfer_kv_cache_all_groups`（L774-992）：L788-790 零块早退 `if num_local_blocks == 0 and not has_replicate_k_blocks: return`；L962 `self.engine.batch_transfer_sync_read(session_id, src_list, dst_list, length_list)`。
- `_mark_failed_recv_request`（L613）：`self.invalid_block_ids.update(local_block_ids[0])`。
- `_handle_request`（L705-755）：`finally` 中 `_send_done_signal_to_free_remote_port`（`remote_port_send_num` 为空则早退）与 `_send_done_recv_signal`（真实 socket 收发）；`request_queue.task_done()`（L750）——直接调用 `_handle_request` 的测试必须先 `request_queue.put(...)` 并 patch 掉两个 send 方法。
- 现有测试先例：`TestKVCacheSendingThread.test_run_handles_get_meta_and_done_recv_msgs`（L217）用真实线程 + DEALER socket 做 ZMQ 往返；`TestKVCacheRecvingThreadBasic.setUp`（L779）构造真实 `KVCacheRecvingThread`（MagicMock engine）；端口计算 `actual_port = base_port + (pp_rank * tp_size + tp_rank + pcp_rank * prefill_tp_size)`。
- `vllm_ascend/envs.py`：`env_variables` dict + 模块级 `__getattr__` 惰性求值（访问时读 `os.getenv`），布尔开关惯例 `bool(int(os.getenv(..., "0")))`。

---

### 任务 1：env 开关 + 消息常量 + 异常类

**文件：**
- 修改：`vllm_ascend/envs.py`（`env_variables` dict 末尾）
- 修改：`vllm_ascend/distributed/kv_transfer/kv_p2p/mooncake_connector.py`（import 区 L26 附近、常量区 `GET_META_MSG` 旁）
- 测试：`tests/ut/kv_offload/test_mooncake_connector.py`（顶部 import 块 + 文件末尾新增 `TestVerifyReq`）

- [ ] **步骤 1：扩充测试文件 import 并编写失败测试**

在 `tests/ut/kv_offload/test_mooncake_connector.py` 的模块级 import 块（L63-84 的 `from vllm_ascend.distributed... import (...)`）中追加符号，并在文件顶部 import 区加两行：

```python
from vllm import envs as vllm_envs
from vllm_ascend import envs as ascend_envs
```

import 块追加（按字母序插入现有列表）：

```python
    KVCacheVerifyExpiredError,
    VERIFY_GRACE_SECONDS,
    VERIFY_REQ_MSG,
    VERIFY_RESP_MSG,
    VERIFY_STATUS_EXPIRED,
    VERIFY_STATUS_VALID,
```

在文件末尾追加测试类：

```python
class TestVerifyReq(unittest.TestCase):
    """VERIFY_REQ Phase 1 单元测试（规格 §8）。"""

    def test_message_constants(self):
        self.assertEqual(VERIFY_REQ_MSG, b"verify_req_msg")
        self.assertEqual(VERIFY_RESP_MSG, b"verify_resp_msg")
        self.assertEqual(VERIFY_STATUS_VALID, b"VALID")
        self.assertEqual(VERIFY_STATUS_EXPIRED, b"EXPIRED")
        self.assertEqual(VERIFY_GRACE_SECONDS, 10)

    def test_verify_expired_error_is_exception(self):
        err = KVCacheVerifyExpiredError("p_req_1 expired")
        self.assertIsInstance(err, Exception)
        self.assertIn("p_req_1", str(err))

    def test_env_switch_default_off(self):
        with patch.dict(os.environ, {"VLLM_ASCEND_VERIFY_KV_BEFORE_PULL": "0"}):
            self.assertFalse(ascend_envs.VLLM_ASCEND_VERIFY_KV_BEFORE_PULL)

    def test_env_switch_enable(self):
        with patch.dict(os.environ, {"VLLM_ASCEND_VERIFY_KV_BEFORE_PULL": "1"}):
            self.assertTrue(ascend_envs.VLLM_ASCEND_VERIFY_KV_BEFORE_PULL)
```

- [ ] **步骤 2：运行测试验证失败**

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/test_mooncake_connector.py::TestVerifyReq -x -q
```
预期：FAIL，`ImportError: cannot import name 'KVCacheVerifyExpiredError'`。

- [ ] **步骤 3：实现 env 开关**

`vllm_ascend/envs.py` 的 `env_variables` dict 末尾（`"VLLM_ASCEND_ENABLE_BATCH_MEMCPY"` 条目之后、闭合 `}` 之前）追加：

```python
    # Whether the decode (kv_consumer) node verifies with the prefill node via a
    # ZMQ VERIFY_REQ message that the remote KV blocks are still held, before
    # issuing the one-sided KV pull. Prevents dirty/stale KV reads when the
    # prefill node force-freed timed-out blocks (vllm-ascend issue #15420).
    # Default off. The P-side VERIFY_REQ handler is unconditional.
    "VLLM_ASCEND_VERIFY_KV_BEFORE_PULL": lambda: bool(int(os.getenv("VLLM_ASCEND_VERIFY_KV_BEFORE_PULL", "0"))),
```

- [ ] **步骤 4：实现常量与异常类**

`vllm_ascend/distributed/kv_transfer/kv_p2p/mooncake_connector.py`：

import 区（L26 `from vllm import envs` 之后）追加：

```python
from vllm_ascend import envs as ascend_envs
```

常量区 `GET_META_MSG`/`DONE_RECVING_MSG` 旁追加：

```python
VERIFY_REQ_MSG = b"verify_req_msg"
VERIFY_RESP_MSG = b"verify_resp_msg"
VERIFY_STATUS_VALID = b"VALID"
VERIFY_STATUS_EXPIRED = b"EXPIRED"
# Deadline floor granted by a successful VERIFY: the request's remaining
# delayed-free lifetime is topped up to this many seconds (never shortened).
VERIFY_GRACE_SECONDS = 10
VERIFY_STATS_SUMMARY_INTERVAL_SECONDS = 60
# Upper bound for the short-lived recently_force_freed bookkeeping (P side).
MAX_RECENTLY_FORCE_FREED = 1024


class KVCacheVerifyExpiredError(Exception):
    """Raised on the D side when the P side reports (or verify cannot confirm)
    that the remote KV blocks for a request are no longer held."""
```

- [ ] **步骤 5：运行测试验证通过**

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/test_mooncake_connector.py::TestVerifyReq -x -q
```
预期：4 passed。

- [ ] **步骤 6：Commit**

```bash
git add vllm_ascend/envs.py vllm_ascend/distributed/kv_transfer/kv_p2p/mooncake_connector.py tests/ut/kv_offload/test_mooncake_connector.py
git commit -m "feat: add VERIFY_REQ message constants and env switch for verify-before-pull"
```

---

### 任务 2：P 侧 `check_and_extend` + force-free 降噪

**文件：**
- 修改：`vllm_ascend/distributed/kv_transfer/kv_p2p/mooncake_connector.py`（`KVCacheTaskTracker.__init__` L167-177、`update_done_task_count` L187-200、`_retrieve_expired_requests` L221-242、`add_delayed_request` 之后新增方法）
- 测试：`tests/ut/kv_offload/test_mooncake_connector.py`（`TestVerifyReq` 追加）

- [ ] **步骤 1：编写失败测试**

`TestVerifyReq` 中追加（`timeout` 取自 env 而非硬编码 480，保证测试对配置鲁棒）：

```python
    def setUp(self):
        self.tracker = KVCacheTaskTracker()

    def _add_held_request(self, request_id: str, delay_start_time: float):
        self.tracker.add_req_to_process(request_id)
        self.tracker.add_delayed_request(request_id, delay_start_time)

    def test_check_and_extend_valid(self):
        self._add_held_request("req_1", time.time())
        self.assertTrue(self.tracker.check_and_extend("req_1"))

    def test_check_and_extend_after_force_free(self):
        self._add_held_request("req_1", time.time() - 10**6)
        self.tracker.get_and_clear_finished_requests()  # 触发 force-free
        self.assertFalse(self.tracker.check_and_extend("req_1"))

    def test_check_and_extend_after_done(self):
        self._add_held_request("req_1", time.time())
        self.tracker.update_done_task_count("req_1")
        self.assertFalse(self.tracker.check_and_extend("req_1"))

    def test_check_and_extend_unknown_request(self):
        self.assertFalse(self.tracker.check_and_extend("ghost"))

    def test_verify_extends_deadline(self):
        timeout = vllm_envs.VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT
        start = time.time()
        # 剩余仅 1s：< G=10s，应被顶到 G
        self._add_held_request("req_1", start - (timeout - 1.0))
        with patch("time.time", return_value=start):
            self.assertTrue(self.tracker.check_and_extend("req_1"))
        deadline = self.tracker.delayed_free_requests["req_1"]
        remaining = deadline + timeout - start
        self.assertAlmostEqual(remaining, VERIFY_GRACE_SECONDS, delta=0.5)

    def test_verify_never_shortens_window(self):
        start = time.time()
        self._add_held_request("req_1", start)  # 剩余 ~480s ≥ G：值必须不动
        value_before = self.tracker.delayed_free_requests["req_1"]
        self.assertTrue(self.tracker.check_and_extend("req_1"))
        self.assertEqual(self.tracker.delayed_free_requests["req_1"], value_before)

    def test_check_and_extend_does_not_create_entry(self):
        # 在 reqs_to_process 但尚未进入延迟释放（不可能被 force-free）：不凭空建条目
        self.tracker.add_req_to_process("req_1")
        self.assertTrue(self.tracker.check_and_extend("req_1"))
        self.assertNotIn("req_1", self.tracker.delayed_free_requests)

    def test_grace_boundary_with_injected_clock(self):
        timeout = vllm_envs.VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT
        start = time.time()
        self._add_held_request("req_1", start - (timeout - 1.0))
        with patch("time.time", return_value=start):
            self.assertTrue(self.tracker.check_and_extend("req_1"))
        # G - ε：仍存活（竞态关闭的确定性证明）
        with patch("time.time", return_value=start + VERIFY_GRACE_SECONDS - 0.1):
            self.assertEqual(self.tracker._retrieve_expired_requests(), set())
        # G + ε：已过期并被 force-free
        with patch("time.time", return_value=start + VERIFY_GRACE_SECONDS + 0.1):
            self.assertEqual(self.tracker._retrieve_expired_requests(), {"req_1"})

    def test_done_after_force_free_is_debug(self):
        logger_name = "vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector"
        self._add_held_request("req_1", time.time() - 10**6)
        self.tracker.get_and_clear_finished_requests()
        self.assertIn("req_1", self.tracker.recently_force_freed)
        with self.assertLogs(logger_name, level="DEBUG") as cm:
            self.tracker.update_done_task_count("req_1")
        self.assertTrue(any(o.startswith("DEBUG:") and "force-free" in o for o in cm.output))
        self.assertNotIn("req_1", self.tracker.recently_force_freed)

    def test_done_for_unknown_request_still_warning(self):
        logger_name = "vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector"
        with self.assertLogs(logger_name, level="WARNING") as cm:
            self.tracker.update_done_task_count("never_seen")
        self.assertTrue(any(o.startswith("WARNING:") and "never_seen" in o for o in cm.output))
```

注意：`setUp` 会让该类**所有**用例（含任务 1 已写的四个）都执行 `self.tracker = KVCacheTaskTracker()`——任务 1 的用例不依赖 `setUp`，不受影响。

- [ ] **步骤 2：运行测试验证失败**

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/test_mooncake_connector.py::TestVerifyReq -x -q
```
预期：FAIL，`AttributeError: 'KVCacheTaskTracker' object has no attribute 'check_and_extend'`。

- [ ] **步骤 3：实现 `check_and_extend` 与降噪**

`mooncake_connector.py` 的 `KVCacheTaskTracker.__init__`（L177 `self.reqs_to_process` 之后）追加：

```python
        # Short-lived bookkeeping of force-freed request ids. A late
        # DONE_RECVING for these is expected (the D side skipped the pull after
        # a failed verify) and is logged at debug instead of warning.
        self.recently_force_freed: OrderedDict[str, float] = OrderedDict()
```

`add_delayed_request`（L219）之后新增方法：

```python
    def check_and_extend(self, request_id: str) -> bool:
        """Verify a request's blocks are still held and extend its deadline.

        Returns False when the request has been released (force-freed or done),
        True otherwise. Extension applies the VERIFY grace floor: the remaining
        delayed-free lifetime is topped up to VERIFY_GRACE_SECONDS and never
        shortened. Only entries already in delayed_free_requests are touched —
        a request tracked but not yet delayed-freed cannot be force-freed.
        """
        with self.done_task_lock:
            if request_id not in self.reqs_to_process:
                return False
            if request_id in self.delayed_free_requests:
                deadline_floor = time.time() - (
                    envs.VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT - VERIFY_GRACE_SECONDS
                )
                if deadline_floor > self.delayed_free_requests[request_id]:
                    self.delayed_free_requests[request_id] = deadline_floor
            return True
```

`_retrieve_expired_requests` 的过期分支（`expired_requests.add(request_id)` 之后）追加：

```python
                self.recently_force_freed[request_id] = current_time
                if len(self.recently_force_freed) > MAX_RECENTLY_FORCE_FREED:
                    self.recently_force_freed.popitem(last=False)
```

`update_done_task_count` 的 `else` 分支（L193-200）整体替换为：

```python
            else:
                if self.recently_force_freed.pop(request_id, None) is not None:
                    logger.debug(
                        "MooncakeConnector received late DONE_RECVING after "
                        "force-free (expected after a failed VERIFY). "
                        "request_id=%s.",
                        request_id,
                    )
                else:
                    logger.warning(
                        "MooncakeConnector finish req not in reqs to process. "
                        "request_id=%s. "
                        "Possible cause: Request was already completed or not properly tracked. "
                        "Check: Verify request lifecycle and tracking logic.",
                        request_id,
                    )
```

- [ ] **步骤 4：运行测试验证通过**

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/test_mooncake_connector.py::TestVerifyReq -x -q
```
预期：14 passed（任务 1 的 4 个 + 本任务 10 个）。

- [ ] **步骤 5：运行现有 tracker 测试确认无回归**

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/test_mooncake_connector.py::TestKVCacheTaskTracker tests/ut/kv_offload/test_mooncake_connector.py::TestGetAndClearFinishedSingleRequests tests/ut/kv_offload/test_mooncake_connector.py::TestGetAndClearFinishedRequests -q
```
预期：全部 passed（`_retrieve_expired_requests`/`update_done_task_count` 的既有行为未变）。

- [ ] **步骤 6：Commit**

```bash
git add vllm_ascend/distributed/kv_transfer/kv_p2p/mooncake_connector.py tests/ut/kv_offload/test_mooncake_connector.py
git commit -m "feat: add KVCacheTaskTracker.check_and_extend with verify grace floor and force-free DONE noise reduction"
```

---

### 任务 3：P 侧 `run_busy_loop` VERIFY 分支（真实线程 ZMQ 测试）

**文件：**
- 修改：`vllm_ascend/distributed/kv_transfer/kv_p2p/mooncake_connector.py`（`run_busy_loop` L362-387 的 if-elif 链）
- 测试：`tests/ut/kv_offload/test_mooncake_connector.py`（`TestVerifyReq` 追加）

- [ ] **步骤 1：编写失败测试**

`TestVerifyReq` 追加（沿用 `test_run_handles_get_meta_and_done_recv_msgs` 的真实线程模式）：

```python
    def _start_sending_thread(self):
        ready_event = threading.Event()
        metadata = make_agent_metadata(engine_id="engine1", kv_caches_base_addr=[[12345678]], num_blocks=2)
        host = "127.0.0.1"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            base_port = s.getsockname()[1]
        thread = KVCacheSendingThread(
            tp_rank=0,
            prefill_tp_size=1,
            local_engine_id="engine1",
            side_channel_host=host,
            side_channel_port=base_port,
            metadata=metadata,
            vllm_config=MockVllmConfig(),
            ready_event=ready_event,
            kv_caches={},
            pcp_rank=0,
        )
        thread.start()
        actual_port = base_port + (
            thread.pp_rank * thread.tp_size + thread.tp_rank + thread.pcp_rank * thread.prefill_tp_size
        )
        self.assertTrue(ready_event.wait(timeout=3), "Server thread startup timeout")
        context = zmq.Context()
        sock = context.socket(zmq.DEALER)
        sock.setsockopt(zmq.RCVTIMEO, 1000)
        sock.connect(f"tcp://{host}:{actual_port}")
        return thread, sock, actual_port

    def _verify_roundtrip(self, sock, transfer_id):
        encoder = msgspec.msgpack.Encoder()
        decoder = msgspec.msgpack.Decoder(type=tuple)
        sock.send_multipart([b"", encoder.encode((VERIFY_REQ_MSG, transfer_id))])
        frames = sock.recv_multipart()
        self.assertEqual(frames[0], b"")
        return decoder.decode(frames[1])

    def test_run_busy_loop_verify_valid(self):
        thread, sock, _ = self._start_sending_thread()
        try:
            thread.task_tracker.add_req_to_process("p_req_1")
            thread.task_tracker.add_delayed_request("p_req_1", time.time())
            msg = self._verify_roundtrip(sock, "p_req_1")
            self.assertEqual(msg[0], VERIFY_RESP_MSG)
            self.assertEqual(msg[1], VERIFY_STATUS_VALID)
        finally:
            sock.close()

    def test_run_busy_loop_verify_expired(self):
        thread, sock, _ = self._start_sending_thread()
        try:
            msg = self._verify_roundtrip(sock, "p_req_gone")
            self.assertEqual(msg[0], VERIFY_RESP_MSG)
            self.assertEqual(msg[1], VERIFY_STATUS_EXPIRED)
        finally:
            sock.close()

    def test_run_busy_loop_verify_malformed_no_reply(self):
        thread, sock, _ = self._start_sending_thread()
        try:
            encoder = msgspec.msgpack.Encoder()
            sock.send_multipart([b"", encoder.encode((VERIFY_REQ_MSG, 12345))])  # 非 str payload
            with self.assertRaises(zmq.Again):
                sock.recv_multipart()  # 不回包 → D 侧按超时降级
        finally:
            sock.close()
```

- [ ] **步骤 2：运行测试验证失败**

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/test_mooncake_connector.py::TestVerifyReq -k verify_valid -x -q
```
预期：FAIL——线程落入 `else` 分支记错误日志且不回包，`sock.recv_multipart()` 抛 `zmq.Again`（RCVTIMEO 1s）。

- [ ] **步骤 3：实现分支**

`run_busy_loop` 中 `elif msg[0] == DONE_RECVING_MSG:` 分支结束（ACK 重试 while 循环之后、`else:` 之前，L387 附近）插入：

```python
                elif msg[0] == VERIFY_REQ_MSG:
                    # Passive handler: always answered regardless of the
                    # D-side VLLM_ASCEND_VERIFY_KV_BEFORE_PULL switch. Keep
                    # the critical section O(1) — all state access goes
                    # through check_and_extend under done_task_lock.
                    if len(msg) != 2 or not isinstance(msg[1], str):
                        logger.error(
                            "Invalid VERIFY_REQ_MSG payload. "
                            "Expected: (VERIFY_REQ_MSG, str transfer_id). "
                            "Actual: %s.",
                            msg,
                        )
                    else:
                        is_valid = self.task_tracker.check_and_extend(msg[1])
                        resp_payload = encoder.encode(
                            (VERIFY_RESP_MSG, VERIFY_STATUS_VALID if is_valid else VERIFY_STATUS_EXPIRED)
                        )
                        sock.send_multipart((identity, b"", resp_payload))
```

- [ ] **步骤 4：运行测试验证通过**

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/test_mooncake_connector.py::TestVerifyReq -x -q
```
预期：17 passed。

- [ ] **步骤 5：运行现有 sending-thread 测试确认无回归**

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/test_mooncake_connector.py::TestKVCacheSendingThread tests/ut/kv_offload/test_mooncake_connector.py::TestMainThreadLoop -q
```
预期：全部 passed。

- [ ] **步骤 6：Commit**

```bash
git add vllm_ascend/distributed/kv_transfer/kv_p2p/mooncake_connector.py tests/ut/kv_offload/test_mooncake_connector.py
git commit -m "feat: handle VERIFY_REQ in P-side run_busy_loop with check_and_extend reply"
```

---

### 任务 4：D 侧 `_verify_remote_blocks_held` + 开关字段 + 计数器

**文件：**
- 修改：`vllm_ascend/distributed/kv_transfer/kv_p2p/mooncake_connector.py`（`KVCacheRecvingThread.__init__` L505 之后、`_return_remote_socket` L1470 之后新增方法）
- 测试：`tests/ut/kv_offload/test_mooncake_connector.py`（`TestVerifyReq` 追加）

- [ ] **步骤 1：编写失败测试**

`TestVerifyReq` 追加：

```python
    def _make_recv_thread(self, env_value: str = "1") -> KVCacheRecvingThread:
        with patch.dict(os.environ, {"VLLM_ASCEND_VERIFY_KV_BEFORE_PULL": env_value}):
            thread = KVCacheRecvingThread(
                tp_rank=0,
                tp_size=4,
                _prefill_pp_size=1,
                engine=MagicMock(),
                local_engine_id="local_engine",
                local_handshake_port=5555,
                side_channel_port=30000,
                local_kv_caches_base_addr=[[0x1000], [0x2000]],
                block_len_per_addr=[[1024], [2048]],
                block_stride_per_addr=[[1024], [2048]],
                ready_event=threading.Event(),
                vllm_config=MockVllmConfig(),
                kv_caches={},
                prefill_pp_layer_partition=None,
            )
        thread.remote_sockets = defaultdict(deque)
        return thread

    def _make_req_meta(self):
        return {
            "request_id": "d_req_1",
            "remote_request_id": "p_req_1",
            "local_block_ids": [[101, 102]],
            "remote_block_ids": [[201, 202]],
            "local_block_ids_replicate_k": tuple(),
            "remote_block_ids_replicate_k": tuple(),
            "group_pulls": [],
            "remote_engine_id": "remote_engine",
            "remote_host": "10.0.0.1",
            "remote_handshake_port": 7777,
            "num_computed_tokens": 0,
            "remote_port_send_num": {},
            "all_task_done": True,
            "shard_idx": 0,
            "remote_block_size": None,
        }

    def test_verify_disabled_by_default(self):
        thread = self._make_recv_thread(env_value="0")
        self.assertFalse(thread.verify_before_pull_enabled)

    def test_verify_enabled_via_env(self):
        thread = self._make_recv_thread(env_value="1")
        self.assertTrue(thread.verify_before_pull_enabled)

    @patch("vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector.ensure_zmq_recv")
    @patch("vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector.ensure_zmq_send")
    def test_verify_remote_blocks_held_valid(self, mock_send, mock_recv):
        thread = self._make_recv_thread()
        mock_recv.return_value = thread.encoder.encode((VERIFY_RESP_MSG, VERIFY_STATUS_VALID))
        self.assertTrue(thread._verify_remote_blocks_held(self._make_req_meta()))
        self.assertEqual(thread.verify_stats["valid"], 1)
        mock_send.assert_called_once()

    @patch("vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector.ensure_zmq_recv")
    @patch("vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector.ensure_zmq_send")
    def test_verify_remote_blocks_held_expired(self, mock_send, mock_recv):
        thread = self._make_recv_thread()
        mock_recv.return_value = thread.encoder.encode((VERIFY_RESP_MSG, VERIFY_STATUS_EXPIRED))
        self.assertFalse(thread._verify_remote_blocks_held(self._make_req_meta()))
        self.assertEqual(thread.verify_stats["expired"], 1)

    @patch("vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector.ensure_zmq_recv")
    @patch("vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector.ensure_zmq_send")
    def test_verify_remote_blocks_held_timeout(self, mock_send, mock_recv):
        thread = self._make_recv_thread()
        mock_recv.side_effect = RuntimeError("Failed to receive data after 3 retries")
        self.assertFalse(thread._verify_remote_blocks_held(self._make_req_meta()))
        self.assertEqual(thread.verify_stats["timeout"], 1)

    @patch("vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector.ensure_zmq_recv")
    @patch("vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector.ensure_zmq_send")
    def test_verify_error_closes_socket_not_returned(self, mock_send, mock_recv):
        thread = self._make_recv_thread()
        mock_sock = MagicMock()
        with patch.object(thread, "_get_remote_socket", return_value=mock_sock):
            mock_recv.side_effect = RuntimeError("boom")
            self.assertFalse(thread._verify_remote_blocks_held(self._make_req_meta()))
        mock_sock.close.assert_called_once()
        target_path = make_zmq_path("tcp", "10.0.0.1", 7777)
        self.assertEqual(len(thread.remote_sockets[target_path]), 0)

    @patch("vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector.ensure_zmq_recv")
    @patch("vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector.ensure_zmq_send")
    def test_verify_success_returns_socket_to_pool(self, mock_send, mock_recv):
        thread = self._make_recv_thread()
        mock_sock = MagicMock()
        with patch.object(thread, "_get_remote_socket", return_value=mock_sock):
            mock_recv.return_value = thread.encoder.encode((VERIFY_RESP_MSG, VERIFY_STATUS_VALID))
            self.assertTrue(thread._verify_remote_blocks_held(self._make_req_meta()))
        mock_sock.close.assert_not_called()
        target_path = make_zmq_path("tcp", "10.0.0.1", 7777)
        self.assertEqual(len(thread.remote_sockets[target_path]), 1)

    def test_verify_counters_summary(self):
        thread = self._make_recv_thread()
        with patch.object(thread, "_record_verify_stat") as mock_record:
            thread._record_verify_stat("valid")
        mock_record.assert_called_once_with("valid")
```

（`test_verify_counters_summary` 只锁接口存在性；三类计数器的行为已由前三个用例覆盖。）

- [ ] **步骤 2：运行测试验证失败**

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/test_mooncake_connector.py::TestVerifyReq -k "remote_blocks_held or disabled_by_default or enabled_via" -x -q
```
预期：FAIL，`AttributeError: 'KVCacheRecvingThread' object has no attribute 'verify_before_pull_enabled'`。

- [ ] **步骤 3：实现**

`KVCacheRecvingThread.__init__` 的 `self.timeout = 1.0`（L505）之后追加：

```python
        # VERIFY before pull (issue #15420). Read once at startup — the hot
        # path must not hit the env lookup.
        self.verify_before_pull_enabled = ascend_envs.VLLM_ASCEND_VERIFY_KV_BEFORE_PULL
        self.verify_stats = {"valid": 0, "expired": 0, "timeout": 0}
        self.verify_stats_lock = threading.Lock()
        self._last_verify_summary_time = time.time()
        self.verify_resp_decoder = msgspec.msgpack.Decoder(type=tuple)
```

`_return_remote_socket`（L1470）之后新增方法：

```python
    def _record_verify_stat(self, result: str) -> None:
        with self.verify_stats_lock:
            self.verify_stats[result] += 1
            now = time.time()
            elapsed = now - self._last_verify_summary_time
            if elapsed >= VERIFY_STATS_SUMMARY_INTERVAL_SECONDS:
                logger.info(
                    "KV verify stats (last %.0fs): valid=%d, expired=%d, timeout=%d. "
                    "timeout may indicate an older P version or network issues.",
                    elapsed,
                    self.verify_stats["valid"],
                    self.verify_stats["expired"],
                    self.verify_stats["timeout"],
                )
                self._last_verify_summary_time = now
                for key in self.verify_stats:
                    self.verify_stats[key] = 0

    def _verify_remote_blocks_held(self, req_meta: dict[str, Any]) -> bool:
        """Ask P whether the remote blocks for this request are still held.

        Returns False on EXPIRED or on any failure (timeout/protocol error):
        conservative — the pull is skipped and the request recomputes. A
        well-formed exchange returns the pooled socket; a failed one closes it
        (a timed-out REQ socket must never be reused, its late reply would
        corrupt the next exchange).
        """
        remote_request_id = req_meta["remote_request_id"]
        remote_host = req_meta["remote_host"]
        remote_handshake_port = req_meta["remote_handshake_port"]
        target = f"{remote_host}:{remote_handshake_port}"
        sock: zmq.Socket | None = None
        reusable = False
        try:
            sock = self._get_remote_socket(remote_host, remote_handshake_port)
            payload = self.encoder.encode((VERIFY_REQ_MSG, remote_request_id))
            ensure_zmq_send(sock, payload, target)
            resp = ensure_zmq_recv(sock, target)
            decoded = self.verify_resp_decoder.decode(resp)
            reusable = True
            if len(decoded) == 2 and decoded[0] == VERIFY_RESP_MSG and decoded[1] == VERIFY_STATUS_VALID:
                self._record_verify_stat("valid")
                logger.debug("KV verify passed for request %s at %s.", remote_request_id, target)
                return True
            self._record_verify_stat("expired")
            logger.warning(
                "Remote KV blocks expired on P side, skipping pull. "
                "remote_request_id=%s, source=%s.",
                remote_request_id,
                target,
            )
            return False
        except Exception as e:
            self._record_verify_stat("timeout")
            logger.warning(
                "KV verify failed, treating as expired and skipping pull. This may be "
                "caused by an older P node version or a network issue. "
                "remote_request_id=%s, source=%s, error=%s.",
                remote_request_id,
                target,
                e,
            )
            return False
        finally:
            if sock is not None:
                if reusable:
                    self._return_remote_socket(sock, remote_host, remote_handshake_port)
                else:
                    sock.close()
```

- [ ] **步骤 4：运行测试验证通过**

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/test_mooncake_connector.py::TestVerifyReq -x -q
```
预期：25 passed。

- [ ] **步骤 5：Commit**

```bash
git add vllm_ascend/distributed/kv_transfer/kv_p2p/mooncake_connector.py tests/ut/kv_offload/test_mooncake_connector.py
git commit -m "feat: add D-side _verify_remote_blocks_held with pooled REQ socket, close-on-error and verify stats"
```

---

### 任务 5：D 侧集成——`_transfer_kv_cache_all_groups` 插入校验 + `_handle_request` 失败路径

**文件：**
- 修改：`vllm_ascend/distributed/kv_transfer/kv_p2p/mooncake_connector.py`（`_transfer_kv_cache_all_groups` L788-790 之后）
- 测试：`tests/ut/kv_offload/test_mooncake_connector.py`（`TestVerifyReq` 追加）

- [ ] **步骤 1：编写失败测试**

`TestVerifyReq` 追加（`_make_recv_thread`/`_make_req_meta` 复用任务 4 的 helper）：

```python
    def _prepare_remote_metadata(self, thread: KVCacheRecvingThread):
        thread.kv_caches_base_addr["remote_engine"][7777] = [[111]]
        thread.kv_caches_base_addr["local_engine"][5555] = [[0x1000]]
        thread.remote_te_port["remote_engine"][7777] = 8888
        thread.remote_block_stride_per_addr["remote_engine"][7777] = [[64]]

    @patch("vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector.ensure_zmq_recv")
    @patch("vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector.ensure_zmq_send")
    def test_transfer_verify_pass_continues(self, mock_send, mock_recv):
        thread = self._make_recv_thread()
        self._prepare_remote_metadata(thread)
        mock_recv.return_value = thread.encoder.encode((VERIFY_RESP_MSG, VERIFY_STATUS_VALID))
        req_meta = self._make_req_meta()
        # group_pulls=[] → 通过校验后构建 src_list 为空 → 干净返回，不发起读
        thread._transfer_kv_cache_all_groups(req_meta)
        self.assertEqual(thread.verify_stats["valid"], 1)

    @patch("vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector.ensure_zmq_recv")
    @patch("vllm_ascend.distributed.kv_transfer.kv_p2p.mooncake_connector.ensure_zmq_send")
    def test_transfer_verify_expired_raises(self, mock_send, mock_recv):
        thread = self._make_recv_thread()
        self._prepare_remote_metadata(thread)
        mock_recv.return_value = thread.encoder.encode((VERIFY_RESP_MSG, VERIFY_STATUS_EXPIRED))
        with self.assertRaises(KVCacheVerifyExpiredError):
            thread._transfer_kv_cache_all_groups(self._make_req_meta())
        self.assertEqual(thread.verify_stats["expired"], 1)

    def test_transfer_verify_expired_skips_engine_read(self):
        thread = self._make_recv_thread()
        self._prepare_remote_metadata(thread)
        with patch.object(thread, "_verify_remote_blocks_held", return_value=False):
            with self.assertRaises(KVCacheVerifyExpiredError):
                thread._transfer_kv_cache_all_groups(self._make_req_meta())
        thread.engine.batch_transfer_sync_read.assert_not_called()

    def test_transfer_verify_enabled_skips_when_zero_blocks(self):
        thread = self._make_recv_thread()
        req_meta = self._make_req_meta()
        req_meta["local_block_ids"] = [[]]  # full prefix hit：零块早退
        with patch.object(thread, "_verify_remote_blocks_held") as mock_verify:
            thread._transfer_kv_cache_all_groups(req_meta)
        mock_verify.assert_not_called()

    def test_handle_request_expired_marks_failed(self):
        thread = self._make_recv_thread()
        self._prepare_remote_metadata(thread)
        thread.task_tracker.add_req_to_process("d_req_1")
        thread.request_queue.put({"request_id": "d_req_1"})
        req_meta = self._make_req_meta()
        with (
            patch.object(thread, "_verify_remote_blocks_held", return_value=False),
            patch.object(thread, "_send_done_recv_signal"),
            patch.object(thread, "_send_done_signal_to_free_remote_port"),
        ):
            thread._handle_request(req_meta)  # 不得抛出：异常在 except 中转为 failed-recv
        self.assertEqual(thread.invalid_block_ids, {101, 102})
        self.assertIn("d_req_1", thread.task_tracker.finished_requests)
        thread.engine.batch_transfer_sync_read.assert_not_called()
```

- [ ] **步骤 2：运行测试验证失败**

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/test_mooncake_connector.py::TestVerifyReq -k "transfer_verify or handle_request_expired" -x -q
```
预期：FAIL——`test_transfer_verify_expired_raises` 等用例中 `_transfer_kv_cache_all_groups` 未调用 verify（不抛 `KVCacheVerifyExpiredError` 而是继续走 group_pulls 空循环干净返回），断言失败。

- [ ] **步骤 3：实现插入**

`_transfer_kv_cache_all_groups` 的零块早退（L789-790）之后、`with self.remote_metadata_lock:` 之前插入：

```python
        # Verify the remote blocks are still held before the one-sided read
        # (issue #15420). Zero-block (full prefix hit) requests never read and
        # skip this. A failed verify raises and is converted to the existing
        # failed-recv path by _handle_request's except clause.
        if self.verify_before_pull_enabled and not self._verify_remote_blocks_held(req_meta):
            raise KVCacheVerifyExpiredError(
                f"Remote KV blocks expired on P side before pull. "
                f"remote_request_id={remote_request_id}, "
                f"source={remote_host}:{remote_handshake_port}"
            )
```

- [ ] **步骤 4：运行测试验证通过**

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/test_mooncake_connector.py::TestVerifyReq -x -q
```
预期：30 passed。

- [ ] **步骤 5：Commit**

```bash
git add vllm_ascend/distributed/kv_transfer/kv_p2p/mooncake_connector.py tests/ut/kv_offload/test_mooncake_connector.py
git commit -m "feat: verify remote blocks held before KV pull, raising KVCacheVerifyExpiredError into failed-recv path"
```

---

### 任务 6：ZMQ 全链路往返 + 全量回归

**文件：**
- 测试：`tests/ut/kv_offload/test_mooncake_connector.py`（`TestVerifyReq` 追加）

- [ ] **步骤 1：编写端到端往返测试（真实 P 线程 + 真实 D 逻辑 + 真实 REQ socket，无 mock）**

`TestVerifyReq` 追加——`_make_req_meta` 的 remote 端点直接指向任务 3 启动的真实 sending 线程端口，
D 侧 `_verify_remote_blocks_held` 走其自身 socket 池创建**真实 REQ**（REQ/ROUTER 生产配对、
REQ 的空帧自动剥离使 `ensure_zmq_recv` 单帧读得到完整 payload）：

```python
    def test_zmq_verify_roundtrip_end_to_end(self):
        thread_p, _unused_sock, actual_port = self._start_sending_thread()
        thread_p.task_tracker.add_req_to_process("p_req_live")
        thread_p.task_tracker.add_delayed_request("p_req_live", time.time())
        thread_d = self._make_recv_thread()
        for transfer_id, expect_held in (("p_req_live", True), ("p_req_gone", False)):
            req_meta = self._make_req_meta()
            req_meta["remote_request_id"] = transfer_id
            req_meta["remote_host"] = "127.0.0.1"
            req_meta["remote_handshake_port"] = actual_port
            held = thread_d._verify_remote_blocks_held(req_meta)
            self.assertEqual(held, expect_held)
        self.assertEqual(thread_d.verify_stats["valid"], 1)
        self.assertEqual(thread_d.verify_stats["expired"], 1)
```

说明：第二次 verify 复用池中 REQ socket（完整 send/recv 交换后 REQ 状态机干净，可安全复用）——
同时验证了池化复用路径。用例结束不 close（daemon 线程与 socket 随测试进程回收，与现有测试一致）。

- [ ] **步骤 2：运行测试验证通过**

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/test_mooncake_connector.py::TestVerifyReq -x -q
```
预期：31 passed。

- [ ] **步骤 3：全量回归**

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/test_mooncake_connector.py -q
```
预期：全文件 passed（既有全部测试类 + TestVerifyReq，0 failed）。

```bash
env TORCH_DEVICE_BACKEND_AUTOLOAD=0 /home/gao/code/python/vllm-ascend-venv/.venv/bin/python -m pytest tests/ut/kv_offload/ -q
```
预期：目录全量 passed（确认未破坏 hybrid/layerwise/ascend_store 测试）。

- [ ] **步骤 4：Lint**

```bash
cd /home/gao/code/python/vllm-ascend && uvx ruff check vllm_ascend/envs.py vllm_ascend/distributed/kv_transfer/kv_p2p/mooncake_connector.py tests/ut/kv_offload/test_mooncake_connector.py
```
预期：无 error（若 ruff 不可用则跳过并记录）。

- [ ] **步骤 5：Commit**

```bash
git add tests/ut/kv_offload/test_mooncake_connector.py
git commit -m "test: add end-to-end ZMQ verify roundtrip between real P thread and D-side verify logic"
```

---

## 自检记录（计划完成后执行）

1. **规格覆盖度**：§4.1 消息常量 → 任务 1；§4.3/§4.4 判定与续命 → 任务 2；§6 row 4/5 → 任务 2/3；§6 row 6 → 任务 4；§6 row 7 → 任务 5；§6 row 8 可观测性 → 任务 2（P 侧降噪）/任务 4（D 侧计数器+分类日志）；§6 row 1 → 任务 1；§8 全部测试行 → 任务 1-6；§3 目标 3（开关关闭路径不变）→ 任务 4/5 的 disabled-by-default 与 skip 测试。无遗漏。
2. **占位符扫描**：所有步骤含完整代码，无 TODO/待定。
3. **类型一致性**：`check_and_extend(request_id: str) -> bool`（任务 2 定义，任务 3 调用）；`_verify_remote_blocks_held(req_meta) -> bool`（任务 4 定义，任务 5/6 调用）；`_record_verify_stat(result: str)`（任务 4 定义，任务 4 测试调用）；`_start_sending_thread` 签名在任务 6 的签名变更已同步任务 3 用例的说明写入任务 6 步骤 1。
