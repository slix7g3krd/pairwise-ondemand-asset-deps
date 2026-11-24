"""六类线上问题的回归测试。

覆盖：分片级校验、依赖图优先级、解压/卸载引用计数、弱网重排、
清单版本与缓存失效、断点续传偏移校验，以及调度确定性。
"""

import pytest

import gameplay.ondemand_asset_deps as mod


# ------------------------------------------------------------ 公共夹具

@pytest.fixture
def graph():
    """带类型、优先级、体积与依赖边的资源图。"""
    return {
        "core": {"type": "core", "priority": 100, "deps": [], "size": 4 * 1024 * 1024},
        "ui_login": {"type": "ui", "priority": 50, "deps": ["core"], "size": 50 * 1024},
        "scene_lobby": {"type": "scene", "priority": 90, "deps": ["core"], "size": 8 * 1024 * 1024},
        "video_intro": {"type": "video", "priority": 60, "deps": ["core"], "size": 30 * 1024 * 1024},
    }


WEAK_NET = {"weak": True, "bandwidth_kbps": 150, "window": 8, "aging": 50}


# ------------------------------------------------------------ 1. 分片级校验

def test_chunk_hash_binds_index_and_is_stable():
    h0a = mod.chunk_hash(b"payload", 0)
    h0b = mod.chunk_hash(b"payload", 0)
    h1 = mod.chunk_hash(b"payload", 1)

    assert h0a == h0b                  # 同分片同内容，哈希稳定
    assert h0a != h1                   # 序号参与哈希，分片互换不能通过
    assert len(h0a) == 64              # sha256 十六进制摘要长度


def test_validation_fails_single_chunk_without_full_redownload():
    ok = mod.validation("abc", "abc", chunk_index=2)
    bad = mod.validation("abc", "abd", chunk_index=2)

    assert ok["ok"] is True
    assert bad["ok"] is False
    assert bad["reason"] == "hash_mismatch"
    assert bad["chunk"] == 2           # 调用方只需重下第 2 片，而非整包

    assert mod.validation(None, "x")["ok"] is False
    assert mod.validation("x", None)["ok"] is False  # 无清单期望值时拒绝盲信


def test_metrics_measure_bytes_saved_by_chunk_redownload():
    stats = mod.metrics([
        {"type": "bundle_total", "bytes": 10_000_000},
        {"type": "chunk_failed", "bytes": 1_000_000},
        {"type": "chunk_failed", "bytes": 1_000_000},
    ])
    assert stats["chunk_failures"] == 2
    assert stats["chunk_redownload_bytes"] == 2_000_000
    assert stats["saved_bytes"] == 8_000_000


# ------------------------------------------------------------ 2. 依赖图优先级与队列

def test_dep_priority_propagates_blocking_weight_and_ui_bonus(graph):
    core = mod.dep_priority(graph, "core")
    ui = mod.dep_priority(graph, "ui_login")

    assert core["blocking"] == 3       # 被三个下游传递依赖
    assert core["effective_priority"] == 100 + 3 * 10 + 500
    assert ui["type_bonus"] == 1000
    assert ui["effective_priority"] > mod.dep_priority(graph, "scene_lobby")["effective_priority"]


def test_prefetch_queue_respects_deps_and_prioritizes_ui(graph):
    queue = mod.prefetch_queue(graph)

    assert queue[0] == "core"          # 所有节点的依赖先行
    assert queue.index("ui_login") < queue.index("scene_lobby")
    assert queue.index("ui_login") < queue.index("video_intro")

    # 拓扑合法性：每个节点都排在其全部依赖之后
    emitted = set()
    for node in queue:
        assert all(dep in emitted for dep in graph[node]["deps"])
        emitted.add(node)
    assert set(queue) == set(graph)


def test_prefetch_queue_reaches_only_start_subtree(graph):
    queue = mod.prefetch_queue(graph, start="ui_login")
    assert set(queue) == {"core", "ui_login"}


def test_prefetch_queue_rejects_cycle():
    cyclic = {
        "a": {"deps": ["b"]},
        "b": {"deps": ["a"]},
    }
    with pytest.raises(ValueError):
        mod.prefetch_queue(cyclic)


def test_determinism_stable_across_runs(graph):
    result = mod.determinism(graph, runs=5)
    assert result["stable"] is True
    assert all(q == result["queues"][0] for q in result["queues"])

    assert mod.determinism([[1, 2], [1, 2]])["stable"] is True
    assert mod.determinism([[1, 2], [2, 1]])["stable"] is False


# ------------------------------------------------------------ 3. 弱网队列重排

def test_weaknet_reorder_moves_small_ui_ahead_without_breaking_deps(graph):
    # 模拟一个被大包占住前部的错误队列
    bad_queue = ["core", "video_intro", "scene_lobby", "ui_login"]
    reordered = mod.weaknet_reorder(bad_queue, graph, WEAK_NET)

    assert reordered[0] == "core"      # 依赖未就绪时再高优先也不能前插
    assert reordered.index("ui_login") < reordered.index("video_intro")
    assert reordered.index("ui_login") < reordered.index("scene_lobby")

    emitted = set()
    for node in reordered:
        assert all(dep in emitted for dep in graph[node]["deps"])
        emitted.add(node)
    assert set(reordered) == set(bad_queue)   # 重排不丢包、不重复


def test_weaknet_reorder_noop_when_network_healthy(graph):
    queue = ["core", "video_intro", "ui_login"]
    assert mod.weaknet_reorder(queue, graph, {"weak": False}) == queue


def test_weaknet_reorder_is_deterministic_and_never_starves(graph):
    queue = mod.prefetch_queue(graph)
    runs = [mod.weaknet_reorder(queue, graph, WEAK_NET) for _ in range(3)]
    assert all(r == runs[0] for r in runs)
    assert sorted(runs[0]) == sorted(queue)


# ------------------------------------------------------------ 4. 解压状态机与卸载引用计数

def test_decompress_holds_ref_and_blocks_unload_until_ready():
    state = {}

    assert mod.decompress_state(state, "lvl1", "downloading")["ok"]
    assert mod.decompress_state(state, "lvl1", "verifying")["ok"]

    result = mod.decompress_state(state, "lvl1", "decompressing")
    assert result["ok"]
    assert result["refs"] == 1        # 解压流程自动持有引用

    guard = mod.unload_guard(state, "lvl1")
    assert guard["unloadable"] is False
    assert guard["reason"] == "decompressing"

    ready = mod.decompress_state(state, "lvl1", "ready")
    assert ready["refs"] == 0         # 解压结束自动释放
    assert mod.unload_guard(state, "lvl1")["unloadable"] is True


def test_business_refs_block_unload_even_when_ready():
    state = {"lvl2": {"state": "ready", "refs": 0}}

    assert mod.ref_count(state, "lvl2", +1) == 1
    guard = mod.unload_guard(state, "lvl2")
    assert guard["unloadable"] is False
    assert guard["reason"] == "in_use"

    assert mod.ref_count(state, "lvl2", -1) == 1 - 1
    assert mod.unload_guard(state, "lvl2")["unloadable"] is True


def test_ref_count_floored_at_zero():
    state = {"x": {"state": "queued", "refs": 0}}
    assert mod.ref_count(state, "x", -1) == 0


def test_illegal_decompress_transition_rejected_and_state_unchanged():
    state = {"lvl3": {"state": "queued", "refs": 0}}
    result = mod.decompress_state(state, "lvl3", "ready")  # 跳过下载/解压
    assert result["ok"] is False
    assert result["reason"].startswith("illegal_transition")
    assert state["lvl3"]["state"] == "queued"


# ------------------------------------------------------------ 5. 清单版本与缓存失效

def test_manifest_version_classifies_upgrade_rollback_divergence():
    local = {"sequence": 10, "hash": "h10"}

    upgrade = mod.manifest_version(local, {"sequence": 11, "hash": "h11"})
    assert upgrade["status"] == "upgrade"
    assert upgrade["action"] == "download"

    rollback = mod.manifest_version(local, {"sequence": 9, "hash": "h9"})
    assert rollback["status"] == "rollback"
    assert rollback["action"] == "block"               # 旧清单不得静默命中

    diverged = mod.manifest_version(local, {"sequence": 10, "hash": "h10x"})
    assert diverged["status"] == "diverged"
    assert diverged["action"] == "refetch"

    same = mod.manifest_version(local, {"sequence": 10, "hash": "h10"})
    assert same["status"] == "up_to_date"


def test_cache_invalidate_on_rollback_and_manifest_change():
    cache = {
        "a": {"hash": "ha", "sequence": 10},
        "b": {"hash": "hb", "sequence": 10},
        "c": {"hash": "hc", "sequence": 9},
        "old": {"hash": "ho", "sequence": 10},
    }
    new_manifest = {
        "sequence": 11,
        "hashes": {"a": "ha", "b": "hb-new", "c": "hc", "d": "hd"},
    }

    result = mod.cache_invalidate(cache, new_manifest)
    reasons = {item["bundle"]: item["reason"] for item in result["invalidated"]}
    assert reasons["a"] == "version_mismatch"
    assert reasons["b"] == "hash_mismatch"
    assert reasons["c"] == "version_mismatch"
    assert reasons["old"] == "removed_from_manifest"
    assert result["kept"] == []

    rollback_manifest = {"sequence": 9, "_rollback": True, "hashes": {"a": "ha"}}
    rolled = mod.cache_invalidate(cache, rollback_manifest)
    assert {item["reason"] for item in rolled["invalidated"]} == {"rollback"}
    assert rolled["cache"] == {}

    # 显式允许回滚时不再以 rollback 为由失效（但其他原因仍生效）
    allowed = mod.cache_invalidate(cache, rollback_manifest, allow_rollback=True)
    assert all(item["reason"] != "rollback" for item in allowed["invalidated"])


def test_audit_reports_corrupt_stale_orphan_missing():
    cache = {
        "a": {"hash": "ha", "sequence": 10},
        "b": {"hash": "hb-old", "sequence": 10},
        "c": {"hash": "hc", "sequence": 10},
        "orphan": {"hash": "hz", "sequence": 10},
    }
    manifest = {
        "sequence": 11,
        "hashes": {"a": "ha", "b": "hb", "c": "hc", "d": "hd"},
    }
    report = mod.audit(cache, manifest)

    assert report["corrupt"] == ["b"]
    assert report["orphan"] == ["orphan"]
    assert report["missing"] == ["d"]
    assert sorted(report["stale"]) == ["a", "c"]


def test_fallback_orders_safe_degradation_modes():
    unverified = {"a": {"verified": False}}
    bundled = {"hashes": {"builtin": "x"}}

    assert mod.fallback("timeout", unverified, bundled)["mode"] == "bundled"

    verified = {"a": {"verified": True}}
    cached = mod.fallback("timeout", verified, bundled)
    assert cached["mode"] == "cached"
    assert cached["bundles"] == ["a"]

    assert mod.fallback("timeout", {}, None)["mode"] == "block"


# ------------------------------------------------------------ 6. 断点续传偏移校验

def test_resume_offset_accepts_only_verified_aligned_boundaries():
    # 400 字节、100 字节/片，仅 0、1 两片校验通过，安全前缀 = 200
    ok = mod.resume_offset(100, file_size=400, chunk_size=100, verified_chunks={0, 1})
    assert ok["safe"] is True and ok["offset"] == 100 and ok["adjusted"] is False

    # 安全前缀边界本身可续传；越过它（第 2 片未校验）才是空洞
    boundary = mod.resume_offset(200, 400, 100, {0, 1})
    assert boundary["safe"] is True and boundary["offset"] == 200

    hole = mod.resume_offset(300, 400, 100, {0, 1})
    assert hole["reason"] == "unverified_hole"
    assert hole["offset"] == 200 and hole["adjusted"] is True

    misaligned = mod.resume_offset(150, 400, 100, {0, 1})
    assert misaligned["reason"] == "misaligned"
    assert misaligned["offset"] == 200

    out_of_range = mod.resume_offset(500, 400, 100, {0, 1})
    assert out_of_range["reason"] == "out_of_range"
    assert out_of_range["offset"] == 200

    negative = mod.resume_offset(-10, 400, 100, {0, 1})
    assert negative["safe"] is False and negative["offset"] == 200


def test_resume_offset_hole_in_middle_falls_back_to_prefix():
    result = mod.resume_offset(300, 400, 100, {0, 2, 3})  # 第 1 片是空洞
    assert result["offset"] == 100
    assert result["reason"] == "unverified_hole"


def test_resume_offset_requires_positive_chunk_size():
    result = mod.resume_offset(50, file_size=400, chunk_size=0)
    assert result["offset"] == 0
    assert result["adjusted"] is True


def test_resume_offset_end_of_fully_verified_file():
    result = mod.resume_offset(400, 400, 100, {0, 1, 2, 3})
    assert result["safe"] is True and result["offset"] == 400
