"""边下边玩（play-while-download）资源调度核心模块。

修复六个已知问题：

1. 分片级校验 —— chunk_hash / validation / metrics
   只校验并重下失败分片，不再因单片损坏重下整包。
2. 依赖图优先级 —— dep_priority / prefetch_queue / determinism
   沿依赖边传播阻塞权重，UI 等关键资源不会被场景包挡在后面，排序确定可复现。
3. 解压与卸载引用计数 —— ref_count / decompress_state / unload_guard
   解压流程持有引用，引用计数归零且状态为 ready 的包才允许卸载。
4. 弱网队列重排 —— weaknet_reorder
   在保证依赖拓扑合法的前提下，按下载代价与时效优先级重排，并防止饥饿。
5. 清单版本与缓存失效 —— manifest_version / cache_invalidate / audit / fallback
   识别升级/回滚/分叉，版本回滚后旧清单缓存一律失效，远端失败时安全降级。
6. 断点续传偏移校验 —— resume_offset
   续传偏移必须落在已校验分片边界上，中间态偏移回退到安全前缀。

所有入口均可无参或以单个整数调用（契约测试用），此时返回该入口的空态默认值，
不抛出异常。
"""

from __future__ import annotations

import hashlib
import heapq
from typing import Any

# ---------------------------------------------------------------- 常量

DEFAULT_ALGO = "sha256"

# 解压状态机：合法的有向状态边
_DECOMPRESS_EDGES = {
    "queued": {"downloading"},
    "downloading": {"verifying", "failed"},
    "verifying": {"decompressing", "failed"},
    "decompressing": {"ready", "failed"},
    "ready": {"unloading"},
    "failed": {"queued"},  # 失败后允许重新入队重试
    "unloading": set(),
}

# 资源类型基础权重：UI 资源体积小且直接阻塞交互，弱网下优先调度
_TYPE_BONUS = {
    "ui": 1000,
    "core": 500,
    "scene": 100,
    "audio": 50,
    "video": 20,
}

# 每个传递依赖方带来的阻塞权重：被越多节点依赖，越靠近关键路径
_BLOCK_WEIGHT = 10


# ---------------------------------------------------------------- 1. 分片级校验

def chunk_hash(data: Any = b"", index: int = 0, algo: str = DEFAULT_ALGO) -> str:
    """计算单个分片的哈希值。

    哈希输入绑定分片序号，防止两个内容相同的分片被互换位置后仍通过校验。

    参数:
        data: 分片字节（或可编码文本）。
        index: 分片在整包中的序号，参与哈希计算。
        algo: hashlib 支持的算法名。
    """
    # 兼容 chunk_hash(1) 形式的契约调用
    if isinstance(data, int) and not isinstance(data, bool):
        index, data = data, b""
    if data is None:
        data = b""
    if isinstance(data, str):
        data = data.encode("utf-8")
    h = hashlib.new(algo)
    h.update(int(index).to_bytes(8, "little", signed=False))
    h.update(bytes(data))
    return h.hexdigest()


def validation(actual: Any = None, expected: Any = None,
               chunk_index: Any = None) -> dict:
    """分片级校验：只比对单个分片的哈希。

    返回校验结果字典；失败时调用方只需重下 ``chunk_index`` 这一个分片，
    而不是重下整个资源包。缺少清单期望值时拒绝盲信本地数据。
    """
    base = {"chunk": chunk_index, "actual": actual, "expected": expected}
    if actual is None:
        return {**base, "ok": False, "reason": "missing_actual_hash"}
    if expected is None:
        return {**base, "ok": False, "reason": "missing_expected_hash"}
    ok = str(actual).lower() == str(expected).lower()
    return {**base, "ok": ok, "reason": "match" if ok else "hash_mismatch"}


def metrics(events: Any = None) -> dict:
    """汇总传输事件，统计分片级校验相对整包重下的流量节省。

    事件格式：``{"type": "chunk_failed"|"bundle_total", "bytes": int}``。
    """
    result = {
        "events": 0,
        "transfers": 0,
        "chunk_failures": 0,
        "chunk_redownload_bytes": 0,
        "whole_bundle_bytes": 0,
        "saved_bytes": 0,
    }
    if not isinstance(events, (list, tuple)):
        return result
    for event in events:
        if not isinstance(event, dict):
            continue
        result["events"] += 1
        kind = event.get("type", "")
        size = int(event.get("bytes", 0) or 0)
        if kind == "chunk_failed":
            result["chunk_failures"] += 1
            result["chunk_redownload_bytes"] += size
        elif kind == "bundle_total":
            result["transfers"] += 1
            result["whole_bundle_bytes"] += size
    result["saved_bytes"] = max(
        0,
        result["whole_bundle_bytes"] - result["chunk_redownload_bytes"],
    )
    return result


# ---------------------------------------------------------------- 2. 依赖图优先级

def _node_info(graph: Any, node: Any) -> dict:
    if isinstance(graph, dict):
        info = graph.get(node)
        if isinstance(info, dict):
            return info
    return {}


def _deps(graph: Any, node: Any) -> list:
    return list(_node_info(graph, node).get("deps", ()) or ())


def _reverse_graph(graph: dict) -> dict:
    """构建 被依赖方 -> 依赖方集合 的反向图。"""
    rev = {node: set() for node in graph}
    for node, info in graph.items():
        if not isinstance(info, dict):
            continue
        for dep in info.get("deps", ()) or ():
            rev.setdefault(dep, set()).add(node)
    return rev


def _transitive_dependents(graph: dict) -> dict:
    """每个节点的全部传递依赖方（即该节点阻塞了哪些下游节点）。"""
    rev = _reverse_graph(graph)
    result: dict = {}

    def walk(node: Any) -> set:
        if node in result:
            return result[node]
        seen: set = set()
        stack = list(rev.get(node, ()))
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            stack.extend(rev.get(cur, ()))
        result[node] = seen
        return seen

    for node in graph:
        walk(node)
    return result


def dep_priority(graph: Any = None, node: Any = None,
                 priority: Any = None) -> dict:
    """计算节点在依赖图中的有效调度优先级（数值越大越优先）。

    有效优先级 = 业务基础优先级 + 传递阻塞权重 + 资源类型权重。
    被大量下游依赖的节点（关键路径）与 UI 资源会被显著提前。
    """
    if not isinstance(graph, dict):
        return {
            "node": node,
            "base_priority": 0,
            "blocking": 0,
            "type_bonus": 0,
            "effective_priority": 0,
        }
    info = _node_info(graph, node)
    base = int(priority) if priority is not None else int(info.get("priority", 0))
    blocking = len(_transitive_dependents(graph).get(node, set()))
    type_bonus = _TYPE_BONUS.get(info.get("type", ""), 0)
    effective = base + blocking * _BLOCK_WEIGHT + type_bonus
    return {
        "node": node,
        "base_priority": base,
        "blocking": blocking,
        "type_bonus": type_bonus,
        "effective_priority": effective,
    }


def prefetch_queue(graph: Any = None, start: Any = None) -> list:
    """按依赖图生成预取队列。

    采用 Kahn 拓扑排序，就绪节点之间按有效优先级、再按节点名做确定性裁决，
    保证：依赖方永远不会排在被依赖方之前；同图同入参输出恒定。
    图中存在环时抛出 ValueError。
    """
    if not isinstance(graph, dict):
        return []

    rev = _reverse_graph(graph)
    indegree = {}
    for node, info in graph.items():
        deps = [d for d in (info.get("deps", ()) or ()) if d in graph] if isinstance(info, dict) else []
        indegree[node] = len(deps)

    def push(heap: list, node: Any) -> None:
        effective = dep_priority(graph, node)["effective_priority"]
        heapq.heappush(heap, (-effective, str(node), node))

    ready: list = []
    for node, deg in indegree.items():
        if deg == 0:
            push(ready, node)

    ordered: list = []
    while ready:
        _, _, node = heapq.heappop(ready)
        ordered.append(node)
        for downstream in rev.get(node, ()):
            indegree[downstream] -= 1
            if indegree[downstream] == 0:
                push(ready, downstream)

    if len(ordered) != len(graph):
        stuck = sorted(str(n) for n in set(graph) - set(ordered))
        raise ValueError(f"dependency cycle detected among: {stuck}")

    if start is not None:
        reachable: set = set()
        stack = [start]
        while stack:
            cur = stack.pop()
            if cur in reachable or cur not in graph:
                continue
            reachable.add(cur)
            stack.extend(_deps(graph, cur))
        ordered = [node for node in ordered if node in reachable]

    return ordered


def determinism(graph: Any = None, runs: int = 3) -> dict:
    """验证调度顺序的确定性。

    传入依赖图时重复构建 ``runs`` 次队列并比较；传入队列列表时逐一两两比较。
    """
    if isinstance(graph, list):
        queues = graph
        stable = all(q == queues[0] for q in queues)
        return {"stable": stable, "queues": queues}
    if not isinstance(graph, dict):
        return {"stable": True, "queues": []}
    queues = [prefetch_queue(graph) for _ in range(max(1, int(runs)))]
    return {
        "stable": all(q == queues[0] for q in queues),
        "queues": queues,
    }


# ---------------------------------------------------------------- 3. 弱网队列重排

def weaknet_reorder(queue: Any = None, graph: Any = None,
                    network: Any = None) -> list:
    """弱网环境下对预取队列做有界重排。

    - ``network["weak"]`` 为 False 时保持原序；
    - 只在原队列前部的有限窗口内挑选，避免单个大包长期占用、小包饥饿；
    - 评分 = 有效优先级 - 预计下载秒数 + 等待老化补偿；
    - 硬约束不变：节点的所有依赖必须先于该节点发出。
    """
    if not isinstance(queue, (list, tuple)):
        return []
    remaining = list(queue)
    if not isinstance(network, dict) or network.get("weak", True) is False:
        return list(remaining)

    bandwidth_kbps = max(1, int(network.get("bandwidth_kbps", 150)))
    window = max(1, int(network.get("window", 8)))
    aging = int(network.get("aging", 50))

    emitted: set = set()
    waits = {node: 0 for node in remaining}
    result: list = []

    def score(node: Any) -> float:
        info = _node_info(graph, node)
        effective = (
            dep_priority(graph, node)["effective_priority"]
            if isinstance(graph, dict)
            else 0
        )
        size_bytes = max(1, int(info.get("size", 1)))
        download_seconds = size_bytes * 8 / (bandwidth_kbps * 1024)
        return effective - download_seconds + waits.get(node, 0) * aging

    while remaining:
        eligible = [
            node for node in remaining
            if all(dep in emitted for dep in _deps(graph, node))
        ]
        # 拓扑合法队列中至少队首就绪；窗口有界，防止无限前插造成饥饿
        candidates = eligible[:window] or eligible[:1]
        best = sorted(candidates, key=lambda n: (-score(n), str(n)))[0]
        result.append(best)
        emitted.add(best)
        remaining.remove(best)
        for node in remaining:
            waits[node] += 1

    return result


# ---------------------------------------------------------------- 4. 解压与卸载引用计数

def ref_count(state: Any = None, bundle_id: Any = None,
              delta: int = 0) -> int:
    """调整资源包引用计数。

    delta=+1 表示业务/流程持有，-1 表示释放；计数下限钳为 0，不允许悬空。
    返回调整后的引用计数。
    """
    if not isinstance(state, dict) or bundle_id is None:
        return 0
    entry = state.setdefault(bundle_id, {"refs": 0, "state": "queued"})
    entry["refs"] = max(0, int(entry.get("refs", 0)) + int(delta))
    return entry["refs"]


def decompress_state(state: Any = None, bundle_id: Any = None,
                     target: Any = None) -> dict:
    """驱动资源包解压状态机，并自动管理解压流程的引用计数。

    进入 ``decompressing`` 时自动 +1（解压持有），离开该状态时自动 -1。
    非法状态跳转被拒绝且原状态保持不变。
    """
    if not isinstance(state, dict) or bundle_id is None:
        return {
            "bundle": bundle_id,
            "state": target,
            "refs": 0,
            "ok": False,
            "reason": "no_state",
        }

    entry = state.setdefault(bundle_id, {"refs": 0, "state": "queued"})
    current = entry.get("state", "queued")

    if target is None or target == current:
        return {
            "bundle": bundle_id,
            "state": current,
            "refs": entry.get("refs", 0),
            "ok": True,
            "reason": "noop",
        }

    if target not in _DECOMPRESS_EDGES.get(current, set()):
        return {
            "bundle": bundle_id,
            "state": current,
            "refs": entry.get("refs", 0),
            "ok": False,
            "reason": f"illegal_transition:{current}->{target}",
        }

    if target == "decompressing":
        entry["refs"] = int(entry.get("refs", 0)) + 1
    if current == "decompressing":
        entry["refs"] = max(0, int(entry.get("refs", 0)) - 1)

    entry["state"] = target
    return {
        "bundle": bundle_id,
        "state": target,
        "refs": entry.get("refs", 0),
        "ok": True,
        "reason": "transitioned",
    }


def unload_guard(state: Any = None, bundle_id: Any = None) -> dict:
    """卸载守卫：判断资源包当前是否可以被卸载。

    可卸载的充要条件：包已加载、状态为 ready、引用计数为 0。
    解压中（持有解压引用）或仍被业务持有的包一律拒绝。
    本函数只做判定，不修改状态；调用方仅在 unloadable 为 True 时执行删除。
    """
    if not isinstance(state, dict):
        return {
            "bundle": bundle_id,
            "state": None,
            "refs": 0,
            "unloadable": False,
            "reason": "no_state",
        }

    entry = state.get(bundle_id)
    if not isinstance(entry, dict):
        return {
            "bundle": bundle_id,
            "state": None,
            "refs": 0,
            "unloadable": False,
            "reason": "not_loaded",
        }

    refs = int(entry.get("refs", 0))
    current = entry.get("state")
    if current == "decompressing":
        reason = "decompressing"
    elif refs > 0:
        reason = "in_use"
    elif current != "ready":
        reason = "not_ready"
    else:
        reason = "ok"
    return {
        "bundle": bundle_id,
        "state": current,
        "refs": refs,
        "unloadable": reason == "ok",
        "reason": reason,
    }


# ---------------------------------------------------------------- 5. 清单版本与缓存失效

def _seq(manifest: Any) -> Any:
    return manifest.get("sequence") if isinstance(manifest, dict) else None


def manifest_version(local: Any = None, remote: Any = None) -> dict:
    """比较本地与远端清单，区分升级、回滚、同版本分叉与一致。

    回滚（远端序列号更小）默认动作是 block，绝不静默接受旧清单；
    同序列号但内容哈希不同视为分叉，需要重新拉取清单。
    """
    if not isinstance(local, dict) or not isinstance(remote, dict):
        return {
            "status": "unknown",
            "action": "block",
            "local_sequence": _seq(local),
            "remote_sequence": _seq(remote),
        }

    local_seq = int(local.get("sequence", 0))
    remote_seq = int(remote.get("sequence", 0))

    if remote_seq > local_seq:
        status, action = "upgrade", "download"
    elif remote_seq < local_seq:
        status, action = "rollback", "block"
    elif local.get("hash") != remote.get("hash"):
        status, action = "diverged", "refetch"
    else:
        status, action = "up_to_date", "keep"

    return {
        "status": status,
        "action": action,
        "local_sequence": local_seq,
        "remote_sequence": remote_seq,
    }


def cache_invalidate(cache: Any = None, manifest: Any = None,
                     allow_rollback: bool = False) -> dict:
    """依据清单使本地缓存失效。

    命中失效的情形：清单回滚（默认全部失效）、包已从清单移除、
    分片哈希不匹配、缓存记录的清单序列号与当前清单不一致。
    返回保留下来的缓存与带原因的失效清单。
    """
    if not isinstance(cache, dict):
        return {"invalidated": [], "kept": [], "cache": {}}

    manifest = manifest if isinstance(manifest, dict) else {}
    hashes = manifest.get("hashes", {})
    sequence = manifest.get("sequence")
    rollback = bool(manifest.get("_rollback", False)) and not allow_rollback

    kept: dict = {}
    invalidated: list = []
    for bundle_id, entry in cache.items():
        if rollback:
            reason = "rollback"
        elif bundle_id not in hashes:
            reason = "removed_from_manifest"
        elif entry.get("hash") != hashes[bundle_id]:
            reason = "hash_mismatch"
        elif sequence is not None and entry.get("sequence") != sequence:
            reason = "version_mismatch"
        else:
            kept[bundle_id] = entry
            continue
        invalidated.append({"bundle": bundle_id, "reason": reason})

    invalidated.sort(key=lambda item: str(item["bundle"]))
    return {
        "invalidated": invalidated,
        "kept": sorted(kept, key=str),
        "cache": kept,
    }


def audit(cache: Any = None, manifest: Any = None) -> dict:
    """按清单审计缓存：ok / stale（版本旧）/ corrupt（哈希坏）/
    orphan（清单已移除）/ missing（清单有、缓存无）。"""
    report = {"ok": [], "stale": [], "corrupt": [], "orphan": [], "missing": []}
    if not isinstance(cache, dict) or not isinstance(manifest, dict):
        return report

    hashes = manifest.get("hashes", {})
    sequence = manifest.get("sequence")
    for bundle_id, entry in cache.items():
        if bundle_id not in hashes:
            report["orphan"].append(bundle_id)
        elif entry.get("hash") != hashes[bundle_id]:
            report["corrupt"].append(bundle_id)
        elif sequence is not None and entry.get("sequence") != sequence:
            report["stale"].append(bundle_id)
        else:
            report["ok"].append(bundle_id)
    for bundle_id in hashes:
        if bundle_id not in cache:
            report["missing"].append(bundle_id)

    for key in report:
        report[key].sort(key=str)
    return report


def fallback(error: Any = None, cache: Any = None,
             bundled_manifest: Any = None) -> dict:
    """远端清单拉取失败时的安全降级策略。

    仅当缓存中的每个包都标记为已完整校验时才允许 cached 模式；
    否则退回包体内置清单（bundled）；两者都不可用则 block，禁止带伤启动。
    """
    if isinstance(cache, dict) and cache:
        entries = [e for e in cache.values() if isinstance(e, dict)]
        if entries and all(e.get("verified") for e in entries):
            return {
                "mode": "cached",
                "reason": "fully_verified_cache",
                "bundles": sorted(cache, key=str),
            }
    if isinstance(bundled_manifest, dict):
        return {
            "mode": "bundled",
            "reason": "use_builtin_assets",
            "bundles": sorted(bundled_manifest.get("hashes", {}), key=str),
        }
    return {"mode": "block", "reason": "no_usable_manifest", "bundles": []}


# ---------------------------------------------------------------- 6. 断点续传偏移校验

def resume_offset(requested: Any = None, file_size: int = 0,
                  chunk_size: int = 0, verified_chunks: Any = ()) -> dict:
    """校验断点续传偏移，返回唯一可信的续传起点。

    合法偏移必须同时满足：
      1. 0 <= offset <= file_size；
      2. 落在分片边界（offset % chunk_size == 0）；
      3. 该偏移之前的每个分片都已校验通过（不存在未校验空洞）。
    任一条件不满足时，回退到“已校验连续前缀”的末尾，并给出调整原因。
    """
    if not isinstance(chunk_size, int) or chunk_size <= 0:
        adjusted = requested not in (0, None)
        return {
            "requested": requested,
            "offset": 0,
            "safe": not adjusted,
            "adjusted": adjusted,
            "reason": "chunk_size_required" if adjusted else "empty",
        }

    verified = {int(i) for i in (verified_chunks or ())}
    prefix = 0
    while prefix in verified:
        prefix += 1
    safe = min(prefix * chunk_size, max(0, int(file_size)))

    if requested is None:
        return {
            "requested": None,
            "offset": safe,
            "safe": True,
            "adjusted": False,
            "reason": "default_to_safe_prefix",
        }

    requested = int(requested)
    if requested < 0 or requested > int(file_size):
        reason = "out_of_range"
    elif requested % chunk_size != 0:
        reason = "misaligned"
    elif requested > safe:
        reason = "unverified_hole"
    else:
        return {
            "requested": requested,
            "offset": requested,
            "safe": True,
            "adjusted": False,
            "reason": "ok",
        }

    return {
        "requested": requested,
        "offset": safe,
        "safe": False,
        "adjusted": True,
        "reason": reason,
    }
