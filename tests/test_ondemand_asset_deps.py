import pytest
import gameplay.ondemand_asset_deps as mod

# 注意：单元素元组必须带逗号，(1) 在 Python 中只是整数 1，
# fn(*1) 会在调用点抛 TypeError，任何实现都无法满足契约。
CASES = [
    ("chunk_hash", (1,)),
    ("dep_priority", (1,)),
    ("ref_count", (1,)),
    ("unload_guard", (1,)),
    ("weaknet_reorder", (1,)),
    ("manifest_version", (1,)),
    ("resume_offset", (1,)),
    ("validation", (1,)),
    ("audit", (1,)),
    ("fallback", (1,)),
    ("cache_invalidate", (1,)),
    ("metrics", (1,)),
    ("prefetch_queue", (1,)),
    ("decompress_state", (1,)),
    ("determinism", (1,)),
]

@pytest.mark.parametrize("fn_name,args", CASES)
def test_contract(fn_name, args):
    fn = getattr(mod, fn_name)
    assert fn(*args) is not None
