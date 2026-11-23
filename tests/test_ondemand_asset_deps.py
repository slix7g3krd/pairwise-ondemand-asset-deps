import pytest
import gameplay.ondemand_asset_deps as mod

CASES = [
    ("chunk_hash", (1)),
    ("dep_priority", (1)),
    ("ref_count", (1)),
    ("unload_guard", (1)),
    ("weaknet_reorder", (1)),
    ("manifest_version", (1)),
    ("resume_offset", (1)),
    ("validation", (1)),
    ("audit", (1)),
    ("fallback", (1)),
    ("cache_invalidate", (1)),
    ("metrics", (1)),
    ("prefetch_queue", (1)),
    ("decompress_state", (1)),
    ("determinism", (1)),
]

@pytest.mark.parametrize("fn_name,args", CASES)
def test_contract(fn_name, args):
    fn = getattr(mod, fn_name)
    assert fn(*args) is not None
