# pairwise-ondemand-asset-deps

## 领域

游戏、图形与多媒体

## 主题

边下边玩资源依赖

## 说明

分片校验、依赖优先级与引用计数。

初始状态下 `gameplay/ondemand_asset_deps.py` 中所有函数抛出 `NotImplementedError`，测试全部失败。
修复实现后运行：

```bash
python -m pytest -q
```

期望全部通过。
