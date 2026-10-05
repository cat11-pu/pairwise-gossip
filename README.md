# pairwise-gossip

只依赖 Python 标准库的内存 gossip 成员协议仿真：时钟、随机源与消息队列都可注入，
不涉及真实网络。

- `gossip/core.py` — 协议内核：成员表、版本向量、流言扩散、反熵交换、失败检测。
- `tests/test_core.py` — 验收用例。

## 运行测试

在项目根目录执行：

```
python3 -m unittest discover -s tests -v
```

Windows 上如果没有 `python3`，可用：

```
python -m unittest discover -s tests -v
```
