---
description: "记录持久化类型更改及其兼容性确认。"
kind: persistence-change
---

# 2026-11-03-credential-cooldown

[English](2026-11-03-credential-cooldown.md) | 中文

## 概述

为持久化的继续执行用户消息增加 credential-cooldown 来源归属。

## 目录

- [声明](#declaration)
- [兼容性](#compatibility)
- [验证](#verification)
- [开发备注](#dev-note)

<a id="declaration"></a>
## 声明

```yaml persistence-change
schemaVersion: 1
id: 2026-11-03-credential-cooldown
baseline: false
changes:
  - root: "event:agent/inbox/spliced"
    previous: "2026-10-13-liveshare-share-viewer-principal"
    after: "a5bdb21e00032aecd2202ff1f04dc882760f1df08cc8baadd5e7300f488bf49b"
    decision: same-version
  - root: "event:developer/message"
    previous: "2026-10-13-liveshare-share-viewer-principal"
    after: "3bdf32ff2d37a8abfbb2c59d0c7c23a88cb218245bd268a46544cf2c8d3daee2"
    decision: same-version
  - root: "event:session/title-llm-request"
    previous: "2026-10-13-liveshare-share-viewer-principal"
    after: "16bc16e55e9579b56624b8967f8e010d1357fe99b46e6f1ad07647cc56366683"
    decision: same-version
  - root: "event:user/message"
    previous: "2026-10-13-liveshare-share-viewer-principal"
    after: "23395e5c644ab05650e61c9448c4dbca73dae7449e1b8f00017f49a5772caeef"
    decision: same-version
```

<a id="compatibility"></a>
## 兼容性

已有日志仍有效，写入版本不变。新读取方原样保留已有消息来源。旧读取方将未知来源 kind 作为归属信息保留，同时保留普通用户消息内容，不将该 kind 解释为可执行恢复指令；恢复由 Host 管理的待处理元数据驱动，而非此来源。该分支已标记 persistenceAttribution，不引入回放权限。

<a id="verification"></a>
## 验证

Loader/HTTP/AgentLoop 密钥池测试和生命周期测试验证固定引用、额度截止时间、取消、持久化恢复和重启恢复。credential-pool 无密钥 CLI 快照已通过。

<a id="dev-note"></a>
## 开发备注

无。
