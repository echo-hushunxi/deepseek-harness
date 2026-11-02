---
description: "Records a persistence type transition and its compatibility acknowledgement."
kind: persistence-change
---

# 2026-11-03-credential-cooldown

English | [中文](2026-11-03-credential-cooldown.zh.md)

## Summary

Adds credential-cooldown attribution to a persisted continuation user message.

## Table of Contents

- [Declaration](#declaration)
- [Compatibility](#compatibility)
- [Verification](#verification)
- [Dev Note](#dev-note)

<a id="declaration"></a>
## Declaration

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
## Compatibility

Existing logs remain valid with no writer-version change. New readers preserve existing message sources unchanged. Older readers retain unknown source kinds as attribution while preserving the ordinary user-message content and never interpreting this kind as an executable recovery instruction; Host-owned pending metadata, not this source, drives recovery. This alternative is annotated persistenceAttribution and introduces no replay authority.

<a id="verification"></a>
## Verification

The Loader/HTTP/AgentLoop pool tests and lifecycle tests verify sticky references, quota deadlines, cancellation, durable recovery, and restart restoration. The keyless credential-pool CLI snapshot passed.

<a id="dev-note"></a>
## Dev Note

None.
