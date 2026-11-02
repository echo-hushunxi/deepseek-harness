import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Context } from '@deepseek-ai/cordis'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { mountAgentLoopTestDependencies, mountAgentLoopTestHarness } from '@deepseek-ai/dsh-agent-loop-testkit'
import { createMessage, createUserMessage, LlmAdapter } from '@deepseek-ai/dsh-llm'
import type { GenerateOptions, LlmFailure, StreamChunk } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'
import * as Jsonl from '@deepseek-ai/dsh-session-persistence-jsonl'
import Storage, { storageBackendServiceKey } from '@deepseek-ai/dsh-storage'
import type { KvUnit } from '@deepseek-ai/dsh-storage'
import * as StorageDomain from '@deepseek-ai/dsh-storage-domain'
import { MemoryMediaPool, MemoryStorageBackend } from '../../../storage/storage-domain/tests/helpers/memory-backend.ts'
import { credentialPoolDomain } from '../src/credential-pool-storage.ts'
import { CredentialPools, type CredentialPoolConfig } from '../src/credential-pool.ts'

const contexts: Context[] = []
const roots: string[] = []
const config = { mock: { apiKeyEnvs: ['KEY_ONE', 'KEY_TWO'] } }

afterEach(async () => {
  await Promise.all(contexts.splice(0).map(ctx => ctx.fiber.dispose()))
  vi.useRealTimers()
  vi.restoreAllMocks()
  await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true })))
})

async function fixture(options: { media?: MemoryMediaPool; unit?: (unit: KvUnit) => void } = {}) {
  const ctx = new Context()
  contexts.push(ctx)
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(Storage)
  const media = options.media ?? new MemoryMediaPool()
  const backend = new MemoryStorageBackend(media)
  const open = backend.kv.open.bind(backend.kv)
  backend.kv.open = async (descriptor) => {
    const unit = await open(descriptor)
    options.unit?.(unit)
    return unit
  }
  ctx.effect(() => ctx.storage.backend.register('memory', backend))
  ctx.provide(storageBackendServiceKey('memory'), backend)
  await ctx.plugin(StorageDomain, { backend: 'memory' })
  const root = await mkdtemp(join(tmpdir(), 'dsh-pool-lifecycle-'))
  roots.push(root)
  await ctx.plugin(Jsonl, { root })
  await mountAgentLoopTestHarness(ctx)
  return { ctx, media }
}

async function install(ctx: Context, configs: Record<string, CredentialPoolConfig> = config) {
  let owner: CredentialPools | undefined
  const fiber = ctx.plugin({
    name: 'credential-pool-policy-test',
    inject: ['agents', 'sessions', 'sessionPersistence', 'storageDomain'],
    async apply(child: Context) { owner = await CredentialPools.create(child, configs) },
  })
  await fiber
  if (owner === undefined) throw new Error('pool fixture did not initialize')
  return { owner, fiber }
}

function rows(media: MemoryMediaPool, table: string): Map<string, unknown> {
  return media.media.get(credentialPoolDomain.name)?.tables.get(table) ?? new Map<string, unknown>()
}

function seeded(options: { pending?: boolean; cooldown?: boolean; provider?: string; retryAt?: number } = {}): MemoryMediaPool {
  const media = new MemoryMediaPool()
  const retryAt = options.retryAt ?? Date.now() + 1000
  const provider = options.provider ?? 'mock'
  media.versions.set(credentialPoolDomain.name, 1)
  media.media.set(credentialPoolDomain.name, {
    global: {},
    tables: new Map<string, Map<string, unknown>>([
      ['bindings', new Map<string, unknown>([['root', { [provider]: 'KEY_ONE' }]])],
      ['cooldowns', new Map<string, unknown>(options.cooldown === false ? [] : [['KEY_ONE', { limitedAt: 0, retryAt }]])],
      ['pending', new Map<string, unknown>(options.pending === false ? [] : [['root', {
        id: 'interrupted', sessionId: 'root', rootSessionId: 'root', provider, ref: 'KEY_ONE', limitedAt: 0, retryAt,
      }]])],
    ]),
  })
  return media
}

async function actor(ctx: Context, id = 'root'): Promise<Agent> {
  return ctx.agentLoop.create(SessionId(id), { provider: 'mock', model: 'mock' })
}

function recover(ctx: Context, agent: Agent, signal: AbortSignal, failure: LlmFailure = { code: 'QUOTA', message: 'quota' }) {
  agent.session.append('turn/start', { turn: 1 })
  agent.session.append('step/start', { turn: 1, step: 1 })
  return ctx.waterfall('agent/request-error', {
    agent, signal, provider: 'mock', turn: 1, step: 1, failure, retryPolicy: undefined,
  }, () => Promise.resolve(undefined))
}

async function armed(ctx: Context, media: MemoryMediaPool): Promise<void> {
  await vi.waitFor(() => { expect(rows(media, 'pending').size).toBeGreaterThan(0) })
  await ctx.sessionPersistence.flush()
  await vi.waitFor(() => { expect(vi.getTimerCount()).toBeGreaterThan(0) })
}

class ReplyAdapter extends LlmAdapter {
  override async * stream(_options: GenerateOptions): AsyncIterable<StreamChunk> {
    yield { type: 'block-start', index: 0, blockType: 'text' }
    yield { type: 'text-delta', index: 0, text: 'recovered' }
    yield { type: 'block-end', index: 0, block: { type: 'text', text: 'recovered' } }
    yield { type: 'finish', reason: { kind: 'stop' } }
  }
}

describe('credential-pool ownership, storage, and cancellation', () => {
  it('keeps the empty configuration independent of Host services and rejects missing facilities', async () => {
    const ctx = new Context()
    contexts.push(ctx)
    await expect(CredentialPools.create(ctx, {})).resolves.toBeUndefined()
    await expect(CredentialPools.create(ctx, config)).rejects.toThrow(/requires agents/)
  })

  it.each([
    { bad: { mock: { apiKeyEnvs: [] } }, message: /distinct credential references/ },
    { bad: { mock: { apiKeyEnvs: ['KEY_ONE', 'KEY_ONE'] } }, message: /distinct credential references/ },
    { bad: { '': { apiKeyEnvs: ['KEY_ONE'] } }, message: /non-empty providers/ },
    { bad: { mock: { apiKeyEnvs: ['KEY_ONE'], cooldownMs: 0 } }, message: /cooldownMs/ },
    { bad: { mock: { apiKeyEnvs: ['KEY_ONE'], cooldownMs: 1.5 } }, message: /cooldownMs/ },
    { bad: { mock: { apiKeyEnvs: ['KEY_ONE'], cooldownMs: 2 ** 31 } }, message: /cooldownMs/ },
  ])('rejects invalid deployment policy $bad', async ({ bad, message }) => {
    const { ctx } = await fixture()
    await expect(CredentialPools.create(ctx, bad)).rejects.toThrow(message)
  })

  it('does not allocate during discovery or for another provider', async () => {
    const { ctx, media } = await fixture()
    const { owner } = await install(ctx)
    await expect(owner.reference('other')).resolves.toBeUndefined()
    await expect(owner.reference('mock')).resolves.toBe('KEY_ONE')
    expect(rows(media, 'bindings').size).toBe(0)
    await ctx.agentLoop.create(SessionId('unselected'))
    expect(rows(media, 'bindings').size).toBe(0)
  })

  it('fails closed for missing Sessions, orphaned children, and cyclic ownership', async () => {
    const { ctx } = await fixture()
    const { owner } = await install(ctx)
    await expect(owner.reference('mock', SessionId('missing'))).rejects.toMatchObject({ code: 'MISSING_SESSION' })
    ctx.sessions.create(SessionId('orphan'), { meta: { origin: 'subagent', parentSession: SessionId('missing') } })
    await expect(owner.reference('mock', SessionId('orphan'))).rejects.toMatchObject({ code: 'MISSING_SESSION' })
    ctx.sessions.create(SessionId('parentless'), { meta: { origin: 'subagent' } })
    await expect(owner.reference('mock', SessionId('parentless'))).rejects.toMatchObject({ code: 'INVALID_SESSION' })
    ctx.sessions.create(SessionId('cycle-a'), { meta: { origin: 'subagent', parentSession: SessionId('cycle-b') } })
    ctx.sessions.create(SessionId('cycle-b'), { meta: { origin: 'subagent', parentSession: SessionId('cycle-a') } })
    await expect(owner.reference('mock', SessionId('cycle-a'))).rejects.toMatchObject({ code: 'INVALID_SESSION' })
  })

  it('refuses removed references instead of rebinding an existing conversation', async () => {
    const { ctx } = await fixture({ media: seeded({ pending: false, retryAt: 0 }) })
    ctx.sessions.create(SessionId('root'))
    const { owner } = await install(ctx, { mock: { apiKeyEnvs: ['KEY_TWO'] } })
    await expect(owner.reference('mock', SessionId('root'))).rejects.toMatchObject({ code: 'MISSING_CREDENTIAL' })
  })

  it.each(['KEY_ONE', 'KEY_TWO'])('pins new conversations to the earliest key %s when all keys are cooling', async (earliest) => {
    const media = seeded({ pending: false, retryAt: Date.now() + (earliest === 'KEY_ONE' ? 1000 : 2000) })
    const medium = media.media.get(credentialPoolDomain.name)
    medium?.tables.get('cooldowns')?.set('KEY_TWO', { limitedAt: 0, retryAt: Date.now() + (earliest === 'KEY_TWO' ? 1000 : 2000) })
    const { ctx } = await fixture({ media })
    ctx.sessions.create(SessionId('new'))
    const { owner } = await install(ctx)
    await expect(owner.reference('mock', SessionId('new'))).rejects.toMatchObject({ code: 'CREDENTIAL_COOLDOWN' })
    expect(rows(media, 'bindings').get('new')).toEqual({ mock: earliest })
  })

  it('honors cancellation before admission and after the cooldown timer is armed', async () => {
    const { ctx, media } = await fixture()
    const { owner } = await install(ctx)
    const agent = await actor(ctx)
    const early = new AbortController()
    early.abort(new Error('cancel before request'))
    await expect(owner.reference('mock', agent.id, early.signal)).rejects.toThrow('cancel before request')
    await expect(recover(ctx, agent, early.signal)).resolves.toBeUndefined()
    expect(rows(media, 'pending').size).toBe(0)
    const active = new AbortController()
    const running = await actor(ctx, 'active')
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const recovery = recover(ctx, running, active.signal)
    await armed(ctx, media)
    active.abort()
    await expect(recovery).resolves.toBeUndefined()
    expect(rows(media, 'pending').size).toBe(0)
    expect(rows(media, 'cooldowns').size).toBe(1)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('retains the wake obligation when shutdown overlaps a durable write', async () => {
    const entered = Promise.withResolvers<undefined>()
    const release = Promise.withResolvers<undefined>()
    const { ctx, media } = await fixture({ unit(unit) {
      const put = unit.putRecord.bind(unit)
      unit.putRecord = async (table, key, value) => {
        if (table === 'pending') { entered.resolve(undefined); await release.promise }
        await put(table, key, value)
      }
    } })
    const { owner, fiber } = await install(ctx)
    const agent = await actor(ctx)
    const recovery = recover(ctx, agent, new AbortController().signal)
    await entered.promise
    const disposed = fiber.dispose()
    release.resolve(undefined)
    await disposed
    await expect(recovery).resolves.toBeUndefined()
    expect(rows(media, 'pending').size).toBe(1)
    expect(rows(media, 'cooldowns').size).toBe(1)
    await expect(owner.reference('mock', agent.id)).rejects.toBeDefined()
  })

  it('reports a failed durable write without silently advancing or borrowing a key', async () => {
    const { ctx, media } = await fixture()
    const { owner } = await install(ctx)
    const agent = await actor(ctx)
    media.failNextWrites = 1
    await expect(recover(ctx, agent, new AbortController().signal)).rejects.toThrow('injected write failure')
    expect(rows(media, 'pending').size).toBe(0)
    await expect(owner.reference('mock', agent.id)).resolves.toBe('KEY_ONE')
  })

  it('retires a user cancellation that arrives during the pending write', async () => {
    const entered = Promise.withResolvers<undefined>()
    const release = Promise.withResolvers<undefined>()
    const { ctx, media } = await fixture({ unit(unit) {
      const put = unit.putRecord.bind(unit)
      unit.putRecord = async (table, key, value) => {
        if (table === 'pending') { entered.resolve(undefined); await release.promise }
        await put(table, key, value)
      }
    } })
    await install(ctx)
    const agent = await actor(ctx)
    const cancel = new AbortController()
    const recovery = recover(ctx, agent, cancel.signal)
    await entered.promise
    cancel.abort()
    release.resolve(undefined)
    await expect(recovery).resolves.toBeUndefined()
    expect(rows(media, 'pending').size).toBe(0)
    expect(rows(media, 'cooldowns').size).toBe(1)
  })

  it('extends every live wait when the same credential receives a later provider deadline', async () => {
    const { ctx, media } = await fixture()
    await install(ctx, { mock: { apiKeyEnvs: ['KEY_ONE'] } })
    const first = await actor(ctx, 'first')
    const second = await actor(ctx, 'second')
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    let firstFinished = false
    const a = recover(ctx, first, new AbortController().signal, { code: 'QUOTA', message: 'quota', providerRetryAfterMs: 1000 })
      .then((result) => { firstFinished = true; return result })
    await armed(ctx, media)
    await vi.advanceTimersByTimeAsync(500)
    const b = recover(ctx, second, new AbortController().signal, { code: 'QUOTA', message: 'quota', providerRetryAfterMs: 2000 })
    await vi.waitFor(() => { expect(rows(media, 'pending').size).toBe(2) })
    await ctx.sessionPersistence.flush()
    await vi.advanceTimersByTimeAsync(500)
    expect(firstFinished).toBe(false)
    await vi.advanceTimersByTimeAsync(1500)
    await expect(a).resolves.toEqual({ kind: 'retry' })
    await expect(b).resolves.toEqual({ kind: 'retry' })
  })

  it('keeps recovery durable and reports retirement failure after a successful assistant reply', async () => {
    const { ctx, media } = await fixture({ media: seeded({ retryAt: 0 }) })
    ctx.llm.registerAdapter(['mock'], new ReplyAdapter())
    const agent = await actor(ctx)
    await ctx.sessions.flush(agent.session)
    ctx.on('session/event', (_session, event) => {
      if (event.type === 'assistant/message') media.failNextWrites = 1
    })
    const { fiber } = await install(ctx)
    await vi.waitFor(() => { expect(agent.session.deriveMessages().some(message => message.role === 'assistant')).toBe(true) })
    await agent.whenIdle()
    await fiber.dispose()
    // Successful user-visible output is not rolled back by metadata failure.
    expect(agent.session.deriveMessages().some(message => message.role === 'assistant')).toBe(true)
  })

  it('makes repeated completion notifications harmless after pending recovery is retired', async () => {
    const { ctx, media } = await fixture({ media: seeded() })
    const agent = await actor(ctx)
    const { fiber } = await install(ctx)
    agent.session.append('turn/start', { turn: 1 })
    agent.session.append('step/start', { turn: 1, step: 1 })
    for (const text of ['first result', 'second result']) {
      agent.session.append('assistant/message', { turn: 1, step: 1, stream: [], message: createMessage({
        role: 'assistant', source: { kind: 'model', provider: 'mock', model: 'mock' }, content: [{ type: 'text', text }],
      }) }, { surfaceOp: 'append' })
    }
    await fiber.dispose()
    expect(rows(media, 'pending').size).toBe(0)
  })

  it('does not write auxiliary failures that arrive after its owner is disposed', async () => {
    const { ctx, media } = await fixture()
    const { fiber } = await install(ctx)
    const agent = await actor(ctx)
    const entered = Promise.withResolvers<undefined>()
    const release = Promise.withResolvers<undefined>()
    class LateAdapter extends LlmAdapter {
      override async * stream(): AsyncIterable<StreamChunk> {
        entered.resolve(undefined)
        await release.promise
        yield { type: 'finish', reason: { kind: 'error', failure: { code: 'QUOTA', message: 'quota' } } }
      }
    }
    ctx.llm.registerAdapter(['mock'], new LateAdapter())
    const result = (async () => {
      const chunks = []
      for await (const chunk of ctx.llm.stream({ provider: 'mock', model: 'mock', sessionId: agent.id, messages: [] })) chunks.push(chunk)
      return chunks
    })()
    await entered.promise
    await fiber.dispose()
    release.resolve(undefined)
    expect(await result).toContainEqual(expect.objectContaining({ type: 'finish' }))
    expect(rows(media, 'cooldowns').size).toBe(0)
  })

  it('does not invent a binding for auxiliary failures and applies the configured default delay', async () => {
    const { ctx, media } = await fixture()
    await install(ctx)
    class QuotaAdapter extends LlmAdapter {
      override async * stream(): AsyncIterable<StreamChunk> {
        yield { type: 'finish', reason: { kind: 'error', failure: { code: 'QUOTA', message: 'quota' } } }
      }
    }
    ctx.llm.registerAdapter(['mock'], new QuotaAdapter())
    const unbound = await ctx.agentLoop.create(SessionId('unbound'), { provider: 'other', model: 'other' })
    for await (const _chunk of ctx.llm.stream({ provider: 'mock', model: 'mock', sessionId: unbound.id, messages: [] })) { /* consume */ }
    expect(rows(media, 'cooldowns').size).toBe(0)
    const bound = await actor(ctx)
    for await (const _chunk of ctx.llm.stream({ provider: 'mock', model: 'mock', sessionId: bound.id, messages: [] })) { /* consume */ }
    const row = credentialPoolDomain.tables.cooldowns.valueSchema.parse(rows(media, 'cooldowns').get('KEY_ONE'))
    expect(row.retryAt - row.limitedAt).toBe(5 * 60 * 60 * 1000)
  })

  it('drains auxiliary ownership resolution before closing its metadata domain', async () => {
    const { ctx, media } = await fixture()
    const { fiber } = await install(ctx)
    const handle = await ctx.agents.create({ sessionId: SessionId('cold'), agentOptions: { provider: 'mock', model: 'mock' } })
    await ctx.sessions.flush(handle.agent.session)
    await handle.dispose()
    const entered = Promise.withResolvers<undefined>()
    const release = Promise.withResolvers<undefined>()
    const stat = ctx.sessionPersistence.stat.bind(ctx.sessionPersistence)
    vi.spyOn(ctx.sessionPersistence, 'stat').mockImplementation(async (id) => {
      entered.resolve(undefined)
      await release.promise
      return stat(id)
    })
    class QuotaAdapter extends LlmAdapter {
      override async * stream(): AsyncIterable<StreamChunk> {
        yield { type: 'finish', reason: { kind: 'error', failure: { code: 'QUOTA', message: 'quota' } } }
      }
    }
    ctx.llm.registerAdapter(['mock'], new QuotaAdapter())
    const consuming = (async () => {
      for await (const _chunk of ctx.llm.stream({ provider: 'mock', model: 'mock', sessionId: SessionId('cold'), messages: [] })) { /* consume */ }
    })()
    await entered.promise
    const disposed = fiber.dispose()
    release.resolve(undefined)
    await Promise.all([disposed, consuming])
    expect(rows(media, 'cooldowns').size).toBe(0)
  })

  it('does not run queued auxiliary writes after teardown starts', async () => {
    const entered = Promise.withResolvers<undefined>()
    const release = Promise.withResolvers<undefined>()
    const { ctx, media } = await fixture({ unit(unit) {
      const put = unit.putRecord.bind(unit)
      unit.putRecord = async (table, key, value) => {
        if (table === 'pending') { entered.resolve(undefined); await release.promise }
        await put(table, key, value)
      }
    } })
    const { fiber } = await install(ctx)
    const agent = await actor(ctx)
    const recovery = recover(ctx, agent, new AbortController().signal, { code: 'QUOTA', message: 'quota', providerRetryAfterMs: 1000 })
    await entered.promise
    const auxiliary = Promise.withResolvers<undefined>()
    class QuotaAdapter extends LlmAdapter {
      override async * stream(): AsyncIterable<StreamChunk> {
        auxiliary.resolve(undefined)
        yield { type: 'finish', reason: { kind: 'error', failure: { code: 'QUOTA', message: 'quota', providerRetryAfterMs: 2000 } } }
      }
    }
    ctx.llm.registerAdapter(['mock'], new QuotaAdapter())
    const consuming = (async () => {
      for await (const _chunk of ctx.llm.stream({ provider: 'mock', model: 'mock', sessionId: agent.id, messages: [] })) { /* consume */ }
    })()
    await auxiliary.promise
    // Queue the auxiliary policy behind the blocked public backend write.
    await ctx.sessionPersistence.flush()
    const disposed = fiber.dispose()
    release.resolve(undefined)
    await Promise.all([recovery, consuming, disposed])
    const row = credentialPoolDomain.tables.cooldowns.valueSchema.parse(rows(media, 'cooldowns').get('KEY_ONE'))
    expect(row.retryAt - row.limitedAt).toBe(1000)
  })

  it('waits on the actual armed timer and retries only at the provider deadline', async () => {
    const { ctx, media } = await fixture()
    await install(ctx)
    const agent = await actor(ctx)
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    let settled = false
    const recovery = recover(ctx, agent, new AbortController().signal, {
      code: 'RATE_LIMIT', message: 'limit', providerRetryAfterMs: 1000,
    }).then((result) => { settled = true; return result })
    await armed(ctx, media)
    const deadline = credentialPoolDomain.tables.cooldowns.valueSchema.parse(rows(media, 'cooldowns').get('KEY_ONE')).retryAt
    await vi.advanceTimersByTimeAsync(deadline - Date.now() - 1)
    expect(settled).toBe(false)
    await vi.advanceTimersByTimeAsync(1)
    await expect(recovery).resolves.toEqual({ kind: 'retry' })
    await ctx.sessions.flush(agent.session)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('does not enter another recovery policy from a callback captured before disposal', async () => {
    const { ctx } = await fixture()
    const { fiber } = await install(ctx)
    const agent = await actor(ctx)
    const entered = Promise.withResolvers<undefined>()
    const release = Promise.withResolvers<undefined>()
    const downstream = vi.fn(() => Promise.resolve(undefined))
    ctx.on('agent/request-error', downstream)
    ctx.on('agent/request-error', async (_payload, next) => {
      entered.resolve(undefined)
      await release.promise
      return next()
    }, { prepend: true })
    const recovery = recover(ctx, agent, new AbortController().signal)
    await entered.promise
    await fiber.dispose()
    release.resolve(undefined)
    await expect(recovery).resolves.toBeUndefined()
    expect(downstream).not.toHaveBeenCalled()
  })

  it('delegates failures for an unpooled provider or an unbound request', async () => {
    const { ctx, media } = await fixture()
    await install(ctx)
    const agent = await ctx.agentLoop.create(SessionId('unbound'), { provider: 'other', model: 'other' })
    const next = vi.fn(() => Promise.resolve(undefined))
    const payload = { agent, signal: new AbortController().signal, provider: 'other', turn: 1, step: 1,
      failure: { code: 'QUOTA', message: 'quota' }, retryPolicy: undefined }
    await ctx.waterfall('agent/request-error', payload, next)
    await ctx.waterfall('agent/request-error', { ...payload, provider: 'mock' }, next)
    expect(next).toHaveBeenCalledTimes(2)
    expect(rows(media, 'pending').size).toBe(0)
  })

  it('keeps a cold parent binding available to a loaded child', async () => {
    const { ctx } = await fixture()
    const { owner } = await install(ctx)
    const parent = await ctx.agents.create({ sessionId: SessionId('cold-parent'), agentOptions: { provider: 'mock', model: 'mock' } })
    await ctx.sessions.flush(parent.agent.session)
    await parent.dispose()
    ctx.sessions.create(SessionId('child'), { meta: { origin: 'subagent', parentSession: SessionId('cold-parent') } })
    await expect(owner.reference('mock', SessionId('child'))).resolves.toBe('KEY_ONE')
  })
})

describe('credential-pool restoration', () => {
  it('does not enqueue a second wake while the original step is still retrying', async () => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const { ctx, media } = await fixture({ media: seeded() })
    const agent = await actor(ctx)
    await ctx.sessions.flush(agent.session)
    const { fiber } = await install(ctx)
    const recovery = recover(ctx, agent, new AbortController().signal, { code: 'QUOTA', message: 'quota', providerRetryAfterMs: 1000 })
    await armed(ctx, media)
    // Advance the restoration alarm first, leaving the live step's alarm queued.
    await vi.advanceTimersToNextTimerAsync()
    await vi.advanceTimersByTimeAsync(1000)
    await expect(recovery).resolves.toEqual({ kind: 'retry' })
    await fiber.dispose()
    expect(agent.inbox.nextTurn).toEqual([])
    expect(rows(media, 'pending').size).toBe(1)
  })

  it('rechecks a prolonged cooldown and leaves a live retry as the only wake driver', async () => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const { ctx, media } = await fixture({ media: seeded() })
    const agent = await actor(ctx)
    await ctx.sessions.flush(agent.session)
    await install(ctx)
    const recovery = recover(ctx, agent, new AbortController().signal, { code: 'QUOTA', message: 'quota', providerRetryAfterMs: 2000 })
    await armed(ctx, media)
    await vi.advanceTimersByTimeAsync(1000)
    expect(agent.inbox.nextTurn).toEqual([])
    await vi.advanceTimersByTimeAsync(1000)
    await expect(recovery).resolves.toEqual({ kind: 'retry' })
    expect(agent.inbox.nextTurn).toEqual([])
  })

  it('rechecks a credential extended while waiting for a busy Agent', async () => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const { ctx, media } = await fixture({ media: seeded({ retryAt: 0 }) })
    const agent = await actor(ctx)
    await ctx.sessions.flush(agent.session)
    const release = Promise.withResolvers<undefined>()
    const maintenance = agent.runMaintenance(() => release.promise)
    const waiting = Promise.withResolvers<undefined>()
    const whenIdle = agent.whenIdle.bind(agent)
    vi.spyOn(agent, 'whenIdle').mockImplementation(() => { waiting.resolve(undefined); return whenIdle() })
    class QuotaAdapter extends LlmAdapter {
      override async * stream(): AsyncIterable<StreamChunk> {
        yield { type: 'finish', reason: { kind: 'error', failure: { code: 'QUOTA', message: 'quota', providerRetryAfterMs: 1000 } } }
      }
    }
    ctx.llm.registerAdapter(['mock'], new QuotaAdapter())
    const { fiber } = await install(ctx)
    try {
      await waiting.promise
      for await (const _chunk of ctx.llm.stream({ provider: 'mock', model: 'mock', sessionId: agent.id, messages: [] })) { /* consume */ }
      release.resolve(undefined)
      await maintenance
      await ctx.sessionPersistence.flush()
      expect(agent.inbox.nextTurn).toEqual([])
      await fiber.dispose()
      expect(rows(media, 'pending').size).toBe(1)
    } finally { release.resolve(undefined); await maintenance }
  })

  it('does not wake a conversation that completes while restoration waits for its live driver', async () => {
    const { ctx, media } = await fixture({ media: seeded({ retryAt: 0 }) })
    const release = Promise.withResolvers<undefined>()
    const entered = Promise.withResolvers<undefined>()
    class BusyAdapter extends ReplyAdapter {
      override async * stream(options: GenerateOptions): AsyncIterable<StreamChunk> {
        entered.resolve(undefined)
        await release.promise
        yield* super.stream(options)
      }
    }
    ctx.llm.registerAdapter(['mock'], new BusyAdapter())
    const agent = await actor(ctx)
    agent.followup(createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: 'finish the game' }] }))
    await entered.promise
    await ctx.sessions.flush(agent.session)
    const waiting = Promise.withResolvers<undefined>()
    const whenIdle = agent.whenIdle.bind(agent)
    vi.spyOn(agent, 'whenIdle').mockImplementation(() => { waiting.resolve(undefined); return whenIdle() })
    const { fiber } = await install(ctx)
    await waiting.promise
    release.resolve(undefined)
    await agent.whenIdle()
    await fiber.dispose()
    expect(rows(media, 'pending').size).toBe(0)
    expect(agent.session.deriveMessages().filter(message => message.source.kind === 'credential-cooldown')).toEqual([])
  })

  it('preserves pending recovery when disposal overlaps resolving the original Session', async () => {
    const { ctx, media } = await fixture({ media: seeded({ retryAt: 0 }) })
    const agent = await actor(ctx)
    await ctx.sessions.flush(agent.session)
    const entered = Promise.withResolvers<undefined>()
    const release = Promise.withResolvers<undefined>()
    ctx.on('agent/resolve-session', async () => { entered.resolve(undefined); await release.promise; return agent })
    const { fiber } = await install(ctx)
    await entered.promise
    const disposed = fiber.dispose()
    release.resolve(undefined)
    await disposed
    expect(rows(media, 'pending').size).toBe(1)
    expect(agent.inbox.nextTurn).toEqual([])
  })

  it('reconstructs a crash-interrupted cooldown and retires a deleted target at its deadline', async () => {
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const media = seeded({ cooldown: false })
    const { ctx } = await fixture({ media })
    await install(ctx)
    expect(rows(media, 'cooldowns').get('KEY_ONE')).toMatchObject({ retryAt: Date.now() + 1000 })
    await vi.advanceTimersByTimeAsync(1000)
    await vi.waitFor(() => { expect(rows(media, 'pending').size).toBe(0) })
    expect(ctx.agents.list()).toEqual([])
  })

  it('leaves disabled providers dormant without waking or discarding their records', async () => {
    const { ctx, media } = await fixture({ media: seeded({ provider: 'disabled', retryAt: 0 }) })
    const { fiber } = await install(ctx)
    await fiber.dispose()
    expect(rows(media, 'pending').size).toBe(1)
  })

  it('wakes a loaded original Agent without creating a replacement identity', async () => {
    const { ctx, media } = await fixture({ media: seeded({ retryAt: 0 }) })
    ctx.llm.registerAdapter(['mock'], new ReplyAdapter())
    const agent = await actor(ctx)
    await ctx.sessions.flush(agent.session)
    await install(ctx)
    await vi.waitFor(() => { expect(agent.session.deriveMessages().some(message => message.source.kind === 'credential-cooldown')).toBe(true) })
    await agent.whenIdle()
    await vi.waitFor(() => { expect(rows(media, 'pending').size).toBe(0) })
    expect(ctx.agents.list().map(item => item.id)).toEqual([agent.id])
  })

  it('preserves a target when its embedding cannot restore it', async () => {
    const { ctx, media } = await fixture({ media: seeded({ retryAt: 0 }) })
    const handle = await ctx.agents.create({ sessionId: SessionId('root'), agentOptions: { provider: 'mock', model: 'mock' } })
    await ctx.sessions.flush(handle.agent.session)
    await handle.dispose()
    const attempted = Promise.withResolvers<undefined>()
    ctx.on('agent/resolve-session', (_id, next) => { attempted.resolve(undefined); return next() })
    const { fiber } = await install(ctx)
    await attempted.promise
    await fiber.dispose()
    expect(rows(media, 'pending').size).toBe(1)
    expect(ctx.agents.list()).toEqual([])
  })

  it('aborts a wait on a busy original Agent during plugin disposal', async () => {
    const { ctx, media } = await fixture({ media: seeded({ retryAt: 0 }) })
    const agent = await actor(ctx)
    await ctx.sessions.flush(agent.session)
    const release = Promise.withResolvers<undefined>()
    const maintenance = agent.runMaintenance(() => release.promise)
    const waiting = Promise.withResolvers<undefined>()
    const whenIdle = agent.whenIdle.bind(agent)
    vi.spyOn(agent, 'whenIdle').mockImplementation(() => { waiting.resolve(undefined); return whenIdle() })
    try {
      const { fiber } = await install(ctx)
      await waiting.promise
      await fiber.dispose()
    } finally {
      release.resolve(undefined)
      await maintenance
    }
    expect(rows(media, 'pending').size).toBe(1)
    expect(agent.inbox.nextTurn).toEqual([])
  })
})
