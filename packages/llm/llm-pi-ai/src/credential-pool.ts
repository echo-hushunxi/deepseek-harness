/** Sticky per-conversation credentials and durable quota recovery. */
import type { Context, Events } from '@deepseek-ai/cordis'
import type { Agent, AgentCancelCause, AgentRegistry, RequestErrorAction } from '@deepseek-ai/dsh-agent'
import { credentialRef, type CredentialRef } from '@deepseek-ai/dsh-credentials'
import { createUserMessage, isAgentLoopRequest, LlmError, QUOTA_EXCEEDED_CODE } from '@deepseek-ai/dsh-llm'
import type { LlmFailure, StreamChunk } from '@deepseek-ai/dsh-llm'
import { RetryId } from '@deepseek-ai/dsh-llm-retry'
import type { SessionStore, SessionId } from '@deepseek-ai/dsh-session'
import type { SessionPersistence } from '@deepseek-ai/dsh-session-persistence'
import type { Domain } from '@deepseek-ai/dsh-storage-domain'
import { randomUUID } from 'node:crypto'
import { credentialPoolDomain } from './credential-pool-storage.ts'

/** One provider's ordered credential references and fallback quota window. */
export interface CredentialPoolConfig {
  /** Distinct references in allocation order; values never enter configuration. */
  apiKeyEnvs: string[]
  /** Wait after a quota failure with no provider delay; omission uses five hours. */
  cooldownMs?: number
}

declare module '@deepseek-ai/dsh-llm' {
  interface MessageSourceMap {
    /** Restart recovery attribution; readers preserve the message without this producer.
     * @persistenceAttribution
     */
    'credential-cooldown': { kind: 'credential-cooldown' }
  }
}

type PoolDomain = Domain<typeof credentialPoolDomain>
interface PoolServices {
  agents: AgentRegistry
  sessions: SessionStore
  persistence: SessionPersistence
}
interface CredentialPool {
  refs: CredentialRef[]
  cooldownMs: number
}

/** Provider delay fallback; configured per pool rather than per request. */
export const DEFAULT_CREDENTIAL_COOLDOWN_MS = 5 * 60 * 60 * 1000
const MAX_TIMER_MS = 2 ** 31 - 1

function quotaFailure(failure: LlmFailure): boolean {
  return failure.status === 429 || failure.code === 'RATE_LIMIT' || failure.code === QUOTA_EXCEEDED_CODE
}

/** Cancellable wall-clock wait, segmented to fit the platform timer range. */
async function waitUntil(deadline: number, signal: AbortSignal): Promise<boolean> {
  while (!signal.aborted && Date.now() < deadline) {
    await new Promise<void>((resolve) => {
      const finish = (): void => {
        clearTimeout(timer)
        signal.removeEventListener('abort', finish)
        resolve()
      }
      const timer = setTimeout(finish, Math.min(MAX_TIMER_MS, deadline - Date.now()))
      signal.addEventListener('abort', finish, { once: true })
    })
  }
  return !signal.aborted
}

/** Observe the existing driver without keeping restoration alive after disposal. */
async function waitForIdle(agent: Agent, signal: AbortSignal): Promise<boolean> {
  const aborted = Promise.withResolvers<boolean>()
  const onAbort = (): void => { aborted.resolve(false) }
  signal.addEventListener('abort', onAbort, { once: true })
  try {
    return await Promise.race([agent.whenIdle().then(() => true), aborted.promise])
  } finally {
    signal.removeEventListener('abort', onAbort)
  }
}

/**
 * Own persistent bindings and the recovery listeners for explicitly configured pools.
 * No pool means no additional service requirements or request behavior.
 */
export class CredentialPools {
  private readonly pools = new Map<string, CredentialPool>()
  private readonly lifetime = new AbortController()
  private readonly operations = new Set<Promise<unknown>>()
  private readonly recovering = new Set<SessionId>()
  private queue: Promise<unknown> = Promise.resolve()

  /**
   * @param ctx - owning adapter fiber.
   * @param domain - opened private metadata domain.
   * @param services - facilities retained for the adapter lifetime.
   */
  private constructor(private readonly ctx: Context, private readonly domain: PoolDomain, private readonly services: PoolServices) {}

  /**
   * Validate the pool definitions and mount recovery before exposing credential lookup.
   * @param ctx - adapter context carrying the configured Host facilities.
   * @param configs - provider route to ordered credential pool.
   * @returns the ready pool owner, or undefined for an empty configuration.
   */
  static async create(ctx: Context, configs: Readonly<Record<string, CredentialPoolConfig>>): Promise<CredentialPools | undefined> {
    if (Object.keys(configs).length === 0) return undefined
    const agents = ctx.get('agents')
    const sessions = ctx.get('sessions')
    const persistence = ctx.get('sessionPersistence')
    const storage = ctx.get('storageDomain')
    if (agents === undefined || sessions === undefined || persistence === undefined || storage === undefined) {
      throw new Error('llm-pi-ai: credentialPools requires agents, sessions, sessionPersistence, and storageDomain')
    }
    const domain = await storage.open(credentialPoolDomain)
    const owner = new CredentialPools(ctx, domain, { agents, sessions, persistence })
    ctx.effect(() => async () => {
      owner.lifetime.abort()
      await Promise.allSettled([...owner.operations])
      await owner.queue
      await domain.close()
    }, 'llm-pi-ai: drain credential-pool recovery')
    for (const [provider, config] of Object.entries(configs)) {
      const refs = config.apiKeyEnvs.map(credentialRef)
      if (provider.length === 0 || refs.length === 0 || new Set(refs).size !== refs.length) {
        throw new Error('llm-pi-ai: credentialPools requires non-empty providers and distinct credential references')
      }
      const cooldownMs = config.cooldownMs ?? DEFAULT_CREDENTIAL_COOLDOWN_MS
      if (!Number.isSafeInteger(cooldownMs) || cooldownMs <= 0 || cooldownMs > MAX_TIMER_MS) {
        throw new Error(`llm-pi-ai: credentialPools.${provider}.cooldownMs must be an integer from 1 through ${MAX_TIMER_MS}`)
      }
      owner.pools.set(provider, { refs, cooldownMs })
    }
    // A pending row is committed first, so a crash between the two writes
    // can rebuild the shared cooldown without losing the wake obligation.
    for (const [, row] of domain.table('pending').entries()) {
      if ((domain.table('cooldowns').get(row.ref)?.retryAt ?? 0) < row.retryAt) {
        await domain.table('cooldowns').put(row.ref, { limitedAt: row.limitedAt, retryAt: row.retryAt })
      }
    }
    ctx.on('agent/request-error', (payload, next) => owner.track(owner.recover(payload, next)), { prepend: true })
    ctx.on('llm/stream', (options, next) => {
      const stream = next()
      const pool = owner.pools.get(options.provider)
      return pool !== undefined && !isAgentLoopRequest(options) && options.sessionId !== undefined
        ? owner.observeAuxiliary(options.provider, options.sessionId, pool, stream) : stream
    })
    ctx.on('agent/created', async ({ agent, signal }) => {
      const provider = agent.model.current()?.provider
      const pool = provider === undefined ? undefined : owner.pools.get(provider)
      if (provider !== undefined && pool !== undefined) {
        await owner.track(owner.bind(provider, pool, agent.session.id, signal))
      }
    })
    ctx.on('session/event', (session, event) => {
      const pending = event.type === 'assistant/message' ? domain.table('pending').get(session.id) : undefined
      if (pending !== undefined) {
        owner.track(owner.serialize(async () => {
          if (domain.table('pending').get(session.id)?.id === pending.id) await domain.table('pending').delete(session.id)
        }))
          .catch((error: unknown) => { ctx.logger.error('llm-pi-ai: unable to retire credential recovery: %o', error) })
      }
    })
    // Only persisted waits need a new driver. Live waits retry the original
    // open step and never inject a continuation message.
    const roots = new Set([...domain.table('pending').entries()].map(([, item]) => item.rootSessionId))
    for (const root of roots) {
      owner.track(owner.restore(root)).catch((error: unknown) => {
        ctx.logger.error('llm-pi-ai: credential recovery remains pending for session %s: %o', root, error)
      })
    }
    return owner
  }

  private track<T>(operation: Promise<T>): Promise<T> {
    const tracked = operation.finally(() => { this.operations.delete(tracked) })
    this.operations.add(tracked)
    return tracked
  }

  private isAlive(): boolean {
    return !this.lifetime.signal.aborted
  }

  private cooldown(ref: CredentialRef): { limitedAt: number; retryAt: number } {
    return this.domain.table('cooldowns').get(ref) ?? { limitedAt: Date.now(), retryAt: 0 }
  }

  private retireOnAbort(signal: AbortSignal): boolean {
    // Agent teardown may precede this plugin's unload in a shipped profile.
    // Its durable recovery must survive either service-disposal order.
    const cause = signal.reason as AgentCancelCause | undefined
    return this.isAlive() && cause?.kind !== 'disposed'
  }

  private async * observeAuxiliary(
    provider: string, sessionId: SessionId, pool: CredentialPool, stream: AsyncIterable<StreamChunk>,
  ): AsyncIterable<StreamChunk> {
    for await (const chunk of stream) {
      if (chunk.type === 'finish' && chunk.reason.kind === 'error' && quotaFailure(chunk.reason.failure) && this.isAlive()) {
        const failure = chunk.reason.failure
        await this.track((async () => {
          const root = await this.rootOf(sessionId)
          if (!this.isAlive()) return
          const ref = this.domain.table('bindings').get(root)?.[provider]
          if (ref !== undefined) {
            const limitedAt = Date.now()
            const delay = Math.ceil(failure.providerRetryAfterMs ?? pool.cooldownMs)
            await this.serialize(async () => {
              if (!this.isAlive()) return
              const retryAt = Math.max(this.cooldown(ref).retryAt,
                limitedAt + Math.min(Number.MAX_SAFE_INTEGER - limitedAt, delay))
              await this.domain.table('cooldowns').put(ref, { limitedAt, retryAt })
            })
          }
        })())
      }
      yield chunk
    }
  }

  private serialize<T>(operation: () => Promise<T>): Promise<T> {
    const next = this.queue.then(operation)
    // The operation's returned promise owns the failure; keep later queue slots usable.
    this.queue = next.catch((_operationFailure: unknown) => undefined)
    return next
  }

  private async rootOf(sessionId: SessionId): Promise<SessionId> {
    let id = sessionId
    const visited = new Set<SessionId>()
    while (!visited.has(id)) {
      visited.add(id)
      const header = this.services.sessions.get(id)?.header
        ?? (await this.services.persistence.stat(id))?.header
      if (header === undefined) throw new LlmError('Credential pooling requires an existing Session', 'MISSING_SESSION')
      if (header.origin !== 'subagent') return id
      if (header.parentSession === undefined) throw new LlmError('A subagent Session has no owning parent', 'INVALID_SESSION')
      id = header.parentSession
    }
    throw new LlmError('Credential pooling found cyclic subagent ownership', 'INVALID_SESSION')
  }

  /**
   * Select once per root conversation and provider; later requests retain that reference.
   * Discovery without a Session uses the first reference without allocating a conversation.
   * @param provider - exact configured route.
   * @param sessionId - calling Session; omitted for model discovery.
   * @param signal - request cancellation, when the caller supplies one.
   * @returns the pinned reference, or undefined when this route has no pool.
   */
  async reference(provider: string, sessionId?: SessionId, signal?: AbortSignal): Promise<CredentialRef | undefined> {
    const pool = this.pools.get(provider)
    if (pool === undefined) return undefined
    if (sessionId === undefined) return pool.refs[0]
    const ref = await this.bind(provider, pool, sessionId, signal)
    signal?.throwIfAborted()
    this.lifetime.signal.throwIfAborted()
    const remaining = this.cooldown(ref).retryAt - Date.now()
    if (remaining > 0) {
      throw new LlmError('The Session credential is cooling down; the original request will resume after its quota resets',
        'CREDENTIAL_COOLDOWN', { providerRetryAfterMs: remaining })
    }
    return ref
  }

  private async bind(provider: string, pool: CredentialPool, sessionId: SessionId, signal?: AbortSignal): Promise<CredentialRef> {
    const root = await this.rootOf(sessionId)
    return this.serialize(async () => {
      signal?.throwIfAborted()
      this.lifetime.signal.throwIfAborted()
      const bindings = this.domain.table('bindings').get(root) ?? {}
      const existing = bindings[provider]
      if (existing !== undefined) {
        if (!pool.refs.includes(existing)) {
          throw new LlmError('The Session credential was removed from its pool; restore its reference to continue', 'MISSING_CREDENTIAL')
        }
        return existing
      }
      const cursors: Record<string, CredentialRef> = this.domain.global.get()
      const previous = cursors[provider]
      const start = previous === undefined ? 0 : (pool.refs.indexOf(previous) + 1) % pool.refs.length
      const order = pool.refs.slice(start).concat(pool.refs.slice(0, start))
      const available = order.find(candidate => this.cooldown(candidate).retryAt <= Date.now())
      // All keys cooling still binds once, to the earliest available key.
      const chosen = available ?? order.reduce((left, right) =>
        this.cooldown(left).retryAt <= this.cooldown(right).retryAt ? left : right)
      await this.domain.global.set({ ...cursors, [provider]: chosen })
      await this.domain.table('bindings').put(root, { ...bindings, [provider]: chosen })
      return chosen
    })
  }

  private async recover(
    payload: Parameters<Events['agent/request-error']>[0],
    next: () => Promise<RequestErrorAction>,
  ): Promise<RequestErrorAction> {
    const { agent, provider, failure, signal, turn, step } = payload
    const pool = this.pools.get(provider)
    if (!this.isAlive()) return
    if (pool === undefined) return next()
    if (!quotaFailure(failure) && failure.code !== 'CREDENTIAL_COOLDOWN') {
      await this.serialize(async () => { await this.domain.table('pending').delete(agent.session.id) })
      return next()
    }
    const root = await this.rootOf(agent.session.id)
    const ref = this.domain.table('bindings').get(root)?.[provider]
    if (ref === undefined) return next()
    const observedAt = Date.now()
    const limitedAt = failure.code === 'CREDENTIAL_COOLDOWN'
      ? this.cooldown(ref).limitedAt : observedAt
    const retryId = RetryId(randomUUID())
    const retryAt = await this.serialize(async () => {
      if (signal.aborted || !this.isAlive()) return limitedAt
      const deadline = observedAt + Math.min(Number.MAX_SAFE_INTEGER - observedAt,
        Math.ceil(failure.providerRetryAfterMs ?? pool.cooldownMs))
      const retryAt = Math.max(this.cooldown(ref).retryAt, deadline)
      await this.domain.table('pending').put(agent.session.id, {
        id: retryId, sessionId: agent.session.id, rootSessionId: root, provider, ref, limitedAt, retryAt,
      })
      await this.domain.table('cooldowns').put(ref, { limitedAt, retryAt })
      return retryAt
    })
    if (signal.aborted || !this.isAlive()) {
      if (this.retireOnAbort(signal)) {
        await this.serialize(async () => { await this.domain.table('pending').delete(agent.session.id) })
      }
      return
    }
    agent.session.append('llm/retry', {
      retryId, turn, step, provider, mode: 'always', policyKey: `credential-pool:${provider}`,
      retry: 1, delayMs: retryAt - observedAt, failure,
    })
    await this.services.sessions.flush(agent.session)
    this.recovering.add(agent.session.id)
    try {
      const fused = AbortSignal.any([signal, this.lifetime.signal])
      while (!fused.aborted) {
        const deadline = Math.max(retryAt, this.cooldown(ref).retryAt)
        if (!await waitUntil(deadline, fused)) break
        if (this.cooldown(ref).retryAt > Date.now()) continue
        agent.session.append('llm/retry-started', { retryId, turn, step, retry: 1 })
        return { kind: 'retry' }
      }
      if (this.retireOnAbort(signal)) {
        await this.serialize(async () => { await this.domain.table('pending').delete(agent.session.id) })
      }
    } finally {
      this.recovering.delete(agent.session.id)
    }
  }

  private async restore(root: SessionId): Promise<void> {
    const entries = () =>
      [...this.domain.table('pending').entries()].filter(([, row]) => row.rootSessionId === root && this.pools.has(row.provider))
    while (this.isAlive()) {
      const pending = entries()
      if (pending.length === 0) return
      if (pending.some(([, row]) => this.recovering.has(row.sessionId))) return
      const deadline = Math.max(...pending.map(([, row]) => Math.max(row.retryAt, this.cooldown(row.ref).retryAt)))
      if (!await waitUntil(deadline, this.lifetime.signal)) return
      if (entries().some(([, row]) => this.cooldown(row.ref).retryAt > Date.now())) continue
      if (await this.services.persistence.stat(root) === undefined) {
        await this.serialize(async () => { for (const [id] of pending) await this.domain.table('pending').delete(id) })
        return
      }
      const agent = await this.ctx.waterfall('agent/resolve-session', root, () => {
        const live = this.services.agents.get(root)
        if (live !== undefined) return Promise.resolve(live)
        return Promise.reject(new Error('Resume the original Session or provide agent/resolve-session'))
      })
      if (!this.isAlive()) return
      if (!await waitForIdle(agent, this.lifetime.signal)) return
      await this.queue
      const ready = entries()
      if (ready.length === 0) return
      if (ready.some(([, row]) => Math.max(row.retryAt, this.cooldown(row.ref).retryAt) > Date.now())) continue
      agent.followup(createUserMessage({
        source: { kind: 'credential-cooldown' },
        content: [{ type: 'text', text: 'Continue the interrupted task after the provider credential cooldown. Preserve completed work and continue from the existing conversation.' }],
      }))
      await this.services.sessions.flush(agent.session)
      await this.serialize(async () => {
        for (const [id, row] of ready) {
          if (this.domain.table('pending').get(id)?.id === row.id) await this.domain.table('pending').delete(id)
        }
      })
      return
    }
  }
}
