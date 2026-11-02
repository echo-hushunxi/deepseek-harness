import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import Loader, { type ModuleLoaderV2 } from '@deepseek-ai/cordis-plugin-loader'
import Include from '@deepseek-ai/cordis-plugin-include'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import LlmRuntime, { createUserMessage } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'
import * as Jsonl from '@deepseek-ai/dsh-session-persistence-jsonl'
import Credentials from '@deepseek-ai/dsh-credentials-local'
import Storage from '@deepseek-ai/dsh-storage'
import * as StorageJson from '@deepseek-ai/dsh-storage-json'
import * as StorageDomain from '@deepseek-ai/dsh-storage-domain'
import * as PiAi from '../src/index.ts'
import { DEFAULT_CREDENTIAL_COOLDOWN_MS } from '../src/credential-pool.ts'
import { assemble } from './assemble.ts'
import { closeMockServers, mockServer, textEvents } from './mock-server.ts'

const contexts: Context[] = []
const roots: string[] = []
const refs = ['POOL_KEY_ONE', 'POOL_KEY_TWO', 'POOL_KEY_THREE']

afterEach(async () => {
  await Promise.all(contexts.splice(0).map(ctx => ctx.fiber.dispose()))
  vi.useRealTimers()
  await closeMockServers()
  await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true })))
})

async function rootDir(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'dsh-credential-pool-'))
  roots.push(root)
  await writeFile(join(root, '.credentials.yaml'), [
    'version: 1', 'refs:',
    '  POOL_KEY_ONE: key-one', '  POOL_KEY_TWO: key-two', '  POOL_KEY_THREE: key-three', '',
  ].join('\n'), { mode: 0o600 })
  return root
}

async function boot(
  root: string,
  baseURL: string,
  options: { refs?: string[]; cooldownMs?: number; restore?: boolean; loopLast?: boolean } = {},
): Promise<Context> {
  const ctx = new Context()
  contexts.push(ctx)
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(Storage)
  await ctx.plugin(StorageJson, { root: join(root, 'domains') })
  await ctx.plugin(StorageDomain, { backend: 'json' })
  await ctx.plugin(Jsonl, { root: join(root, 'sessions') })
  await ctx.plugin(Credentials, { path: join(root, '.credentials.yaml'), debounceMs: 10 })
  if (!options.loopLast) await ctx.plugin(AgentLoop, { agents: [] })
  if (options.restore) {
    ctx.on('agent/resolve-session', async (id) => {
      return (await ctx.agentLoop.resume(ctx, { resumeSessionId: id })).agent
    })
  }
  const configPath = join(root, 'credential-pool.cordis.yml')
  await writeFile(configPath, [
    '- id: pi-ai', "  name: '@deepseek-ai/dsh-llm-pi-ai'", '  config:',
    '    credentialPools:', '      deepseek:',
    `        apiKeyEnvs: ${JSON.stringify(options.refs ?? refs)}`,
    ...options.cooldownMs === undefined ? [] : [`        cooldownMs: ${options.cooldownMs}`],
    '    providers:', '      deepseek:', '        apiKeyEnv: POOL_KEY_ONE', `        baseURL: ${JSON.stringify(baseURL)}`, '',
  ].join('\n'))
  ctx.baseUrl = pathToFileURL(root).href + '/'
  await ctx.plugin(Loader)
  ctx.loader.builtins.include = Include
  const internal: ModuleLoaderV2 = {
    version: 'v2', loadCache: new Map(),
    import: (specifier) => {
      if (specifier !== '@deepseek-ai/dsh-llm-pi-ai') throw new Error(`unexpected import: ${specifier}`)
      return Promise.resolve(PiAi)
    },
    register(): never { throw new Error('unexpected module hook registration') },
    getOrCreateModuleJob(): never { throw new Error('unexpected module job creation') },
    resolveSync(): never { throw new Error('unexpected synchronous module resolution') },
    load(): never { throw new Error('unexpected module load') },
  }
  ctx.loader.internal = internal
  await ctx.loader.create({ name: 'cordis:include', config: { path: pathToFileURL(configPath).href } })
  await ctx.loader.await()
  if (options.loopLast) await ctx.plugin(AgentLoop, { agents: [] })
  expect([...ctx.loader.entries()].filter(entry => entry.fiber === undefined && !entry.disabled)).toEqual([])
  return ctx
}

function prompt(text = 'hello') {
  return createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text }] })
}

async function request(ctx: Context, id: string): Promise<void> {
  const agent = ctx.agents.get(SessionId(id)) ?? await ctx.agentLoop.create(SessionId(id), { provider: 'deepseek', model: 'deepseek-flash' })
  agent.followup(prompt())
  await agent.whenIdle()
}

async function metadata(root: string) {
  return JSON.parse(await readFile(join(root, 'domains', 'pi_ai_credential_pool.json'), 'utf8')) as {
    tables: {
      bindings: Record<string, Record<string, string>>
      cooldowns: Record<string, { limitedAt: number; retryAt: number }>
      pending: Record<string, unknown>
    }
  }
}

describe('pi-ai credential pools through the Loader, HTTP adapter, and AgentLoop', () => {
  it('fails before provider I/O when the embedding has not mounted pool facilities', async () => {
    const ctx = new Context()
    contexts.push(ctx)
    await ctx.plugin(LlmRuntime)
    await ctx.plugin(PiAi, { credentialPools: { deepseek: { apiKeyEnvs: refs } }, providers: { deepseek: { apiKeyEnv: 'POOL_KEY_ONE' } } })
    const result = await assemble(ctx, { model: 'deepseek-flash', messages: [], sessionId: SessionId('unavailable') })
    expect(result.finish).toMatchObject({ kind: 'error', failure: { code: 'MISSING_CREDENTIAL_POOL' } })
  })

  it('keeps direct plugin callers compatible when they omit optional pool configuration', async () => {
    const ctx = new Context()
    contexts.push(ctx)
    await ctx.plugin(LlmRuntime)
    await ctx.plugin({ name: 'no-pool-config', inject: PiAi.inject, apply(child: Context) {
      PiAi.apply(child, { providers: PiAi.Config({}).providers })
    } })
    expect(ctx.llm.listProviders()).toEqual([])
  })

  it('allocates new conversations in order and keeps follow-ups on the original key', async () => {
    const root = await rootDir()
    const server = await mockServer(Array.from({ length: 5 }, () => ({ events: textEvents })))
    const ctx = await boot(root, server.url)
    await request(ctx, 'game-one')
    await request(ctx, 'game-two')
    await request(ctx, 'game-one')
    await request(ctx, 'game-three')
    await request(ctx, 'game-two')
    expect(server.headers.map(header => header.authorization)).toEqual([
      'Bearer key-one', 'Bearer key-two', 'Bearer key-one', 'Bearer key-three', 'Bearer key-two',
    ])
    const disk = await readFile(join(root, 'domains', 'pi_ai_credential_pool.json'), 'utf8')
    expect(disk).not.toContain('key-one')
    expect(disk).not.toContain('key-two')
  })

  it('serializes simultaneous first requests and shares the parent binding with a subagent', async () => {
    const root = await rootDir()
    const server = await mockServer(Array.from({ length: 5 }, () => ({ events: textEvents })))
    const ctx = await boot(root, server.url)
    await Promise.all(['a', 'b', 'c'].map(id => request(ctx, id)))
    expect(server.headers.map(header => header.authorization).sort()).toEqual(['Bearer key-one', 'Bearer key-three', 'Bearer key-two'])
    ctx.sessions.create(SessionId('child'), { meta: { origin: 'subagent', parentSession: SessionId('a') } })
    const parent = await assemble(ctx, { model: 'deepseek-flash', sessionId: SessionId('child'), messages: [prompt()] })
    expect(parent.finish.kind).toBe('stop')
    const binding = (await metadata(root)).tables.bindings['a']?.deepseek
    const values: Record<string, string> = { POOL_KEY_ONE: 'key-one', POOL_KEY_TWO: 'key-two', POOL_KEY_THREE: 'key-three' }
    expect(server.headers.at(-1)?.authorization).toBe(`Bearer ${values[binding!]}`)
    const fork = ctx.sessions.fork(ctx.agents.get(SessionId('a'))!.session, undefined, SessionId('ordinary-fork'))
    await assemble(ctx, { model: 'deepseek-flash', sessionId: fork.id, messages: [prompt()] })
    expect((await metadata(root)).tables.bindings['ordinary-fork']).toEqual({ deepseek: 'POOL_KEY_ONE' })
  })

  it('preserves bindings and allocation across restart and reordered pool configuration', async () => {
    const root = await rootDir()
    const server = await mockServer(Array.from({ length: 3 }, () => ({ events: textEvents })))
    const first = await boot(root, server.url)
    await request(first, 'game-one')
    await first.fiber.dispose()
    const second = await boot(root, server.url, { refs: [...refs].reverse() })
    const resumed = await second.agentLoop.resume(second, { resumeSessionId: SessionId('game-one') })
    resumed.agent.followup(prompt())
    await resumed.agent.whenIdle()
    await request(second, 'new-game')
    expect(server.headers[1]?.authorization).toBe('Bearer key-one')
    expect(server.headers[2]?.authorization).toBe('Bearer key-three')
  })

  it('waits five hours on quota exhaustion and retries the same step with the same key', async () => {
    const root = await rootDir()
    const server = await mockServer([
      { status: 429, body: '{"error":{"message":"quota exceeded","type":"insufficient_quota"}}' },
      { events: textEvents },
    ])
    const ctx = await boot(root, server.url)
    const agent = await ctx.agentLoop.create(SessionId('quota-game'), { provider: 'deepseek', model: 'deepseek-flash' })
    const scheduled = Promise.withResolvers<undefined>()
    ctx.on('session/event', (_session, event) => { if (event.type === 'llm/retry') scheduled.resolve(undefined) })
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    agent.followup(prompt())
    await scheduled.promise
    const event = agent.session.snapshotEvents().find(row => row.type === 'llm/retry')
    expect(event?.data).toMatchObject({ delayMs: DEFAULT_CREDENTIAL_COOLDOWN_MS })
    expect(server.headers).toHaveLength(1)
    await vi.advanceTimersByTimeAsync(DEFAULT_CREDENTIAL_COOLDOWN_MS - 1)
    expect(server.headers).toHaveLength(1)
    await vi.advanceTimersByTimeAsync(1)
    await agent.whenIdle()
    expect(server.headers.map(header => header.authorization)).toEqual(['Bearer key-one', 'Bearer key-one'])
    expect(agent.session.snapshotEvents().filter(row => row.type === 'turn/start')).toHaveLength(1)
    expect(agent.session.deriveMessages().at(-1)?.content).toEqual([{ type: 'text', text: 'hello' }])
  })

  it('cancels the pending wake without changing the key or retrying after cancellation', async () => {
    const root = await rootDir()
    const server = await mockServer([{ status: 429, body: '{"error":{"message":"rate limit"}}' }])
    const ctx = await boot(root, server.url)
    const agent = await ctx.agentLoop.create(SessionId('canceled'), { provider: 'deepseek', model: 'deepseek-flash' })
    const scheduled = Promise.withResolvers<undefined>()
    ctx.on('session/event', (_session, event) => { if (event.type === 'llm/retry') scheduled.resolve(undefined) })
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    agent.followup(prompt())
    await scheduled.promise
    agent.cancel({ kind: 'user' })
    await agent.whenIdle()
    await vi.advanceTimersByTimeAsync(DEFAULT_CREDENTIAL_COOLDOWN_MS)
    expect(server.headers).toHaveLength(1)
    expect((await metadata(root)).tables.pending).toEqual({})
  })

  it('retains a suspended conversation across shutdown and automatically wakes that identity after restart', async () => {
    const root = await rootDir()
    const server = await mockServer([
      { status: 429, body: '{"error":{"message":"rate limit"}}' },
      { events: textEvents },
    ])
    const first = await boot(root, server.url, { loopLast: true })
    const agent = await first.agentLoop.create(SessionId('restart-game'), { provider: 'deepseek', model: 'deepseek-flash' })
    const scheduled = Promise.withResolvers<undefined>()
    first.on('session/event', (_session, event) => { if (event.type === 'llm/retry') scheduled.resolve(undefined) })
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    agent.followup(prompt('build my game'))
    await scheduled.promise
    await first.fiber.dispose()
    expect(Object.keys((await metadata(root)).tables.pending)).toEqual(['restart-game'])
    const second = await boot(root, server.url, { restore: true })
    await vi.advanceTimersByTimeAsync(DEFAULT_CREDENTIAL_COOLDOWN_MS - 1)
    expect(server.headers).toHaveLength(1)
    await vi.advanceTimersByTimeAsync(1)
    await vi.waitFor(() =>{  expect(second.agents.get(SessionId('restart-game'))?.status).toBe('idle') })
    expect(server.headers.map(header => header.authorization)).toEqual(['Bearer key-one', 'Bearer key-one'])
    expect(second.agents.list().map(item => item.id)).toEqual(['restart-game'])
    const wakes = second.agents.get(SessionId('restart-game'))?.session.snapshotEvents()
      .filter(row => row.type === 'user/message' && row.data.source.kind === 'credential-cooldown')
    expect(wakes).toHaveLength(1)
    expect(wakes?.[0]?.data).toMatchObject({
      content: [{ type: 'text', text: 'Continue the interrupted task after the provider credential cooldown. Preserve completed work and continue from the existing conversation.' }],
    })
  })

  it('leaves authentication errors terminal instead of starting a quota wake', async () => {
    const root = await rootDir()
    const server = await mockServer([{ status: 401, body: '{"error":{"message":"invalid key"}}' }])
    const ctx = await boot(root, server.url)
    await request(ctx, 'bad-auth')
    expect(server.headers).toHaveLength(1)
    expect((await metadata(root)).tables.pending).toEqual({})
    expect(ctx.agents.get(SessionId('bad-auth'))?.session.snapshotEvents().filter(row => row.type === 'llm/retry')).toEqual([])
  })

  it.each([
    { header: '120', delay: 120_000 },
    { header: 'Wed, 21 Oct 2037 07:28:00 GMT', delay: 60_000 },
    { header: 'invalid', delay: DEFAULT_CREDENTIAL_COOLDOWN_MS },
  ])('honors HTTP Retry-After $header before retrying the pinned key', async ({ header, delay }) => {
    const root = await rootDir()
    const server = await mockServer([
      { status: 429, headers: { 'Retry-After': header }, body: '{"error":{"message":"too many requests"}}' },
      { events: textEvents },
    ])
    const ctx = await boot(root, server.url)
    const agent = await ctx.agentLoop.create(SessionId('retry-after'), { provider: 'deepseek', model: 'deepseek-flash' })
    const scheduled = Promise.withResolvers<undefined>()
    ctx.on('session/event', (_session, event) => { if (event.type === 'llm/retry') scheduled.resolve(undefined) })
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    vi.setSystemTime(new Date('2037-10-21T07:27:00Z'))
    agent.followup(prompt())
    await scheduled.promise
    expect(agent.session.snapshotEvents().find(row => row.type === 'llm/retry')?.data).toMatchObject({ delayMs: delay })
    await vi.advanceTimersByTimeAsync(delay)
    await agent.whenIdle()
    expect(server.headers.map(item => item.authorization)).toEqual(['Bearer key-one', 'Bearer key-one'])
  })

  it('keeps another new conversation usable while the first key cools down', async () => {
    const root = await rootDir()
    const server = await mockServer([
      { status: 429, body: '{"error":{"message":"quota exceeded"}}' },
      { events: textEvents }, { events: textEvents },
    ])
    const ctx = await boot(root, server.url)
    const first = await ctx.agentLoop.create(SessionId('first-cooling'), { provider: 'deepseek', model: 'deepseek-flash' })
    const scheduled = Promise.withResolvers<undefined>()
    ctx.on('session/event', (_session, event) => { if (event.type === 'llm/retry') scheduled.resolve(undefined) })
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    first.followup(prompt())
    await scheduled.promise
    await request(ctx, 'second-usable')
    expect(server.headers.map(item => item.authorization)).toEqual(['Bearer key-one', 'Bearer key-two'])
    await vi.advanceTimersByTimeAsync(DEFAULT_CREDENTIAL_COOLDOWN_MS)
    await first.whenIdle()
    expect(server.headers.at(-1)?.authorization).toBe('Bearer key-one')
  })

  it('never falls back to the profile key when the assigned reference is missing', async () => {
    const root = await rootDir()
    const server = await mockServer([])
    const ctx = await boot(root, server.url, { refs: ['UNSET_POOL_KEY'] })
    await request(ctx, 'missing-key')
    expect(server.headers).toEqual([])
    expect(ctx.agents.get(SessionId('missing-key'))?.session.snapshotEvents().at(-1)?.data).toMatchObject({
      reason: { kind: 'error', error: { code: 'MISSING_CREDENTIAL' } },
    })
  })

  it('refuses pooled inference without a Session while leaving discovery available', async () => {
    const root = await rootDir()
    const server = await mockServer([])
    const ctx = await boot(root, server.url)
    const response = await assemble(ctx, { model: 'deepseek-flash', messages: [prompt()] })
    expect(response.finish).toMatchObject({ kind: 'error', failure: { code: 'MISSING_SESSION' } })
    expect(server.headers).toEqual([])
    expect((await ctx.llm.listModels('deepseek')).length).toBeGreaterThan(0)
  })

  it('shares auxiliary-call quota with the conversation instead of borrowing another key', async () => {
    const root = await rootDir()
    const server = await mockServer([
      { status: 429, headers: { 'Retry-After': '1' }, body: '{"error":{"message":"quota exceeded"}}' },
      { events: textEvents },
    ])
    const ctx = await boot(root, server.url)
    const agent = await ctx.agentLoop.create(SessionId('auxiliary'), { provider: 'deepseek', model: 'deepseek-flash' })
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
    const response = await assemble(ctx, { model: 'deepseek-flash', sessionId: agent.id, messages: [prompt('title')] })
    expect(response.finish.kind).toBe('error')
    expect((await metadata(root)).tables.cooldowns.POOL_KEY_ONE).toMatchObject({ retryAt: Date.now() + 1000 })
    const scheduled = Promise.withResolvers<undefined>()
    ctx.on('session/event', (_session, event) => { if (event.type === 'llm/retry') scheduled.resolve(undefined) })
    agent.followup(prompt())
    await scheduled.promise
    expect(server.headers).toHaveLength(1)
    await vi.advanceTimersByTimeAsync(1000)
    await agent.whenIdle()
    expect(server.headers.map(item => item.authorization)).toEqual(['Bearer key-one', 'Bearer key-one'])
  })
})
