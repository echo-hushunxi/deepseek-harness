/** Credential lookup cost without provider transport. Run with Node --expose-gc --import tsx; pass baseline or pool. */
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Context } from '@deepseek-ai/cordis'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import { credentialRef } from '@deepseek-ai/dsh-credentials'
import Credentials from '@deepseek-ai/dsh-credentials-local'
import { SessionId } from '@deepseek-ai/dsh-session'
import * as Jsonl from '@deepseek-ai/dsh-session-persistence-jsonl'
import Storage from '@deepseek-ai/dsh-storage'
import * as StorageJson from '@deepseek-ai/dsh-storage-json'
import * as StorageDomain from '@deepseek-ai/dsh-storage-domain'
import { CredentialPools } from '../src/credential-pool.ts'
const mode = process.argv[2]
if (mode !== 'baseline' && mode !== 'pool') throw new Error('baseline or pool required')
if (global.gc === undefined) throw new Error('--expose-gc required')
const root = await mkdtemp(join(tmpdir(), 'dsh-pool-perf-'))
const ctx = new Context()
try {
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(Storage)
  await ctx.plugin(StorageJson, { root: join(root, 'domains') })
  await ctx.plugin(StorageDomain, { backend: 'json' })
  await ctx.plugin(Jsonl, { root: join(root, 'sessions') })
  await writeFile(join(root, '.credentials.yaml'), 'version: 1\nrefs:\n  PERF_KEY: fictitious-perf-key\n', { mode: 0o600 })
  await ctx.plugin(Credentials, { path: join(root, '.credentials.yaml'), watch: false })
  const session = ctx.sessions.create(SessionId('perf-existing'))
  const owner = mode === 'pool' ? await CredentialPools.create(ctx, { mock: { apiKeyEnvs: ['PERF_KEY'] } }) : undefined
  const fixed = credentialRef('PERF_KEY')
  const lookup = async () => {
    const ref = owner === undefined ? fixed : await owner.reference('mock', session.id)
    if (ref === undefined) throw new Error('missing reference')
    const hit = await ctx.credentials.resolve(ref)
    if (hit === undefined) throw new Error('missing fixture credential')
  }
  const first = performance.now()
  await lookup()
  const firstMs = performance.now() - first
  for (let i = 0; i < 1000; ++i) await lookup()
  global.gc()
  const before = process.memoryUsage().heapUsed
  let peak = before
  const start = performance.now()
  for (let i = 0; i < 20000; ++i) {
    await lookup()
    if (i % 100 === 0) peak = Math.max(peak, process.memoryUsage().heapUsed)
  }
  const runtimeMs = performance.now() - start
  global.gc()
  const steady = process.memoryUsage().heapUsed
  console.log(JSON.stringify({
    mode, calls: 20000, firstMs, runtimeMs, beforeHeap: before, peakHeap: peak,
    steadyHeap: steady, maxRssKiB: process.resourceUsage().maxRSS,
  }))
} finally { await ctx.fiber.dispose(); await rm(root, { recursive: true, force: true }) }
