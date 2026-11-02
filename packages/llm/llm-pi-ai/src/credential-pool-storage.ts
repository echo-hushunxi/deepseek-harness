/** Host-owned credential assignments and interrupted-request deadlines. */
import { z } from 'zod'
import { credentialRef } from '@deepseek-ai/dsh-credentials'
import { SessionId } from '@deepseek-ai/dsh-session'
import { defineDomain, domainTable } from '@deepseek-ai/dsh-storage-domain'

const reference = z.string().transform(credentialRef)
const sessionId = z.string().transform(SessionId)
const instant = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER)

/** Non-secret metadata; credential values remain in the credential service. */
export const credentialPoolDomain = defineDomain({
  name: 'pi_ai_credential_pool',
  version: 1,
  global: { schema: z.record(z.string(), reference), initial: {} },
  tables: {
    bindings: domainTable<SessionId, Record<string, ReturnType<typeof credentialRef>>>(z.record(z.string(), reference)),
    cooldowns: domainTable<ReturnType<typeof credentialRef>, { limitedAt: number; retryAt: number }>(z.object({
      limitedAt: instant,
      retryAt: instant,
    })),
    pending: domainTable(z.object({
      id: z.string().min(1),
      sessionId,
      rootSessionId: sessionId,
      provider: z.string().min(1),
      ref: reference,
      limitedAt: instant,
      retryAt: instant,
    })),
  },
})
