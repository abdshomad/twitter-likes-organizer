import { describe, it, expect } from 'vitest'
import { createCordisApp } from './index'

describe('Cordis Microkernel & Modular Plugins', () => {
  it('should initialize microkernel and register all services', async () => {
    const ctx = await createCordisApp({ port: 4024 })

    expect(ctx.lancedb).toBeDefined()
    expect(ctx.ingestion).toBeDefined()
    expect(ctx.media).toBeDefined()
    expect(ctx.ai).toBeDefined()
    expect(ctx.exporter).toBeDefined()
    expect(ctx.dashboard).toBeDefined()
  })
})
