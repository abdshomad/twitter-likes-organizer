import { Context } from 'cordis'
import * as LanceDBPlugin from '@twitter-likes-organizer/plugin-lancedb'
import * as IngestionPlugin from '@twitter-likes-organizer/plugin-ingestion'
import * as MediaPlugin from '@twitter-likes-organizer/plugin-media'
import * as AIPlugin from '@twitter-likes-organizer/plugin-ai'
import * as ExporterPlugin from '@twitter-likes-organizer/plugin-exporter'
import * as DashboardPlugin from '@twitter-likes-organizer/plugin-web-dashboard'

export interface AppConfig {
  host?: string
  port?: number
  dataDir?: string
}

export class AppContext extends Context {
  constructor(config: AppConfig = {}) {
    super()
    this.provide('config', config)
  }
}

export async function createCordisApp(config: AppConfig = {}) {
  const host = config.host || process.env.HOST || '0.0.0.0'
  const port = config.port || Number(process.env.PORT) || 4024
  const dataDir = config.dataDir || process.env.DATA_DIR || 'data'

  const ctx = new AppContext({ host, port, dataDir })

  // Register all modular plugins into Cordis microkernel
  ctx.plugin(LanceDBPlugin)
  ctx.plugin(IngestionPlugin)
  ctx.plugin(MediaPlugin)
  ctx.plugin(AIPlugin)
  ctx.plugin(ExporterPlugin)
  ctx.plugin(DashboardPlugin, { host, port })

  ctx.on('ready', () => {
    console.log(`[Cordis Core] Microkernel running with 6 modular plugins on ${host}:${port}`)
  })

  return ctx
}

if (process.argv[1] && process.argv[1].endsWith('src/index.ts')) {
  createCordisApp().then((ctx) => ctx.start())
}
