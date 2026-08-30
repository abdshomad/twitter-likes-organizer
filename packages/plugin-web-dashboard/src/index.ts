import { Context } from 'cordis'
import http from 'node:http'

declare module 'cordis' {
  interface Context {
    dashboard: DashboardService
  }
}

export interface DashboardConfig {
  port?: number
  host?: string
}

export class DashboardService {
  public host: string
  public port: number

  constructor(public ctx: Context, config: DashboardConfig = {}) {
    this.host = config.host || '0.0.0.0'
    this.port = config.port || 4024

    ctx.on('ready', () => {
      console.log(`[Dashboard Plugin] Active on http://${this.host}:${this.port}`)
    })
  }
}

export function apply(ctx: Context, config: DashboardConfig = {}) {
  ctx.provide('dashboard', new DashboardService(ctx, config))
}
