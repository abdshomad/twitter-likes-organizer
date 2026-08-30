import { Context } from 'cordis'
import { spawn } from 'node:child_process'

declare module 'cordis' {
  interface Context {
    lancedb: LanceDBService
  }
}

export interface SearchOptions {
  query?: string
  tag?: string
  limit?: number
  semantic?: boolean
}

export class LanceDBService {
  constructor(public ctx: Context) {}

  async getStats(): Promise<{ total_likes: number; indexed_vectors: number; archived_media_files: number; tags_count: number }> {
    return new Promise((resolve, reject) => {
      const py = spawn('uv', [
        'run',
        'python',
        '-c',
        'from src.storage.lancedb_client import LanceDBStore; import json; print(json.dumps(LanceDBStore().get_stats()))',
      ])
      let out = ''
      py.stdout.on('data', (d) => (out += d.toString()))
      py.on('close', (code) => {
        if (code === 0) {
          try {
            resolve(JSON.parse(out.trim()))
          } catch (e) {
            reject(e)
          }
        } else {
          resolve({ total_likes: 0, indexed_vectors: 0, archived_media_files: 0, tags_count: 0 })
        }
      })
    })
  }

  async search(options: SearchOptions = {}): Promise<any[]> {
    const query = options.query || ''
    const tag = options.tag || ''
    const limit = options.limit || 50
    return new Promise((resolve) => {
      const script = `
from src.storage.lancedb_client import LanceDBStore
import json
store = LanceDBStore()
results = store.search_hybrid(query=${JSON.stringify(query)}, tag=${tag ? JSON.stringify(tag) : 'None'}, limit=${limit})
print(json.dumps(results))
`
      const py = spawn('uv', ['run', 'python', '-c', script])
      let out = ''
      py.stdout.on('data', (d) => (out += d.toString()))
      py.on('close', (code) => {
        if (code === 0 && out.trim()) {
          try {
            resolve(JSON.parse(out.trim()))
          } catch {
            resolve([])
          }
        } else {
          resolve([])
        }
      })
    })
  }
}

export function apply(ctx: Context) {
  ctx.provide('lancedb', new LanceDBService(ctx))
}
