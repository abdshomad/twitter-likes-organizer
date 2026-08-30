import { Context } from 'cordis'
import { spawn } from 'node:child_process'

declare module 'cordis' {
  interface Context {
    exporter: ExporterService
  }
}

export class ExporterService {
  constructor(public ctx: Context) {}

  async exportToMarkdown(exportDir: string = 'data/exports'): Promise<{ count: number; dir: string }> {
    return new Promise((resolve, reject) => {
      const script = `
from src.storage.lancedb_client import LanceDBStore
from src.exporter.markdown_exporter import export_tweets_to_directory
import json
store = LanceDBStore()
tweets = store.get_all_tweets()
files = export_tweets_to_directory(tweets, ${JSON.stringify(exportDir)})
print(json.dumps({'count': len(files), 'dir': ${JSON.stringify(exportDir)}}))
`
      const py = spawn('uv', ['run', 'python', '-c', script])
      let out = ''
      py.stdout.on('data', (d) => (out += d.toString()))
      py.on('close', (code) => {
        if (code === 0 && out.trim()) {
          try {
            resolve(JSON.parse(out.trim()))
          } catch (e) {
            reject(e)
          }
        } else {
          resolve({ count: 0, dir: exportDir })
        }
      })
    })
  }
}

export function apply(ctx: Context) {
  ctx.provide('exporter', new ExporterService(ctx))
}
