import { Context } from 'cordis'
import { spawn } from 'node:child_process'

declare module 'cordis' {
  interface Context {
    media: MediaService
  }
}

export class MediaService {
  constructor(public ctx: Context) {}

  async downloadMedia(tweetId: string, url: string): Promise<string[]> {
    return new Promise((resolve) => {
      const script = `
from src.media.downloader import MediaDownloader
downloader = MediaDownloader()
paths = downloader.download_tweet_media({'id': ${JSON.stringify(tweetId)}, 'url': ${JSON.stringify(url)}})
import json
print(json.dumps(paths))
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
  ctx.provide('media', new MediaService(ctx))
}
