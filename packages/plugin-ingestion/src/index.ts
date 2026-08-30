import { Context } from 'cordis'
import { spawn } from 'node:child_process'

declare module 'cordis' {
  interface Context {
    ingestion: IngestionService
  }
}

export class IngestionService {
  constructor(public ctx: Context) {}

  async parseArchive(filePath: string): Promise<number> {
    return new Promise((resolve, reject) => {
      const script = `
from src.ingestion.archive_parser import parse_archive_file
from src.storage.lancedb_client import LanceDBStore
tweets = parse_archive_file(${JSON.stringify(filePath)})
store = LanceDBStore()
count = store.upsert_tweets(tweets)
print(count)
`
      const py = spawn('uv', ['run', 'python', '-c', script])
      let out = ''
      py.stdout.on('data', (d) => (out += d.toString()))
      py.on('close', (code) => {
        if (code === 0) {
          resolve(Number(out.trim()) || 0)
        } else {
          reject(new Error(`Failed to parse archive file: code ${code}`))
        }
      })
    })
  }

  async triggerScraper(username: string, maxTweets: number = 50): Promise<number> {
    return new Promise((resolve, reject) => {
      const script = `
import asyncio
from src.ingestion.playwright_scraper import PlaywrightXScraper
from src.storage.lancedb_client import LanceDBStore
async def main():
    scraper = PlaywrightXScraper()
    tweets = await scraper.scrape_likes(${JSON.stringify(username)}, max_tweets=${maxTweets})
    store = LanceDBStore()
    print(store.upsert_tweets(tweets))
asyncio.run(main())
`
      const py = spawn('uv', ['run', 'python', '-c', script])
      let out = ''
      py.stdout.on('data', (d) => (out += d.toString()))
      py.on('close', (code) => {
        if (code === 0) {
          resolve(Number(out.trim()) || 0)
        } else {
          reject(new Error(`Scraper run failed with code ${code}`))
        }
      })
    })
  }
}

export function apply(ctx: Context) {
  ctx.provide('ingestion', new IngestionService(ctx))
}
