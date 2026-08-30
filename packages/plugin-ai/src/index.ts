import { Context } from 'cordis'
import { spawn } from 'node:child_process'

declare module 'cordis' {
  interface Context {
    ai: AIService
  }
}

export class AIService {
  constructor(public ctx: Context) {}

  async generateTags(text: string): Promise<string[]> {
    return new Promise((resolve) => {
      const script = `
from src.ai.tagger import AITagger
import json
tagger = AITagger()
tags = tagger.generate_tags(${JSON.stringify(text)})
print(json.dumps(tags))
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

  async embedText(text: string): Promise<number[]> {
    return new Promise((resolve) => {
      const script = `
from src.ai.embedder import VectorEmbedder
import json
embedder = VectorEmbedder()
vec = embedder.embed_text(${JSON.stringify(text)})
print(json.dumps(vec))
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
  ctx.provide('ai', new AIService(ctx))
}
