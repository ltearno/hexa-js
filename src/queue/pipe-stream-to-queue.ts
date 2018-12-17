import * as fs from 'fs'
import { Readable } from 'stream'
import { QueueWrite, QueueMng } from './queue'

const IS_DEBUG = false

export class StreamToQueuePipe<T> {
    constructor(private s: Readable, private q: QueueWrite<T> & QueueMng, high: number = 10, low: number = 5) {
        // queue has too much items => pause inputs
        q.addLevelListener(high, 1, () => {
            //console.log(`pause inputs`)
            s.pause()
        })

        // queue has low items => resume inputs
        q.addLevelListener(low, -1, () => {
            //console.log(`resume reading`)
            s.resume()
        })
    }

    start(): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.s.on('data', chunk => {
                IS_DEBUG && console.log(`stream data rx`)
                this.q.push((chunk as any) as T)
            }).on('end', () => {
                IS_DEBUG && console.log(`stream end`)
                resolve(true)
            }).on('error', (err) => {
                IS_DEBUG && console.log(`stream error ${err}`)
                reject(err)
            })
        })
    }
}


export class FileStreamToQueuePipe {
    private s: Readable

    constructor(private path: string, private offset: number, private q: QueueWrite<{ offset: number; buffer: Buffer }> & QueueMng, high: number = 10, low: number = 5) {
        this.s = fs.createReadStream(path, { flags: 'r', autoClose: true, start: offset, encoding: null })

        // queue has too much items => pause inputs
        q.addLevelListener(high, 1, () => {
            //console.log(`pause inputs`)
            this.s.pause()
        })

        // queue has low items => resume inputs
        q.addLevelListener(low, -1, () => {
            //console.log(`resume reading`)
            this.s.resume()
        })
    }

    start(): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.s.on('data', chunk => {
                IS_DEBUG && console.log(`stream data rx`)
                let offset = this.offset
                this.offset += chunk.length
                this.q.push({
                    offset: offset,
                    buffer: chunk as Buffer
                })
            }).on('end', () => {
                IS_DEBUG && console.log(`stream end`)
                resolve(true)
            }).on('error', (err) => {
                IS_DEBUG && console.log(`stream error ${err}`)
                reject(err)
            })
        })
    }
}
