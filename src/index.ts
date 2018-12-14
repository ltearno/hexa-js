import fs = require('fs')
import * as TestTools from './test-tools'
import { Readable } from 'stream';

console.log(`hello world`)

interface QueueItem<T> {
    data: T
}

interface QueueListener {
    (): any
}

interface ListenerSubscription {
    forget()
}

class Queue<T> {
    private queue: QueueItem<T>[] = []
    private listenersUp: Map<number, QueueListener[]> = new Map()
    private listenersDown: Map<number, QueueListener[]> = new Map()
    private listenersLevel: Map<number, QueueListener[]> = new Map()

    async push(data: T): Promise<boolean> {
        this.queue.push({ data })

        if (this.listenersUp.has(this.queue.length))
            this.listenersUp.get(this.queue.length).forEach(listener => listener())

        if (this.listenersLevel.has(this.queue.length))
            this.listenersLevel.get(this.queue.length).forEach(listener => listener())

        return true
    }

    async pop(): Promise<T> {
        const result = this.queue.shift().data

        if (this.listenersDown.has(this.queue.length))
            this.listenersDown.get(this.queue.length).forEach(listener => listener())

        if (this.listenersLevel.has(this.queue.length))
            this.listenersLevel.get(this.queue.length).forEach(listener => listener())

        return result
    }

    addLevelListener(level: number, front: number, listener: () => any): ListenerSubscription {
        let list: Map<Number, QueueListener[]> = null
        if (front < 0)
            list = this.listenersDown
        else if (front > 0)
            list = this.listenersUp
        else
            list = this.listenersLevel

        if (list.has(level))
            list.get(level).push(listener)
        else
            list.set(level, [listener])

        return {
            forget: () => list.set(level, list.get(level).filter(l => l != listener))
        }
    }

    empty() {
        return !this.queue.length
    }
}

class QueueToConsumerPipe {
    constructor(private q: Queue<any>, private consumer: (data: any) => Promise<void>) { }

    start() {
        this.readLoop()
    }

    private async readLoop() {
        while (true) {
            console.log(`LOOP wait for something`)
            await this.waitForSomethingAvailable()

            let data = await this.q.pop()

            console.log(`LOOP processing data ...`)
            await this.consumer(data)
            console.log(`LOOP processing done.`)

            if (!data) {
                console.log(`LOOP end`)
                return
            }
        }
    }

    private waitForSomethingAvailable(): Promise<void> {
        if (!this.q.empty())
            return Promise.resolve()

        return new Promise(resolve => {
            let l = this.q.addLevelListener(1, 1, async () => {
                l.forget()
                resolve()
            })
        })
    }
}

class StreamToQueuePipe {
    constructor(private s: Readable, private q: Queue<any>, high: number = 10, low: number = 5) {
        // queue has too much items => pause inputs
        q.addLevelListener(high, 1, async () => {
            console.log(`pause inputs`)
            s.pause()
        })

        // queue has low items => resume inputs
        q.addLevelListener(low, -1, async () => {
            console.log(`resume reading`)
            s.resume()
        })
    }

    start() {
        this.s.on('data', chunk => {
            console.log(`stream data`)
            this.q.push(chunk)
        }).on('end', () => {
            console.log(`stream end`)
        }).on('error', (err) => {
            console.log(`stream error ${err}`)
        })
    }
}

async function run() {
    let inputStream = fs.createReadStream('../blockchain-js/blockchain-js-ui/dist/main.3c6f510d5841f58101ea.js', {
        autoClose: true,
        encoding: 'utf8'
    })

    let q = new Queue<string>()

    let s2q = new StreamToQueuePipe(inputStream, q)

    s2q.start()

    setTimeout(() => {
        console.log(`start receiving !`)

        let p = new QueueToConsumerPipe(q, async data => {
            console.log(`received data !!!`)
            await TestTools.wait(700)
        })
        p.start()

    }, 5000)
}

run()