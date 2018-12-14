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

    constructor(public name: string) { }

    async push(data: T): Promise<boolean> {
        this.queue.push({ data })

        this.displayState('push')

        if (this.listenersUp.has(this.queue.length))
            this.listenersUp.get(this.queue.length).forEach(listener => listener())

        if (this.listenersLevel.has(this.queue.length))
            this.listenersLevel.get(this.queue.length).forEach(listener => listener())

        return true
    }

    async pop(): Promise<T> {
        const result = this.queue.shift().data

        this.displayState('pop')

        if (this.listenersDown.has(this.queue.length))
            this.listenersDown.get(this.queue.length).forEach(listener => listener())

        if (this.listenersLevel.has(this.queue.length))
            this.listenersLevel.get(this.queue.length).forEach(listener => listener())

        return result
    }

    empty() {
        return !this.queue.length
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

    private displayState(op: string) {
        console.log(`queue state ${this.name} after ${op}: ${this.queue.length}`)
    }
}

function waitForSomethingAvailable(q: Queue<any>): Promise<void> {
    if (!q.empty())
        return Promise.resolve()

    return new Promise(resolve => {
        let l = q.addLevelListener(1, 1, async () => {
            l.forget()
            resolve()
        })
    })
}

class QueueToConsumerPipe {
    constructor(private q: Queue<any>, private consumer: (data: any) => Promise<void>) { }

    start() {
        this.readLoop()
    }

    private async readLoop() {
        while (true) {
            console.log(`LOOP wait for something on ${this.q.name}`)
            await waitForSomethingAvailable(this.q)

            let data = await this.q.pop()

            console.log(`LOOP processing data on ${this.q.name} ...`)
            await this.consumer(data)
            console.log(`LOOP processing done on ${this.q.name}.`)

            if (!data) {
                console.log(`LOOP end on ${this.q.name}`)
                return
            }
        }
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

class QueueToQueuePipe {
    private pauseFinisher: () => any = null
    private resumePromise: Promise<void> = null

    constructor(private s: Queue<any>, private q: Queue<any>, high: number = 10, low: number = 5) {
        // queue has too much items => pause inputs
        q.addLevelListener(high, 1, async () => {
            console.log(`Q2Q pause inputs from ${this.s.name}`)
            this.pauseFinisher = null
            this.resumePromise = new Promise(resolve => {
                this.pauseFinisher = resolve
            })
        })

        // queue has low items => resume inputs
        q.addLevelListener(low, -1, async () => {
            console.log(`Q2Q resume reading from ${this.s.name}`)
            let pauseFinisher = this.pauseFinisher
            this.pauseFinisher = null
            this.resumePromise = null
            if (pauseFinisher)
                pauseFinisher()
            else
                console.warn(`weird no finisher for pause from ${this.s.name}`)

        })
    }

    async start() {
        while (true) {
            // if paused, wait for unpause
            if (this.resumePromise)
                await this.resumePromise

            console.log(`Q2Q wait for something on ${this.s.name}`)
            await waitForSomethingAvailable(this.s)

            let data = await this.s.pop()

            console.log(`Q2Q processing data from ${this.s.name}...`)
            await this.q.push(data)

            if (!data) {
                console.log(`Q2Q end  on ${this.s.name}`)
                return
            }
        }
    }
}

async function run() {
    let inputStream = fs.createReadStream('../blockchain-js/blockchain-js-ui/dist/main.3c6f510d5841f58101ea.js', {
        autoClose: true,
        encoding: 'utf8'
    })

    let q1 = new Queue<string>('q1')
    let q2 = new Queue<string>('q2')
    let q3 = new Queue<string>('q3')

    let s2q1 = new StreamToQueuePipe(inputStream, q1, 100, 20)
    let q1q2 = new QueueToQueuePipe(q1, q2, 5, 1)
    let q2q3 = new QueueToQueuePipe(q2, q3, 5, 1)

    s2q1.start()
    q1q2.start()
    q2q3.start()

    setTimeout(() => {
        console.log(`start receiving from q3`)

        let p = new QueueToConsumerPipe(q3, async data => {
            console.log(`received data !!!`)
            await TestTools.wait(700)
        })
        p.start()

    }, 2500)
}

run()