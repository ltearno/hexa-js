import fs = require('fs')
import * as TestTools from './test-tools'

console.log(`hello world`)

interface QueueItem<T> {
    data: T
}

interface QueueListener {
    (): any
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

    addLevelListener(level: number, front: number, listener: () => any) {
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
    }
}

class Pipe {

}

async function run() {
    let q = new Queue<string>()

    /*q.addLevelListener(3, 1, () => console.log(`level 3 up reached`))
    q.addLevelListener(4, 1, () => console.log(`level 4 up reached`))
    q.addLevelListener(4, -1, () => console.log(`level 4 down reached`))
    q.addLevelListener(2, 0, () => console.log(`level 2 reached`))

    q.push("titi1")
    q.push("titi2")
    q.push("titi3")
    q.push("titi4")
    q.push("titi5")
    q.push("titi6")
    q.pop()
    q.pop()
    q.pop()
    q.pop()
    q.pop()
    q.pop()*/

    let loopOn = false

    let startLoop = async () => {
        if (loopOn) {
            console.error(`STARTED ALREADY`)
            return
        }

        loopOn = true
        while (loopOn) {
            let data = await q.pop()
            console.log(`receive data from queue`)

            console.log(`processing data ...`)
            await TestTools.wait(1000)
            console.log(`processing done.`)
        }
    }

    // queue begins to receive items => start read loop
    q.addLevelListener(1, 1, async () => {
        console.log(`start reading`)

        startLoop()
    })

    // queue has no more items => end read loop
    q.addLevelListener(0, -1, async () => {
        console.log(`end reading, no more items`)
        loopOn = false
    })

    // queue has too much items => pause inputs
    q.addLevelListener(10, 1, async () => {
        console.log(`pause reading`)
        loopOn = false
    })

    // queue has low items => resume inputs
    q.addLevelListener(5, -1, async () => {
        console.log(`resume reading`)
        loopOn = false
    })

    let inputStream = fs.createReadStream('yarn.lock', {
        autoClose: true,
        encoding: 'utf8'
    })

    inputStream.on('data', chunk => {
        console.log(`data`)
        q.push(chunk)
    }).on('end', () => {
        console.log(`end`)
    }).on('error', (err) => {
        console.log(`error ${err}`)
    })
    //inputStream.pause()
    //inputStream.resume()
}

run()