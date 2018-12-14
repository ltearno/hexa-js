import fs = require('fs')
import * as TestTools from './test-tools'

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

    let startLoop = async () => {
        while (true) {
            console.log(`LOOP wait for something`)
            await waitForSomethingAvailable()

            let data = await q.pop()
            if (!data) {
                console.log(`LOOP end`)
                return
            }

            console.log(`LOOP receive data from queue ${data}`)

            console.log(`LOOP processing data ...`)
            await TestTools.wait(1000)
            console.log(`LOOP processing done.`)
        }
    }

    let waitForSomethingAvailable = (): Promise<void> => {
        if (!q.empty())
            return Promise.resolve()

        return new Promise(resolve => {
            let l = q.addLevelListener(1, 1, async () => {
                l.forget()
                resolve()
            })
        })
    }

    startLoop()

    // queue has too much items => pause inputs
    q.addLevelListener(10, 1, async () => {
        console.log(`pause inputs`)
        inputStream.pause()
    })

    // queue has low items => resume inputs
    q.addLevelListener(5, -1, async () => {
        console.log(`resume reading`)
        inputStream.resume()
    })

    let inputStream = fs.createReadStream('../blockchain-js/blockchain-js-ui/dist/main.3c6f510d5841f58101ea.js', {
        autoClose: true,
        encoding: 'utf8'
    })

    inputStream.on('data', chunk => {
        console.log(`stream data`)
        q.push(chunk)
    }).on('end', () => {
        console.log(`stream end`)
    }).on('error', (err) => {
        console.log(`stream error ${err}`)
    })
}

run()