const IS_DEBUG = false

export interface QueueItem<T> {
    data: T
}

export interface QueueListener {
    (): Promise<void>
}

export interface ListenerSubscription {
    forget()
}

export interface QueueMng {
    name: string
    addLevelListener(level: number, front: number, listener: QueueListener): ListenerSubscription
    empty(): boolean
}

export type Popper<T> = () => Promise<T>

export interface QueueRead<T> {
    name?: string
    isFinished(): boolean
    pop: Popper<T>
}

export interface QueueWrite<T> {
    name?: string
    push(data: T): Promise<boolean>
    finish()
    size(): number
}

export class Queue<T> implements QueueRead<T>, QueueWrite<T>, QueueMng {
    private queue: QueueItem<T>[] = []
    private listenersUp: Map<number, QueueListener[]> = new Map()
    private listenersDown: Map<number, QueueListener[]> = new Map()
    private listenersLevel: Map<number, QueueListener[]> = new Map()

    private finished: boolean = false

    constructor(public name: string) { }

    finish() {
        if (this.empty())
            this.finished = true
        else {
            let s = this.addLevelListener(0, -1, async () => {
                s.forget()
                this.finished = true
            })
        }
    }

    isFinished() {
        return this.finished
    }

    size() {
        return this.queue.length
    }

    async push(data: T): Promise<boolean> {
        this.queue.push({ data })

        IS_DEBUG && this.displayState('push')

        let listenersToCall = []

        if (this.listenersUp.has(this.queue.length))
            this.listenersUp.get(this.queue.length).forEach(listener => listenersToCall.push(listener))

        if (this.listenersLevel.has(this.queue.length))
            this.listenersLevel.get(this.queue.length).forEach(listener => listenersToCall.push(listener))

        for (let listener of listenersToCall) {
            await listener()
        }

        return true
    }

    async pop(): Promise<T> {
        const result = this.queue.shift().data

        IS_DEBUG && this.displayState('pop')

        let listenersToCall: QueueListener[] = []

        if (this.listenersDown.has(this.queue.length))
            this.listenersDown.get(this.queue.length).forEach(listener => listenersToCall.push(listener))

        if (this.listenersLevel.has(this.queue.length))
            this.listenersLevel.get(this.queue.length).forEach(listener => listenersToCall.push(listener))

        for (let listener of listenersToCall) {
            await listener()
        }

        return result
    }

    empty() {
        return !this.queue.length
    }

    addLevelListener(level: number, front: number, listener: QueueListener): ListenerSubscription {
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
        console.log(`queue ${this.name} ${op}: ${this.queue.length}`)
    }
}

async function waitForSomethingAvailable<T>(q: QueueRead<T> & QueueMng): Promise<T> {
    if (q.empty()) {
        await new Promise(resolve => {
            let l = q.addLevelListener(1, 1, async () => {
                l.forget()
                resolve()
            })
        })
    }

    return await q.pop()
}

export function waitPopper<T>(q: QueueRead<T> & QueueMng): Popper<T> {
    return async () => {
        return await waitForSomethingAvailable(q)
    }
}

// wait so that queue stays lower than high and higher than low levels
export async function waitAndPush<T>(q: QueueWrite<T> & QueueMng, data: T, high: number, low: number) {
    if (high < low)
        throw 'impossible waitandpush !'

    if (q.size() > high) {
        await new Promise(resolve => {
            q.addLevelListener(low, -1, async () => {
                resolve()
            })
        })
    }

    await q.push(data)
}