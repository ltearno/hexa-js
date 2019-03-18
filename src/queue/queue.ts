import * as Log from '../log'

const log = Log.buildLogger('queues')

const IS_DEBUG = false
const IS_DIAGNOSTICS = true

const weakQueuesList = new Set<Queue<any>>()
if (IS_DIAGNOSTICS) {
    setInterval(() => {
        console.log(`#### QUEUES DIAGNOSTIC`)
        console.log(`${weakQueuesList.size} queues`)
        weakQueuesList.forEach(queue => {
            console.log(`- ${queue.diagnose()}`)
        })
        console.log(`####`)
    }, 5000)
}

export interface QueueItem<T> {
    data: T
}

export interface QueueListener {
    (): void
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
export type Pusher<T> = (value: T) => Promise<boolean>

export interface QueueRead<T> {
    name?: string
    pop(): T
}

export interface QueueWrite<T> {
    name?: string
    push(value: T): boolean
    size(): number
}

export class Queue<T> implements QueueRead<T>, QueueWrite<T>, QueueMng {
    private queue: QueueItem<T>[] = []
    private listenersUp: Map<number, Set<QueueListener>> = new Map()
    private listenersDown: Map<number, Set<QueueListener>> = new Map()
    private listenersLevel: Map<number, Set<QueueListener>> = new Map()

    constructor(public name: string) {
        if (IS_DIAGNOSTICS)
            weakQueuesList.add(this)
    }

    diagnose() {
        let downSum = 0
        this.listenersDown.forEach((v, k) => { downSum += v.size })
        let upSum = 0
        this.listenersUp.forEach((v, k) => { upSum += v.size })
        let levelSum = 0
        this.listenersLevel.forEach((v, k) => { levelSum += v.size })
        return `"${this.name}" size:${this.size()} listeners: u:${upSum} d:${downSum} l:${levelSum}`
    }

    size() {
        return this.queue.length
    }

    push(data: T): boolean {
        this.queue.push({ data })

        IS_DEBUG && this.displayState('push')

        let listenersToCall = []

        if (this.listenersUp.has(this.queue.length))
            this.listenersUp.get(this.queue.length).forEach(listener => listenersToCall.push(listener))

        if (this.listenersLevel.has(this.queue.length))
            this.listenersLevel.get(this.queue.length).forEach(listener => listenersToCall.push(listener))

        for (let listener of listenersToCall) {
            listener()
        }

        return true
    }

    pop(): T {
        const result = this.queue.shift().data
        this.updateAfterPop()

        if (IS_DIAGNOSTICS && !result)
            weakQueuesList.delete(this)

        return result
    }

    private updateAfterPop() {
        IS_DEBUG && this.displayState('pop')

        let listenersToCall: QueueListener[] = []

        if (this.listenersDown.has(this.queue.length))
            this.listenersDown.get(this.queue.length).forEach(listener => listenersToCall.push(listener))

        if (this.listenersLevel.has(this.queue.length))
            this.listenersLevel.get(this.queue.length).forEach(listener => listenersToCall.push(listener))

        for (let listener of listenersToCall) {
            listener()
        }
    }

    async popFilter(filter: (item: T) => boolean): Promise<T> {
        const resultIndex = this.queue.map(queueItem => queueItem.data).findIndex(filter)
        if (resultIndex < 0)
            throw 'not found item when popFilter'

        let result = this.queue[resultIndex].data
        this.queue.splice(resultIndex, 1)

        this.updateAfterPop()
        return result
    }

    empty() {
        return !this.queue.length
    }

    addLevelListener(level: number, front: number, listener: QueueListener): ListenerSubscription {
        let list: Map<Number, Set<QueueListener>> = null
        if (front < 0)
            list = this.listenersDown
        else if (front > 0)
            list = this.listenersUp
        else
            list = this.listenersLevel

        if (!list.has(level))
            list.set(level, new Set())
        list.get(level).add(listener)

        return {
            forget: () => list.get(level).delete(listener)
        }
    }

    private displayState(op: string) {
        console.log(`queue ${this.name} ${op}: ${this.queue.length}`)
    }
}

export function waitPopper<T>(q: QueueRead<T> & QueueMng): Popper<T> {
    return async () => {
        return await waitAndPop(q)
    }
}

export function waitPusher<T>(q: QueueWrite<T> & QueueMng, high: number, low: number): Pusher<T> {
    return async (data: T) => {
        return await waitAndPush(q, data, high, low)
    }
}

export async function waitAndPop<T>(q: QueueRead<T> & QueueMng): Promise<T> {
    if (q.empty()) {
        await new Promise(resolve => {
            let l = q.addLevelListener(1, 1, () => {
                l.forget()
                resolve()
            })
        })
    }

    return q.pop()
}

// wait so that queue stays lower than high and higher than low levels
export async function waitAndPush<T>(q: QueueWrite<T> & QueueMng, data: T, high: number, low: number) {
    if (high < low)
        throw 'impossible waitandpush !'

    if (q.size() > high) {
        await new Promise(resolve => {
            let l = q.addLevelListener(low, -1, () => {
                l.forget()
                resolve()
            })
        })
    }

    return q.push(data)
}

export function directPusher<T>(q: Queue<T>): Pusher<T> {
    return async (data: T) => {
        return q.push(data)
    }
}

// extract from one queue, transform, and push to other queue. finish if null is encountered
export async function tunnelTransform<S, D>(popper: Popper<S>, addShaInTxPusher: Pusher<D>, t: (i: S) => Promise<D>) {
    while (true) {
        let item = await popper()
        if (!item)
            break

        let transformed = await t(item)

        await addShaInTxPusher(transformed)
    }
}

export async function waitForQueue(q: Queue<any>): Promise<void> {
    if (q.empty()) {
        await new Promise(resolve => {
            let l = q.addLevelListener(1, 1, () => {
                l.forget()
                resolve()
            })
        })
    }
}

export async function waitLevel(q: Queue<any>, level: number, front: number): Promise<void> {
    await new Promise(resolve => {
        let l = q.addLevelListener(level, front, () => {
            l.forget()
            resolve()
        })
    })
}

export async function manyToOneTransfert<T>(sourceQueues: { queue: Queue<T>; listener: (q: T) => void }[], rpcTxPusher: Pusher<T>) {
    while (sourceQueues.length) {
        if (sourceQueues.every(source => source.queue.empty()))
            await Promise.race(sourceQueues.map(source => waitForQueue(source.queue)))

        let rpcRequest = null
        for (let i = 0; i < sourceQueues.length; i++) {
            if (!sourceQueues[i].queue.empty()) {
                rpcRequest = sourceQueues[i].queue.pop()
                sourceQueues[i].listener && sourceQueues[i].listener(rpcRequest)
                if (rpcRequest) {
                    await rpcTxPusher(rpcRequest)
                }
                else {
                    log.dbg(`finished source ${sourceQueues[i].queue.name}`)
                    sourceQueues.splice(i, 1)
                }
                break
            }
        }
    }
}