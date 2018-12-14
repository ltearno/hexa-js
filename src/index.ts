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