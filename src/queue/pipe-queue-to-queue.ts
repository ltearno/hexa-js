import { QueueRead, QueueWrite, QueueMng, waitForSomethingAvailable } from './queue'

const IS_DEBUG = false

export class QueueToQueuePipe<T> {
    private pauseFinisher: () => any = null
    private resumePromise: Promise<void> = null

    constructor(private s: QueueRead<T> & QueueMng, private q: QueueWrite<T> & QueueMng, high: number, low: number) {
        if (high <= low) {
            console.error(`high <= low !!!`)
            return
        }

        // queue has too much items => pause inputs
        q.addLevelListener(high, 1, async () => {
            IS_DEBUG && console.log(`q2q ${this.s.name}->${this.q.name} pause inputs`)
            this.pauseFinisher = null
            this.resumePromise = new Promise(resolve => {
                this.pauseFinisher = resolve
            })
        })

        // queue has low items => resume inputs
        q.addLevelListener(low, -1, async () => {
            if (this.pauseFinisher) {
                IS_DEBUG && console.log(`q2q ${this.s.name}->${this.q.name} unpause`)
                let pauseFinisher = this.pauseFinisher
                this.pauseFinisher = null
                this.resumePromise = null
                if (pauseFinisher)
                    pauseFinisher()
            }

        })
    }

    async start() {
        while (true) {
            // if paused, wait for unpause
            if (this.resumePromise) {
                IS_DEBUG && console.log(`q2q ${this.s.name}->${this.q.name} wait unpause`)
                await this.resumePromise
            }

            IS_DEBUG && console.log(`q2q ${this.s.name}->${this.q.name} wait data`)
            let data: any = await waitForSomethingAvailable(this.s)

            IS_DEBUG && console.log(`q2q ${this.s.name}->${this.q.name} tx data`)
            await this.q.push(data)

            if (this.s.isFinished()) {
                IS_DEBUG && console.log(`q2q ${this.s.name}->${this.q.name} end of job !`)
                this.q.finish()
                return
            }
        }
    }
}