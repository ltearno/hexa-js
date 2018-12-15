import { QueueRead, QueueWrite, QueueMng, waitForSomethingAvailable } from './queue'

export class QueueToQueuePipe {
    private pauseFinisher: () => any = null
    private resumePromise: Promise<void> = null

    constructor(private s: QueueRead<any> & QueueMng, private q: QueueWrite<any> & QueueMng, high: number, low: number) {
        if (high <= low) {
            console.error(`high <= low !!!`)
            return
        }

        // queue has too much items => pause inputs
        q.addLevelListener(high, 1, async () => {
            console.log(`q2q ${this.s.name}->${this.q.name} pause inputs`)
            this.pauseFinisher = null
            this.resumePromise = new Promise(resolve => {
                this.pauseFinisher = resolve
            })
        })

        // queue has low items => resume inputs
        q.addLevelListener(low, -1, async () => {
            if (this.pauseFinisher) {
                console.log(`q2q ${this.s.name}->${this.q.name} unpause`)
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
                console.log(`q2q ${this.s.name}->${this.q.name} wait unpause`)
                await this.resumePromise
            }

            console.log(`q2q ${this.s.name}->${this.q.name} wait data`)
            await waitForSomethingAvailable(this.s)

            let data = await this.s.pop()

            console.log(`q2q ${this.s.name}->${this.q.name} tx data`)
            await this.q.push(data)

            if (this.s.isFinished()) {
                console.log(`q2q ${this.s.name}->${this.q.name} end of job !`)
                this.q.finish()
                return
            }
        }
    }
}