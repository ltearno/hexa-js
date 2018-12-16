import { QueueRead, QueueWrite, QueueMng, waitForSomethingAvailable } from './queue'

const IS_DEBUG = false

export class QueueToQueuePipe<Source, Destination> {
    private pauseFinisher: () => any = null
    private resumePromise: Promise<void> = null

    constructor(private s: QueueRead<Source> & QueueMng, private q: QueueWrite<Destination> & QueueMng, high: number, low: number) {
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

    transform: (dataIn: Source) => Promise<Destination> = null

    async start() {
        while (true) {
            // if paused, wait for unpause
            if (this.resumePromise) {
                IS_DEBUG && console.log(`q2q ${this.s.name}->${this.q.name} wait unpause`)
                await this.resumePromise
            }

            IS_DEBUG && console.log(`q2q ${this.s.name}->${this.q.name} wait data`)
            await waitForSomethingAvailable(this.s)

            // sometimes types are transformed, sometimes not
            // TODO express that with the TS type system
            let data: any = await this.s.pop()

            if (this.transform) {
                IS_DEBUG && console.log(`q2q ${this.s.name}->${this.q.name} transform`)
                data = await this.transform(data)
            }

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