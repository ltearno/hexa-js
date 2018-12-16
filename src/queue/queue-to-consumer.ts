import { QueueRead, QueueMng, waitForSomethingAvailable } from './queue'

const IS_DEBUG = false

export class QueueToConsumerPipe<T> {
    constructor(
        private q: QueueRead<T> & QueueMng,
        private consumer: (data: T) => Promise<void>,
        private finish: () => void) {
    }

    start() {
        this.readLoop()
    }

    private async readLoop() {
        while (true) {
            IS_DEBUG && console.log(`q2c wait for ${this.q.name}`)
            let data = await waitForSomethingAvailable(this.q)

            //console.log(`q2c processing data on ${this.q.name} ...`)
            await this.consumer(data)
            //console.log(`LOOP processing done on ${this.q.name}.`)

            if (this.q.isFinished()) {
                IS_DEBUG && console.log(`q2c end on ${this.q.name}`)
                this.finish()
                return
            }
        }
    }
}