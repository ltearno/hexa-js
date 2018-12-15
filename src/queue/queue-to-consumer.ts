import { Queue, waitForSomethingAvailable } from './queue'

export class QueueToConsumerPipe {
    constructor(private q: Queue<any>, private consumer: (data: any) => Promise<void>, private finish: () => void) { }

    start() {
        this.readLoop()
    }

    private async readLoop() {
        while (true) {
            console.log(`q2c wait for ${this.q.name}`)
            await waitForSomethingAvailable(this.q)

            let data = await this.q.pop()

            //console.log(`q2c processing data on ${this.q.name} ...`)
            await this.consumer(data)
            //console.log(`LOOP processing done on ${this.q.name}.`)

            if (this.q.isFinished()) {
                console.log(`q2c end on ${this.q.name}`)
                this.finish()
                return
            }
        }
    }
}