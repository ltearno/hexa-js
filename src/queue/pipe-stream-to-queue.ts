import { Readable } from 'stream'
import { QueueWrite, QueueMng } from './queue'

export class StreamToQueuePipe<T> {
    constructor(private s: Readable, private q: QueueWrite<T> & QueueMng, high: number = 10, low: number = 5) {
        // queue has too much items => pause inputs
        q.addLevelListener(high, 1, async () => {
            //console.log(`pause inputs`)
            s.pause()
        })

        // queue has low items => resume inputs
        q.addLevelListener(low, -1, async () => {
            //console.log(`resume reading`)
            s.resume()
        })
    }

    start() {
        let c = 1
        this.s.on('data', chunk => {
            console.log(`stream data rx ${c}`)
            this.q.push((chunk as any) as T)
        }).on('end', () => {
            console.log(`stream end`)
            this.q.finish()
        }).on('error', (err) => {
            console.log(`stream error ${err}`)
        })
    }
}
