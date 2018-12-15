import * as fs from 'fs'
import { Queue } from './queue/queue'
import { StreamToQueuePipe } from './queue/pipe-stream-to-queue'
import { QueueToQueuePipe } from './queue/pipe-queue-to-queue'
import { QueueToConsumerPipe } from './queue/queue-to-consumer'
import * as TestTools from './test-tools'

async function oldrun() {
    let inputStream = fs.createReadStream('../blockchain-js/blockchain-js-ui/dist/main.3c6f510d5841f58101ea.js', {
        autoClose: true,
        encoding: 'utf8'
    })

    let q1 = new Queue<string>('q1')
    let q2 = new Queue<string>('q2')
    let q3 = new Queue<string>('q3')

    let s2q1 = new StreamToQueuePipe(inputStream, q1, 10, 2)
    let q1q2 = new QueueToQueuePipe(q1, q2, 5, 3)
    let q2q3 = new QueueToQueuePipe(q2, q3, 5, 1)

    s2q1.start()
    q1q2.start()
    q2q3.start()

    setTimeout(() => {
        console.log(`start receiving from q3`)

        let p = new QueueToConsumerPipe(q3, async data => {
            console.log(`received data `)
            await TestTools.wait(70)
        }, () => {
            console.log(`FINISHED RECEIVED`)
        })
        p.start()

    }, 2500)
}