import * as fs from 'fs'
import { Queue } from './queue/queue'
import { StreamToQueuePipe } from './queue/pipe-stream-to-queue'
import { QueueToQueuePipe } from './queue/pipe-queue-to-queue'
import { QueueToConsumerPipe } from './queue/queue-to-consumer'
import * as TestTools from './test-tools'

import * as Tools from './tools'
import * as NetworkApi from './network-api-node-impl'
import { resolve } from 'dns';

/*

queues :
- list dirs & files => request sha & wait return
- pour chaque wait return, si envoi nécessaire, enfiler l'info de fichier+offset
- sha buffers queue (from {file,offset} queue, have always X buffers ready to send on the wire)
- rpc calls (prioritaires ?) : ouvrir tx, attendre que les queues soient vides, et valider tx puis quitter


*/

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

async function run() {
    let rpcQueue = new Queue<string>('rpc')

    let app = Tools.createExpressApp(8080)
    app.ws('/queue', (ws, req) => {
        console.log(`opened ws`)

        ws.on('error', err => {
            console.log(`error on ws ${err}`)
            ws.close()
        })

        ws.on('close', () => {
            console.log(`closed ws`)
            console.log(`FINISHED RECEIVING`)
        })

        ws.on('message', async (message) => {
            console.log(`received ws message`)
            await rpcQueue.pop()
        })

        ws.send('hello')
    })

    let network = new NetworkApi.NetworkApiNodeImpl()
    let ws = network.createClientWebSocket('ws://localhost:8080/queue')
    ws.on('open', () => {
        console.log('opened ws client, go !')

        let inputStream = fs.createReadStream('../blockchain-js/blockchain-js-ui/dist/main.3c6f510d5841f58101ea.js', {
            autoClose: true,
            encoding: 'utf8'
        })

        let q1 = new Queue<string>('q1')
        let s2q1 = new StreamToQueuePipe(inputStream, q1, 10, 2)
        s2q1.start()

        let p = new QueueToConsumerPipe(q1, async data => {
            if (rpcQueue.size() > 5) {
                // wait until only 2
                await new Promise(resolve => {
                    rpcQueue.addLevelListener(2, -1, async () => {
                        resolve()
                    })
                })
            }

            rpcQueue.push('o')
            ws.send(data)
        }, () => {
            console.log(`FINISHED SENDING`)
            ws.close()
        })
        p.start()
    })
    ws.on('message', () => {
        console.log('message ws client')
    })
    ws.on('close', () => console.log('close ws client'))
    ws.on('error', () => console.log('error ws client'))
}

run()