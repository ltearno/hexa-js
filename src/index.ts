import * as fs from 'fs'
import { Queue, waitForSomethingAvailable } from './queue/queue'
import { StreamToQueuePipe } from './queue/pipe-stream-to-queue'
import { QueueToConsumerPipe } from './queue/queue-to-consumer'

import * as Tools from './tools'
import * as NetworkApi from './network-api-node-impl'
import * as Serialisation from './serialisation'

/*

queues :
- list dirs & files => request sha & wait return
- pour chaque wait return, si envoi nécessaire, enfiler l'info de fichier+offset
- sha buffers queue (from {file,offset} queue, have always X buffers ready to send on the wire)
- rpc calls (prioritaires ?) : ouvrir tx, attendre que les queues soient vides, et valider tx puis quitter


*/

async function run() {
    let app = Tools.createExpressApp(8080)
    app.ws('/queue', async (ws, req) => {
        console.log(`opened ws`)

        ws.on('error', err => {
            console.log(`error on ws ${err}`)
            ws.close()
        })

        ws.on('close', () => {
            console.log(`closed ws`)
        })

        let rcvQ = new Queue<Buffer>('rcv')

        ws.on('message', async (message) => {
            rcvQ.push(message)
        })

        while (true) {
            if (rcvQ.isFinished()) {
                console.log(`FINISHED RECEIVING`)
                break
            }
            await waitForSomethingAvailable(rcvQ)
            let value = await rcvQ.pop()
            console.log(`proc begin ${JSON.stringify(Serialisation.deserialize(value))}`)
            //await TestTools.wait(200)
            console.log(`proc end`)
            ws.send('lk')
        }
    })

    let network = new NetworkApi.NetworkApiNodeImpl()
    let ws = network.createClientWebSocket('ws://localhost:8080/queue')
    let sendRpcQueue = new Queue<string>('rpc')
    ws.on('open', () => {
        console.log('opened ws client, go !')

        let inputStream = fs.createReadStream('../blockchain-js/blockchain-js-ui/dist/main.3c6f510d5841f58101ea.js', {
            flags: 'r',
            encoding: null,
            start: 0,
            autoClose: true
        })

        let q1 = new Queue<Buffer>('q1')
        let s2q1 = new StreamToQueuePipe(inputStream, q1, 5, 1)
        s2q1.start()

        let p = new QueueToConsumerPipe(q1, async data => {
            if (sendRpcQueue.size() > 5) {
                // wait until only 2
                await new Promise(resolve => {
                    sendRpcQueue.addLevelListener(2, -1, async () => {
                        resolve()
                    })
                })
            }

            sendRpcQueue.push('o')
            ws.send(Serialisation.serialize([data]))
        }, () => {
            console.log(`FINISHED SENDING`)
            sendRpcQueue.finish()
        })
        p.start()
    })
    ws.on('message', async () => {
        //console.log('message ws client')
        await sendRpcQueue.pop()
    })
    ws.on('close', () => console.log('close ws client'))
    ws.on('error', () => console.log('error ws client'))
}

run()