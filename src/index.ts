import * as fs from 'fs'
import { Queue, waitAndPush, waitPopper, QueueRead } from './queue/queue'
import { StreamToQueuePipe } from './queue/pipe-stream-to-queue'
import { QueueToConsumerPipe } from './queue/queue-to-consumer'

import * as Tools from './tools'
import * as TestTools from './test-tools'
import * as NetworkApi from './network-api'
import * as NetworkApiImpl from './network-api-node-impl'
import * as Serialisation from './serialisation'
import * as DirectoryLister from './directory-lister'

/*

queues :
- list dirs & files => request sha & wait return
- pour chaque wait return, si envoi nécessaire, enfiler l'info de fichier+offset
- sha buffers queue (from {file,offset} queue, have always X buffers ready to send on the wire)
- rpc calls (prioritaires ?) : ouvrir tx, attendre que les queues soient vides, et valider tx puis quitter

*/

async function processMessage(buffer: Buffer, dataProcessor: (data: any[]) => Promise<void>, ws: NetworkApi.WebSocket, sendRpcQueue: QueueRead<any>) {
    let list = Serialisation.deserialize(buffer)
    let messageId = list.shift()
    if (messageId == 0) {
        // it's an ack
        //console.log(`receive ack for ${messageId}`)
        await sendRpcQueue.pop()
    }
    else {
        await dataProcessor(list)
        ws.send(Serialisation.serialize([0, messageId]))
    }
}

function server() {
    let app = Tools.createExpressApp(8080)
    app.ws('/queue', async (ws, req) => {
        console.log(`opened ws`)

        let sendRpcQueue = new Queue<string>('rpc')

        ws.on('error', err => {
            console.log(`error on ws ${err}`)
            ws.close()
        })

        ws.on('close', () => {
            console.log(`closed ws`)
            rcvQ.finish()
        })

        let rcvQ = new Queue<Buffer>('rcv')
        ws.on('message', async (message) => {
            rcvQ.push(message)
        })

        let popper = waitPopper(rcvQ)

        while (true) {
            if (rcvQ.isFinished()) {
                console.log(`FINISHED RECEIVING`)
                break
            }

            let buffer = await popper()
            await processMessage(buffer, async (data: any) => {
                //console.log(`DATA RCV ${JSON.stringify(data)}`)
                console.log(`${data[0].name}`)
            }, ws, sendRpcQueue)
        }
    })
}

let nextMessageIdBase = TestTools.uuidv4().substr(0, 7)
let nextMessageId = 1

function client() {
    let network = new NetworkApiImpl.NetworkApiNodeImpl()
    let ws = network.createClientWebSocket('ws://localhost:8080/queue')
    let sendRpcQueue = new Queue<string>('rpc')
    ws.on('open', async () => {
        console.log('opened ws client, go !')

        let directoryLister = new DirectoryLister.DirectoryLister('./', () => null)

        let inputStream = fs.createReadStream('../blockchain-js/blockchain-js-ui/dist/main.3c6f510d5841f58101ea.js', {
            flags: 'r',
            encoding: null,
            start: 0,
            autoClose: true
        })

        //let q1 = new Queue<Buffer>('q1')
        let fileInfos = new Queue<DirectoryLister.FileIteration>('fileslist')
        let s2q1 = new StreamToQueuePipe(directoryLister, fileInfos, 50, 10)
        s2q1.start()

        let waitedShas = new Queue<DirectoryLister.FileIteration>('waited-shas')
        let askShaStatus = new Queue<DirectoryLister.FileIteration>('ask-sha-status');

        (async () => {
            let popper = waitPopper(fileInfos)
            while (true) {
                if (fileInfos.isFinished())
                    break

                let fileInfo = await popper()

                let send1 = waitAndPush(waitedShas, fileInfo, 50, 8)
                let send2 = waitAndPush(askShaStatus, fileInfo, 50, 8)

                await send1
                await send2
            }
        })()

        let p = new QueueToConsumerPipe(askShaStatus, async data => {
            let messageId = nextMessageIdBase + (nextMessageId++)
            await waitAndPush(sendRpcQueue, messageId, 20, 10)
            ws.send(Serialisation.serialize([messageId, data]))
        }, () => {
            console.log(`FINISHED SENDING`)
            sendRpcQueue.finish()
        })
        p.start()


        let rcvQ = new Queue<Buffer>('rcv')
        ws.on('message', async (message) => {
            rcvQ.push(message)
        })

        while (true) {
            let popper = waitPopper(rcvQ)
            if (rcvQ.isFinished()) {
                console.log(`FINISHED CLIENT RECEIVING`)
                break
            }

            waitedShas.pop()
            let buffer = await popper()
            await processMessage(buffer, async data => {
                console.log(`CLIENT DATA RCV`)
            }, ws, sendRpcQueue)
        }
    })
    ws.on('close', () => console.log('close ws client'))
    ws.on('error', () => console.log('error ws client'))
}

async function run() {
    server()
    client()
}

run()