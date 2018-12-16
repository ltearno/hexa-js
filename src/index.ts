import * as fs from 'fs'
import { Queue, waitPusher, waitPopper, QueueRead } from './queue/queue'
import { StreamToQueuePipe } from './queue/pipe-stream-to-queue'
import { QueueToConsumerPipe } from './queue/queue-to-consumer'

import * as Tools from './tools'
import * as TestTools from './test-tools'
import * as NetworkApi from './network-api'
import * as NetworkApiImpl from './network-api-node-impl'
import * as Serialisation from './serialisation'
import * as DirectoryLister from './directory-lister'
import * as Transport from './network-transport'
import { QueueToQueuePipe } from './queue/pipe-queue-to-queue';

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

function directPusher<T>(q: Queue<T>) {
    return async (data: T) => {
        return await q.push(data)
    }
}

interface RpcQuery {
}

interface RpcReply {
}

function server() {
    let app = Tools.createExpressApp(8080)
    app.ws('/queue', async (ws, req) => {
        console.log(`opened ws`)

        let rpcTxIn = new Queue<RpcQuery>('rpc-tx-in')
        let rpcTxOut = new Queue<{ request: RpcQuery; reply: RpcReply }>('rpc-tx-out')
        let rpcRxIn = new Queue<{ id: string; reply: RpcReply }>('rpc-rx-in')
        let rpcRxOut = new Queue<{ id: string; request: RpcQuery }>('rpc-rx-out')

        let transport = new Transport.Transport(waitPopper(rpcTxIn), directPusher(rpcTxOut), directPusher(rpcRxOut), waitPopper(rpcRxIn), ws)
        transport.start()

        ws.on('error', err => {
            console.log(`error on ws ${err}`)
            ws.close()
        })

        ws.on('close', () => {
            console.log(`closed ws`)
            // todo close transport
        })

        let requestToProcessWaiter = waitPopper(rpcRxOut)
        while (true) {
            let { id, request } = await requestToProcessWaiter()

            console.log(`process RPC request...`)
            await rpcRxIn.push({
                id: id,
                reply: { test: 0 }
            })
            console.log(`processed RPC request`)
        }
    })
}

function client() {
    let network = new NetworkApiImpl.NetworkApiNodeImpl()
    let ws = network.createClientWebSocket('ws://localhost:8080/queue')
    let sendRpcQueue = new Queue<string>('rpc')
    ws.on('open', async () => {
        console.log('opened ws client, go !')

        let rpcTxIn = new Queue<RpcQuery>('rpc-tx-in')
        let rpcTxOut = new Queue<{ request: RpcQuery; reply: RpcReply }>('rpc-tx-out')
        let rpcRxIn = new Queue<{ id: string; reply: RpcReply }>('rpc-rx-in')
        let rpcRxOut = new Queue<{ id: string; request: RpcQuery }>('rpc-rx-out')

        let transport = new Transport.Transport(waitPopper(rpcTxIn), directPusher(rpcTxOut), directPusher(rpcRxOut), waitPopper(rpcRxIn), ws)
        transport.start()






        let directoryLister = new DirectoryLister.DirectoryLister('./', () => null)

        /*let inputStream = fs.createReadStream('../blockchain-js/blockchain-js-ui/dist/main.3c6f510d5841f58101ea.js', {
            flags: 'r',
            encoding: null,
            start: 0,
            autoClose: true
        })*/

        let fileInfos = new Queue<DirectoryLister.FileIteration>('fileslist')
        let s2q1 = new StreamToQueuePipe(directoryLister, fileInfos, 50, 10)
        s2q1.start()

        let askShaStatus = new Queue<DirectoryLister.FileIteration>('ask-sha-status');

        (async () => {
            let popper = waitPopper(fileInfos)
            let askShaStatusPusher = waitPusher(askShaStatus, 50, 8)

            while (true) {
                if (fileInfos.isFinished())
                    break

                let fileInfo = await popper()

                await askShaStatusPusher(fileInfo)
            }
        })()

        let askShaStatus2Rpc = new QueueToQueuePipe(askShaStatus, rpcTxIn, 20, 10)
        askShaStatus2Rpc.start()

        let popper = waitPopper(rpcTxOut)
        while (true) {
            let { request, reply } = await popper()
            console.log(`received rpc reply ${JSON.stringify(reply)} ${JSON.stringify(request)}`)
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