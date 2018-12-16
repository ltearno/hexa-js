import { Queue, waitPusher, waitPopper, QueueRead } from './queue/queue'
import { StreamToQueuePipe } from './queue/pipe-stream-to-queue'

import * as Tools from './tools'
import * as NetworkApiImpl from './network-api-node-impl'
import * as DirectoryLister from './directory-lister'
import * as Transport from './network-transport'
import * as HashTools from './hash-tools'

enum RequestType {
    AddShaInTx = 0,
    ShaBytes = 1
}

interface FileSpec {
    name: string
    isDirectory: boolean
    lastWrite: number
    size: number
}

interface AddShaInTx {
    type: RequestType.AddShaInTx
    sha: string
    file: FileSpec
}

/*

queues :
- list dirs & files => request sha & wait return
- pour chaque wait return, si envoi nécessaire, enfiler l'info de fichier+offset
- sha buffers queue (from {file,offset} queue, have always X buffers ready to send on the wire)
- rpc calls (prioritaires ?) : ouvrir tx, attendre que les queues soient vides, et valider tx puis quitter

*/

function directPusher<T>(q: Queue<T>) {
    return async (data: T) => {
        return await q.push(data)
    }
}

type RpcQuery = AddShaInTx

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

        // add sha in tx requests, which stores the sha in the tx and returns the knwon bytes length (for sending sha bytes)

        let addShaInTx = new Queue<AddShaInTx>('add-sha-in-tx')

        {
            (async () => {
                let popper = waitPopper(fileInfos)
                let addShaInTxPusher = waitPusher(addShaInTx, 50, 8)

                while (true) {
                    if (fileInfos.isFinished())
                        break

                    let fileInfo = await popper()

                    await addShaInTxPusher({
                        type: RequestType.AddShaInTx,
                        sha: fileInfo.isDirectory ? '' : await HashTools.hashFile(fileInfo.name),
                        file: fileInfo
                    })
                }
            })()
        }

        {
            (async () => {
                // TODO choose source between addShaInTx, shaBytes, rpcCalls, etc...
                let popper = waitPopper(addShaInTx)
                let rpcTxPusher = waitPusher(rpcTxIn, 20, 10)

                while (true) {
                    let rpcRequest = await popper()

                    await rpcTxPusher(rpcRequest)
                }
            })()
        }

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