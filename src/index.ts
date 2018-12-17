import { Queue, waitPusher, waitPopper } from './queue/queue'
import { StreamToQueuePipe, FileStreamToQueuePipe } from './queue/pipe-stream-to-queue'

import * as fs from 'fs'
import * as Tools from './tools'
import * as NetworkApiImpl from './network-api-node-impl'
import * as DirectoryLister from './directory-lister'
import * as Transport from './network-transport'
import * as HashTools from './hash-tools'
import * as FsTools from './FsTools'

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

type AddShaInTx = [RequestType.AddShaInTx, string, FileSpec] // type, sha, file
type AddShaInTxReply = [number] // length
type ShaBytes = [RequestType.ShaBytes, string, number, Buffer] // type, sha, offset, buffer

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

type RpcQuery = AddShaInTx | ShaBytes
type RpcReply = any[]

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
        let nbBytesReceived = 0
        while (true) {
            let { id, request } = await requestToProcessWaiter()

            if (request[0] == RequestType.ShaBytes) {
                nbBytesReceived += request[3].length
                console.log(`received bytes ${nbBytesReceived}`)
            }

            await rpcRxIn.push({
                id: id,
                reply: [0]
            })
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

                    await addShaInTxPusher([
                        RequestType.AddShaInTx,
                        fileInfo.isDirectory ? '' : await HashTools.hashFile(fileInfo.name),
                        fileInfo
                    ])
                }
            })()
        }

        let shasToSend = new Queue<{ sha: string; file: FileSpec; offset: number }>('shas-to-send')
        let shaBytes = new Queue<ShaBytes>('sha-bytes')

        {
            (async () => {
                let popper = waitPopper(shasToSend)
                let shaBytesPusher = waitPusher(shaBytes, 1000, 500)

                while (true) {
                    let shaToSend = await popper()

                    /*let buffers = new Queue<{ offset: number; buffer: Buffer }>('file-buffers')
                    let f2q = new FileStreamToQueuePipe(shaToSend.file.name, shaToSend.offset, buffers, 100, 80)
                    // TODO : launch the buffers reading loop which transform buffers to ShaBytes and send them
                    await f2q.start()
                    buffers.finish()*/
                    // TODO => take in account finish in waiter ?

                    let offset = shaToSend.offset
                    const bufferLength = 4096

                    let file = await FsTools.openFile(shaToSend.file.name, 'r')

                    while (offset < shaToSend.file.size) {
                        let readLength = offset + bufferLength <= shaToSend.file.size ? bufferLength : (shaToSend.file.size - offset)

                        let buffer = await FsTools.readFile(file, offset, readLength)

                        await shaBytesPusher([
                            RequestType.ShaBytes,
                            shaToSend.sha,
                            offset,
                            buffer
                        ])

                        offset += readLength
                    }

                    await FsTools.closeFile(file)

                    console.log(`finished push ${shaToSend.file.name}, still ${shasToSend.size()} to do, ${addShaInTx.size()} sha to add in tx`)
                }
            })()
        }

        {
            (async () => {
                let rpcTxPusher = waitPusher(rpcTxIn, 20, 10)

                let waitForQueue = async <T>(q: Queue<T>): Promise<void> => {
                    if (q.empty()) {
                        await new Promise(resolve => {
                            let l = q.addLevelListener(1, 1, () => {
                                l.forget()
                                resolve()
                            })
                        })
                    }
                }

                while (true) {
                    if (shaBytes.empty() && addShaInTx.empty())
                        await Promise.race([waitForQueue(shaBytes), waitForQueue(addShaInTx)])

                    let rpcRequest = null
                    if (!shaBytes.empty())
                        rpcRequest = await shaBytes.pop()
                    else
                        rpcRequest = await addShaInTx.pop()

                    await rpcTxPusher(rpcRequest)
                }
            })()
        }

        {
            (async () => {
                let popper = waitPopper(rpcTxOut)
                let shasToSendPusher = waitPusher(shasToSend, 20, 10)

                while (true) {
                    let { request, reply } = await popper()
                    if (request[0] == RequestType.AddShaInTx) {
                        let remoteLength = (reply as AddShaInTxReply).length
                        if (!request[2].isDirectory && remoteLength < request[2].size) {
                            await shasToSendPusher({ sha: request[1], file: request[2], offset: remoteLength })
                        }
                    }
                    //console.log(`received rpc reply ${JSON.stringify(request.type)} ${JSON.stringify(reply)}`)
                }
            })()
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