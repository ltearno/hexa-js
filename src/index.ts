import { Queue, QueueWrite, QueueMng, waitPusher, waitPopper, Popper, Pusher } from './queue/queue'
import { StreamToQueuePipe } from './queue/pipe-stream-to-queue'
import { Readable } from 'stream'

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

function directPusher<T>(q: Queue<T>): Pusher<T> {
    return async (data: T) => {
        return q.push(data)
    }
}

// extract from one queue, transform, and push to other queue. finish if null is encountered
async function tunnelTransform<S, D>(popper: Popper<S>, addShaInTxPusher: Pusher<D>, t: (i: S) => Promise<D>) {
    while (true) {
        let fileInfo = await popper()
        if (!fileInfo)
            break

        let transformed = await t(fileInfo)

        await addShaInTxPusher(transformed)
    }

    await addShaInTxPusher(null)
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
            rpcRxOut.push(null)
        })

        await tunnelTransform(
            waitPopper(rpcRxOut),
            directPusher(rpcRxIn),
            async (p: { id: string; request: RpcQuery }) => {
                let { id, request } = p

                if (request[0] == RequestType.ShaBytes) {
                    return {
                        id,
                        reply: ['ok written']
                    }
                }

                return {
                    id,
                    reply: [0]
                }
            })

        console.log(`bye bye !`)
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

        {
            (async () => {
                let s2q1 = new StreamToQueuePipe(directoryLister, fileInfos, 50, 10)
                await s2q1.start()
                fileInfos.push(null)
            })()
        }

        let addShaInTx = new Queue<AddShaInTx>('add-sha-in-tx')

        tunnelTransform(
            waitPopper(fileInfos),
            waitPusher(addShaInTx, 50, 8),
            async i => {
                return [
                    RequestType.AddShaInTx,
                    i.isDirectory ? '' : await HashTools.hashFile(i.name),
                    i
                ] as AddShaInTx
            }
        ).then(_ => {
            console.log(`finished directory parsing`)
            addShaInTx.push(null)
        })

        let shasToSend = new Queue<{ sha: string; file: FileSpec; offset: number }>('shas-to-send')
        let shaBytes = new Queue<ShaBytes>('sha-bytes')

        {
            (async () => {
                let popper = waitPopper(shasToSend)

                while (true) {
                    let shaToSend = await popper()

                    let f2q = new FileStreamToQueuePipe(shaToSend.file.name, shaToSend.sha, shaToSend.offset, shaBytes, 200, 150)
                    await f2q.start()
                    //console.log(`transferred file  ! ${shaToSend.file.name}`)

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

                let nbToFinish = 2
                while (true) {
                    if (shaBytes.empty() && addShaInTx.empty())
                        await Promise.race([waitForQueue(shaBytes), waitForQueue(addShaInTx)])

                    let rpcRequest = null
                    if (!shaBytes.empty()) {
                        rpcRequest = await shaBytes.pop()
                    }
                    else {
                        rpcRequest = await addShaInTx.pop()
                        if (!rpcRequest)
                            console.log(`finished addShaInTx`)
                    }

                    if (!rpcRequest) {
                        nbToFinish--
                        if (!nbToFinish)
                            break
                    }
                    else {
                        await rpcTxPusher(rpcRequest)
                    }
                }

                console.log(`finished rpcPush`)
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


class FileStreamToQueuePipe {
    private s: Readable

    constructor(path: string, private sha: string, private offset: number, private q: QueueWrite<ShaBytes> & QueueMng, high: number = 10, low: number = 5) {
        this.s = fs.createReadStream(path, { flags: 'r', autoClose: true, start: offset, encoding: null })

        let paused = false

        // queue has too much items => pause inputs
        q.addLevelListener(high, 1, () => {
            //console.log(`pause inputs`)
            paused = true
            this.s.pause()
        })

        // queue has low items => resume inputs
        q.addLevelListener(low, -1, () => {
            //console.log(`resume reading`)
            if (paused)
                this.s.resume()
        })
    }

    start(): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.s.on('data', chunk => {
                let offset = this.offset
                this.offset += chunk.length
                this.q.push([
                    RequestType.ShaBytes,
                    this.sha,
                    offset,
                    chunk as Buffer
                ])
            }).on('end', () => {
                resolve(true)
            }).on('error', (err) => {
                console.log(`stream error ${err}`)
                reject(err)
            })
        })
    }
}