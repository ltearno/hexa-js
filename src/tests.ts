import { Queue, QueueWrite, QueueMng, waitPusher, waitPopper, directPusher, tunnelTransform, Pusher, ListenerSubscription } from './queue/queue'
import { StreamToQueuePipe } from './queue/pipe-stream-to-queue'
import { Readable } from 'stream'

import * as fs from 'fs'
import * as Tools from './express-tools'
import * as NetworkApiImpl from './network-api-node-impl'
import * as DirectoryLister from './directory-lister'
import * as Transport from './network-transport'
import * as HashTools from './hash-tools'
import * as Log from './log'

/**
 * TODO
 * 
 * merge RequestType and method name
 * and always call a method on server implementation
 * 
 * calling rpcProxy and pushing a call message are just two ways for the same
 * thing
 */

const log = Log.buildLogger('tests')

enum RequestType {
    AddShaInTx = 0,
    ShaBytes = 1,
    Call = 2
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
type RpcCall = [RequestType.Call, string, ...any[]]
type RpcQuery = AddShaInTx | ShaBytes | RpcCall
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

        let rpcServer = {
            test: async (a: number) => {
                return a * 2
            }
        }

        await tunnelTransform(
            waitPopper(rpcRxOut),
            directPusher(rpcRxIn),
            async (p: { id: string; request: RpcQuery }) => {
                let { id, request } = p

                switch (request[0]) {
                    case RequestType.AddShaInTx:
                        return {
                            id,
                            reply: [0]
                        }

                    case RequestType.ShaBytes:
                        return {
                            id,
                            reply: ['ok written']
                        }

                    case RequestType.Call:
                        request.shift()
                        let methodName = request.shift()
                        let args = request

                        let result = await rpcServer[methodName](...args)

                        return {
                            id,
                            reply: result
                        }
                }
            })

        console.log(`bye bye !`)
    })
}

async function multiInOneOutLoop(sourceQueues: { queue: Queue<RpcQuery>; listener: (q: RpcQuery) => void }[], rpcTxPusher: Pusher<RpcQuery>) {
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

    while (sourceQueues.length) {
        if (sourceQueues.every(source => source.queue.empty()))
            await Promise.race(sourceQueues.map(source => waitForQueue(source.queue)))

        let rpcRequest = null
        for (let i = 0; i < sourceQueues.length; i++) {
            if (!sourceQueues[i].queue.empty()) {
                rpcRequest = sourceQueues[i].queue.pop()
                sourceQueues[i].listener(rpcRequest)
                if (rpcRequest) {
                    await rpcTxPusher(rpcRequest)
                }
                else {
                    console.log(`finished rpc source ${sourceQueues[i].queue.name}`)
                    sourceQueues.splice(i, 1)
                }
                break
            }
        }
    }

    console.log(`finished rpcPush`)
}

function client() {
    let network = new NetworkApiImpl.NetworkApiNodeImpl()
    let ws = network.createClientWebSocket('ws://localhost:5005/hexa-backup')
    ws.on('open', async () => {
        console.log('opened ws client, go !')

        let rpcTxIn = new Queue<RpcQuery>('rpc-tx-in')
        let rpcTxOut = new Queue<{ request: RpcQuery; reply: RpcReply }>('rpc-tx-out')
        let rpcRxIn = new Queue<{ id: string; reply: RpcReply }>('rpc-rx-in')
        let rpcRxOut = new Queue<{ id: string; request: RpcQuery }>('rpc-rx-out')

        let transport = new Transport.Transport(waitPopper(rpcTxIn), directPusher(rpcTxOut), directPusher(rpcRxOut), waitPopper(rpcRxIn), ws)
        transport.start()



        let rpcCalls = new Queue<RpcCall>('rpc-calls')

        let rpcResolvers = new Map<RpcCall, (value: any) => void>()
        function callRpc(rpcCall: RpcCall): Promise<any> {
            return new Promise((resolve) => {
                rpcResolvers.set(rpcCall, resolve)
                rpcCalls.push(rpcCall)
            })
        }

        function createProxy<T>(): T {
            return <T>new Proxy({}, {
                get(target, propKey, receiver) {
                    return async (...args) => {
                        args = args.slice()
                        args.unshift(propKey)
                        args.unshift(RequestType.Call)

                        return await callRpc(args as RpcCall)
                    };
                }
            });
        }





        let fileInfos = new Queue<DirectoryLister.FileIteration>('fileslist')

        let addShaInTx = new Queue<AddShaInTx>('add-sha-in-tx')
        let closedAddShaInTx = false
        let nbAddShaInTxInTransport = 0

        let shasToSend = new Queue<{ sha: string; file: FileSpec; offset: number }>('shas-to-send')
        let shaBytes = new Queue<ShaBytes>('sha-bytes')

        {
            let directoryLister = new DirectoryLister.DirectoryLister('./', () => null);
            (async () => {
                let s2q1 = new StreamToQueuePipe(directoryLister, fileInfos, 50, 10)
                await s2q1.start()
                fileInfos.push(null)
            })()
        }

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

        {
            (async () => {
                let popper = waitPopper(shasToSend)

                while (true) {
                    let shaToSend = await popper()
                    if (!shaToSend)
                        break

                    let f2q = new FileStreamToQueuePipe(shaToSend.file.name, shaToSend.sha, shaToSend.offset, shaBytes, 200, 150)
                    await f2q.start()
                    //console.log(`transferred file  ! ${shaToSend.file.name}`)

                    console.log(`finished push ${shaToSend.file.name}`)
                }

                console.log(`finished shasToSend`)
                shaBytes.push(null)
            })()
        }


        // RPC thing

        {
            let rpcTxPusher = waitPusher(rpcTxIn, 20, 10)

            multiInOneOutLoop([
                { queue: rpcCalls, listener: (q) => 4 },
                { queue: shaBytes, listener: q => null },
                {
                    queue: addShaInTx, listener: q => {
                        if (q)
                            nbAddShaInTxInTransport++
                        else
                            closedAddShaInTx = true
                    }
                }
            ], rpcTxPusher).then(_ => rpcTxPusher(null))
        }

        {
            (async () => {
                let popper = waitPopper(rpcTxOut)
                let shasToSendPusher = waitPusher(shasToSend, 20, 10)

                while (true) {
                    let rpcItem = await popper()
                    if (!rpcItem)
                        break

                    let { request, reply } = rpcItem
                    switch (request[0]) {
                        case RequestType.AddShaInTx: {
                            let remoteLength = (reply as AddShaInTxReply).length
                            if (!request[2].isDirectory && remoteLength < request[2].size) {
                                await shasToSendPusher({ sha: request[1], file: request[2], offset: remoteLength })
                            }

                            nbAddShaInTxInTransport--
                            if (!nbAddShaInTxInTransport && closedAddShaInTx)
                                await shasToSendPusher(null)

                            break
                        }

                        case RequestType.ShaBytes:
                            break

                        case RequestType.Call:
                            rpcResolvers.get(request)(reply)
                            break
                    }
                    //console.log(`received rpc reply ${JSON.stringify(request.type)} ${JSON.stringify(reply)}`)
                }

                console.log(`finished rpcTxOut`)
            })()
        }
    })
    ws.on('close', () => console.log('close ws client'))
    ws.on('error', () => console.log('error ws client'))
}

async function run() {
    //server()
    //client()

    /*let h = await HashTools.hashString('toto')

    let p = `/home/arnaud/Téléchargements/source-code-pro-2.030R-ro-1.050R-it.zip`
    let sha = await HashTools.hashFile(p)
    console.log(`${sha}`)*/

    //let browser = new DirectoryBrowser()
}

run()



class FileStreamToQueuePipe {
    private s: Readable
    private highListener: ListenerSubscription
    private lowListener: ListenerSubscription

    constructor(path: string, private sha: string, private offset: number, private q: QueueWrite<ShaBytes> & QueueMng, high: number = 10, low: number = 5) {
        this.s = fs.createReadStream(path, { flags: 'r', autoClose: true, start: offset, encoding: null })

        let paused = false

        // queue has too much items => pause inputs
        this.highListener = q.addLevelListener(high, 1, () => {
            //console.log(`pause inputs`)
            paused = true
            this.s.pause()
        })

        // queue has low items => resume inputs
        this.lowListener = q.addLevelListener(low, -1, () => {
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
                this.highListener.forget()
                this.lowListener.forget()
                resolve(true)
            }).on('error', (err) => {
                console.log(`stream error ${err}`)
                this.highListener.forget()
                this.lowListener.forget()
                reject(err)
            })
        })
    }
}