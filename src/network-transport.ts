import * as Queue from './queue/queue'
import * as NetworkApi from './network-api'
import * as Serialisation from './serialisation'
import * as TestTools from './tools'

const TYPE_REQUEST = 0
const TYPE_REPLY = 1

export class Transport<Request extends any[], Reply extends any[]> {
    constructor(
        private txin: Queue.Popper<Request>,
        private txout: Queue.Pusher<{ request: Request; reply: Reply }>,
        private rxout: Queue.Pusher<{ id: string; request: Request }>,
        private rxin: Queue.Popper<{ id: string; reply: Reply }>,
        private ws: NetworkApi.WebSocket,
        private highPendingRequests: number = 20,
        private lowPendingRequests: number = 15
    ) { }

    private nextMessageId = 1

    private networkQueue = new Queue.Queue<{ messageId: string; request: Request }>('network')
    private networkQueuePusher = Queue.waitPusher(this.networkQueue, this.highPendingRequests, this.lowPendingRequests)

    private rcvQueue = new Queue.Queue<Buffer>('rcv')

    private finishedTx = false

    // main loop
    async start() {
        this.ws.on('message', message => {
            this.rcvQueue.push(message)
        })

        { // rcv queue
            let rcvQueuePopper = Queue.waitPopper(this.rcvQueue);
            (async () => {
                while (true) {
                    let data = Serialisation.deserialize(await rcvQueuePopper())
                    let type = data.shift()
                    let messageId = data.shift()

                    switch (type) {
                        case TYPE_REQUEST:
                            await this.rxout({ id: messageId, request: data as Request })
                            break

                        case TYPE_REPLY:
                            let item = await this.networkQueue.popFilter(item => item.messageId == messageId)
                            await this.txout({ request: item.request, reply: data as Reply })

                            if (this.finishedTx && this.networkQueue.empty())
                                await this.txout(null)
                            break

                        default:
                            throw 'unknwown message type'
                    }
                }
            })()
        }

        { // rxin queue
            (async () => {
                while (true) {
                    let { id, reply } = await this.rxin()
                    try {
                        this.ws.send(Serialisation.serialize([TYPE_REPLY, id].concat(reply)))
                    }
                    catch (err) {
                        //console.warn(`error sending on ws, will probably close soon`)
                    }
                }
            })()
        }

        { // txin queue
            (async () => {
                while (true) {
                    let request = await this.txin()
                    if (!request)
                        break

                    let messageId = (this.nextMessageId++).toString()

                    await this.networkQueuePusher({ messageId, request })
                    try {
                        this.ws.send(Serialisation.serialize([TYPE_REQUEST, messageId].concat(request)))
                    }
                    catch (err) {
                        //console.warn(`error sending on ws, will probably close soon`)
                    }
                }

                this.finishedTx = true
            })()
        }
    }
}