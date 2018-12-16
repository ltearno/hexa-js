import * as Queue from './queue/queue'
import * as NetworkApi from './network-api'
import * as Serialisation from './serialisation'
import * as TestTools from './test-tools'

const TYPE_REQUEST = 0
const TYPE_REPLY = 1

export class Transport {
    constructor(
        private txin: Queue.Popper<any>,
        private txout: Queue.Pusher<any>,
        private rxout: Queue.Pusher<{ id: string; request: any }>,
        private rxin: Queue.Popper<{ id: string; reply: any }>,
        private ws: NetworkApi.WebSocket
    ) { }

    private nextMessageBase = TestTools.uuidv4().substr(0, 3) + '#'
    private nextMessageId = 1

    private networkQueue = new Queue.Queue<{ messageId: string; request: any }>('network')
    private networkQueuePusher = Queue.waitPusher(this.networkQueue, 20, 10)

    private rcvQueue = new Queue.Queue<Buffer>('rcv')

    // main loop
    async start() {
        this.ws.on('message', message => {
            this.rcvQueue.push(message)
        })
        
        { // rcv queue
            let rcvQueuePopper = Queue.waitPopper(this.rcvQueue);
            (async () => {
                while (true) {
                    let [type, messageId, data] = Serialisation.deserialize(await rcvQueuePopper())

                    switch (type) {
                        case TYPE_REQUEST:
                            await this.rxout({ id: messageId, request: data })
                            break

                        case TYPE_REPLY:
                            let item = await this.networkQueue.popFilter(item => item.messageId == messageId)
                            await this.txout({ request: item.request, reply: data })
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
                    this.ws.send(Serialisation.serialize([TYPE_REPLY, id, reply]))
                }
            })()
        }

        { // txin queue
            (async () => {
                while (true) {
                    let request = await this.txin()
                    let messageId = this.nextMessageBase + (this.nextMessageId++)

                    await this.networkQueuePusher({ messageId, request })
                    this.ws.send(Serialisation.serialize([TYPE_REQUEST, messageId, request]))
                }
            })()
        }
    }
}