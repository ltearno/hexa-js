export interface WebSocket {
    on(eventType: string, listener: (data: any) => any)
    send(data: string | Buffer)
    close()
}

export interface NetworkApi {
    createClientWebSocket(endpoint: string): WebSocket
}