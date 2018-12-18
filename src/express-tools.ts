import * as express from 'express'
import * as bodyParser from 'body-parser'

declare function ws(this: express.Server, url: string, callback: any)

declare module "express" {
    interface Server {
        ws(url: string, callback: any)
    }
}

export function createExpressApp(port: number): express.Server {
    let app = express()

    require('express-ws')(app)
    app.use(bodyParser.json())

    app.listen(port, () => console.log(`listening http on port ${port}`))

    return app
}