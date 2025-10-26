import type {
    RxCollection,
    RxDatabase,
    RxReplicationHandler
} from '../../types/index.d.ts';

import type {
    WebSocket,
    ServerOptions
} from 'isomorphic-ws';
import pkg from 'isomorphic-ws';
const { WebSocketServer } = pkg;

import type {
    WebsocketMessageResponseType,
    WebsocketMessageType,
    WebsocketServerOptions,
    WebsocketServerState
} from './websocket-types.ts';
import { rxStorageInstanceToReplicationHandler } from '../../replication-protocol/index.ts';
import {
    PROMISE_RESOLVE_VOID, getFromMapOrCreate
} from '../../plugins/utils/index.ts';
import { Subject } from 'rxjs';

export function startSocketServer(options: ServerOptions): WebsocketServerState {
    const wss = new WebSocketServer(options);
    let closed = false;
    function closeServer() {
        if (closed) {
            return PROMISE_RESOLVE_VOID;
        }
        closed = true;
        onConnection$.complete();
        return new Promise<void>((res, rej) => {
            /**
             * We have to close all client connections,
             * otherwise wss.close() will never call the callback.
             * @link https://github.com/websockets/ws/issues/1288#issuecomment-360594458
             */
            for (const ws of wss.clients) {
                ws.close();
            }
            wss.close((err: any) => {
                if (err) {
                    rej(err);
                } else {
                    res();
                }
            });
        });
    }

    const onConnection$ = new Subject<WebSocket>();
    wss.on('connection', (ws: any) => onConnection$.next(ws));

    return {
        server: wss,
        close: closeServer,
        onConnection$: onConnection$.asObservable()
    };
}

const REPLICATION_HANDLER_BY_COLLECTION: WeakMap<RxCollection, RxReplicationHandler<any, any>> = new Map();
export function getReplicationHandlerByCollection<RxDocType>(
    database: RxDatabase<any>,
    collectionName: string
): RxReplicationHandler<RxDocType, any> {
    if (!database.collections[collectionName]) {
        throw new Error('collection ' + collectionName + ' does not exist');
    }

    const collection = database.collections[collectionName];
    const handler = getFromMapOrCreate<RxCollection, RxReplicationHandler<RxDocType, any>>(
        REPLICATION_HANDLER_BY_COLLECTION,
        collection,
        () => {
            return rxStorageInstanceToReplicationHandler(
                collection.storageInstance,
                collection.conflictHandler,
                database.token
            );

        }
    );
    return handler;
}

export function startWebsocketServer(options: WebsocketServerOptions): WebsocketServerState {
    const { database, ...wsOptions } = options;
    const serverState = startSocketServer(wsOptions);

    // auto close when the database gets destroyed
    database.onDestroy.push(() => serverState.close());

    serverState.onConnection$.subscribe(ws => {
        const onCloseHandlers: Function[] = [];
        ws.onclose = () => {
            onCloseHandlers.map(fn => fn());
        };
        ws.on('message', async (messageString: string) => {
            const s = messageString.toString();
            const message: WebsocketMessageType = JSON.parse(messageString);
            const handler = getReplicationHandlerByCollection(database, message.collection);
            if (message.method === 'auth') {
                return;
            }

            // console.log(`ws.on('message'... -> ${message.method} - ${message.collection}`);
            const method = handler[message.method];

            /**
             * If it is not a function,
             * it means that the client requested the masterChangeStream$
             */
            if (typeof method !== 'function') {
                // console.log(`ws.on('message'... ->  1`);
                const changeStreamSub = handler.masterChangeStream$.subscribe(ev => {
                    // если 1-й элемент массива message.params присутствует, значит там объект
                    // filterByField, который пришел от клиента websocket-client.ts строка 206
                    const filterByField = message.params[0];

                    if (filterByField != null) {
                        // фильтруем документы, чтобы клиенту не отправлялись только его данные
                        if (Object.prototype.hasOwnProperty.call(filterByField, message.collection)) {
                            var filterFieldName = filterByField[message.collection].fieldName;
                            var filterFieldValue = filterByField[message.collection].value;
                            ev.documents = ev.documents.filter((el) =>
                                el[filterFieldName] == filterFieldValue
                            );
                        }
                        // console.log(ev.documents, message.params[0]);
                    }

                    // сообщение клиенту отправляем, только если кол-во документов больше 0
                    if (ev.documents.length > 0) {
                        const streamResponse: WebsocketMessageResponseType = {
                            id: 'stream',
                            collection: message.collection,
                            result: ev
                        };
                        // console.log(`ws.on('message'... ->  send 1 ${JSON.stringify(streamResponse)}`);
                        ws.send(JSON.stringify(streamResponse));
                    }
                });
                onCloseHandlers.push(() => changeStreamSub.unsubscribe());
                return;
            }
            else {
                // console.log(`ws.on('message'... ->  2`);
            }

            const result = await (method as any)(...message.params);
            const response: WebsocketMessageResponseType = {
                id: message.id,
                collection: message.collection,
                result
            };
            // console.log(`ws.on('message'... ->  send 2`);
            ws.send(JSON.stringify(response));
        });
    });


    return serverState;
}
