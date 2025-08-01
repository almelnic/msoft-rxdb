import type {
    Observable,
} from 'rxjs';
import type {
    ServerOptions,
    ClientOptions,
    WebSocketServer,
    WebSocket
} from 'ws';
import type {
    RxCollection,
    RxDatabase,
    RxReplicationHandler,
    StringKeys
} from '../../types/index.d.ts';

export type WebsocketServerOptions = {
    database: RxDatabase<any, any, any>;
} & ServerOptions;

export type WebsocketServerState = {
    server: WebSocketServer;
    close: () => Promise<void>;
    onConnection$: Observable<WebSocket>;
};

export type WebsocketClientOptions<RxDocType> = {
    replicationIdentifier: string;
    collection: RxCollection<RxDocType>;
    url: string;
    batchSize?: number;
    live?: boolean;
    headers?: { [k: string]: string; };
    filters?: [any];
    ignorePushInitialSync?: boolean;    
} & ClientOptions;

export type WebsocketMessageType = {
    [x: string]: {};
    id: string;
    collection: string;
    method: StringKeys<RxReplicationHandler<any, any>> | 'auth';
    params: any[];
};

export type WebsocketMessageResponseType = {
    id: string;
    collection: string;
    result: any;
};
