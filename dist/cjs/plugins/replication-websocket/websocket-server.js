"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getReplicationHandlerByCollection = getReplicationHandlerByCollection;
exports.startSocketServer = startSocketServer;
exports.startWebsocketServer = startWebsocketServer;
var _isomorphicWs = _interopRequireDefault(require("isomorphic-ws"));
var _index = require("../../replication-protocol/index.js");
var _index2 = require("../../plugins/utils/index.js");
var _rxjs = require("rxjs");
var {
  WebSocketServer
} = _isomorphicWs.default;
function startSocketServer(options) {
  var wss = new WebSocketServer(options);
  var closed = false;
  function closeServer() {
    if (closed) {
      return _index2.PROMISE_RESOLVE_VOID;
    }
    closed = true;
    onConnection$.complete();
    return new Promise((res, rej) => {
      /**
       * We have to close all client connections,
       * otherwise wss.close() will never call the callback.
       * @link https://github.com/websockets/ws/issues/1288#issuecomment-360594458
       */
      for (var ws of wss.clients) {
        ws.close();
      }
      wss.close(err => {
        if (err) {
          rej(err);
        } else {
          res();
        }
      });
    });
  }
  var onConnection$ = new _rxjs.Subject();
  wss.on('connection', ws => onConnection$.next(ws));
  return {
    server: wss,
    close: closeServer,
    onConnection$: onConnection$.asObservable()
  };
}
var REPLICATION_HANDLER_BY_COLLECTION = new Map();
function getReplicationHandlerByCollection(database, collectionName) {
  if (!database.collections[collectionName]) {
    throw new Error('collection ' + collectionName + ' does not exist');
  }
  var collection = database.collections[collectionName];
  var handler = (0, _index2.getFromMapOrCreate)(REPLICATION_HANDLER_BY_COLLECTION, collection, () => {
    return (0, _index.rxStorageInstanceToReplicationHandler)(collection.storageInstance, collection.conflictHandler, database.token);
  });
  return handler;
}
function startWebsocketServer(options) {
  var {
    database,
    ...wsOptions
  } = options;
  var serverState = startSocketServer(wsOptions);

  // auto close when the database gets destroyed
  database.onDestroy.push(() => serverState.close());
  serverState.onConnection$.subscribe(ws => {
    var onCloseHandlers = [];
    ws.onclose = () => {
      onCloseHandlers.map(fn => fn());
    };
    ws.on('message', async messageString => {
      var s = messageString.toString();
      var message = JSON.parse(messageString);
      var handler = getReplicationHandlerByCollection(database, message.collection);
      if (message.method === 'auth') {
        return;
      }

      // console.log(`ws.on('message'... -> ${message.method} - ${message.collection}`);
      var method = handler[message.method];

      /**
       * If it is not a function,
       * it means that the client requested the masterChangeStream$
       */
      if (typeof method !== 'function') {
        // console.log(`ws.on('message'... ->  1`);
        var changeStreamSub = handler.masterChangeStream$.subscribe(ev => {
          // если 1-й элемент массива message.params присутствует, значит там объект
          // filterByField, который пришел от клиента websocket-client.ts строка 206
          var filterByField = message.params[0];
          if (filterByField != null) {
            // фильтруем документы, чтобы клиенту не отправлялись только его данные
            if (Object.prototype.hasOwnProperty.call(filterByField, message.collection)) {
              var filterFieldName = filterByField[message.collection].fieldName;
              var filterFieldValue = filterByField[message.collection].value;
              ev.documents = ev.documents.filter(el => el[filterFieldName] == filterFieldValue);
            }
            // console.log(ev.documents, message.params[0]);
          }

          // сообщение клиенту отправляем, только если кол-во документов больше 0
          if (ev.documents.length > 0) {
            var streamResponse = {
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
      } else {
        // console.log(`ws.on('message'... ->  2`);
      }
      var result = await method(...message.params);
      var response = {
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
//# sourceMappingURL=websocket-server.js.map