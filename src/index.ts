/**
 * this is the main entry-point for custom builds
 * it can be used as standalone but is also used in the batteries-included main-export
 */

import './types/modules/graphql-client.d';
import './types/modules/mocha.parallel.d';
import './types/modules/modifiyjs.d';


export { addRxPlugin } from './plugin';

export {
    createRxDatabase,
    removeRxDatabase,
    isRxDatabase,
    dbCount,
    _collectionNamePrimary // used in tests
} from './rx-database';
export {
    INTERNAL_CONTEXT_COLLECTION,
    INTERNAL_CONTEXT_ENCRYPTION,
    INTERNAL_CONTEXT_REPLICATION_PRIMITIVES,
    getPrimaryKeyOfInternalDocument,
    STORAGE_TOKEN_DOCUMENT_KEY
} from './rx-database-internal-store';

export {
    overwritable
} from './overwritable';

export {
    isRxCollection,
    RxCollectionBase,
    createRxCollection // used in tests
} from './rx-collection';

export {
    fillObjectDataBeforeInsert
} from './rx-collection-helper';

export {
    isRxDocument
} from './rx-document';

export {
    flattenEvents
} from './rx-change-event';

export {
    getDocumentOrmPrototype,
    getDocumentPrototype
} from './rx-document-prototype-merge';

export {
    isInstanceOf as isRxQuery
} from './rx-query';
export * from './rx-query-helper';

export {
    isInstanceOf as isRxSchema,
    createRxSchema,
    RxSchema,
    getIndexes,
    normalizeRxJsonSchema,
    getFinalFields,
    getPreviousVersions,
    toTypedRxJsonSchema
} from './rx-schema';
export * from './rx-schema-helper';

export * from './rx-storage-helper';

export * from './custom-index';

export {
    _clearHook // used in tests
} from './hooks';

export * from './query-cache';

export * from './util';

// TODO how to do 'export type * ..' ?
export type {
    JsonSchemaTypes,
    GraphQLSyncPullOptions,
    GraphQLSyncPushOptions,
    AtomicUpdateFunction,
    CollectionsOfDatabase,
    MangoQuery,
    MangoQueryNoLimit,
    JsonSchema,
    ExtractDocumentTypeFromTypedRxJsonSchema,
    KeyFunctionMap,
    MangoQuerySelector,
    MangoQuerySortDirection,
    MangoQuerySortPart,
    MigrationState,
    NumberFunctionMap,
    DeepReadonlyObject,
    RxAttachment,
    RxAttachmentCreator,
    RxCollection,
    RxCacheReplacementPolicy,
    RxChangeEvent,
    RxChangeEventBulk,
    RxCollectionCreator,
    RxCollectionGenerated,
    RxCollectionHookCallback,
    RxCollectionHookCallbackNonAsync,
    RxCollectionHookNoInstance,
    RxCollectionHookNoInstanceCallback,
    RxDatabase,
    RxDatabaseCreator,
    RxDocument,
    RxDumpCollection,
    RxDumpCollectionAny,
    RxDumpCollectionAsAny,
    RxDumpDatabase,
    Buffer,
    Debug,
    ExtractDTcol,
    RxDatabaseGenerated,
    RxDocumentBase,

    // stuff from the RxStorage interface
    RxDocumentData,
    RxDocumentWriteData,
    WithDeleted,
    BulkWriteRow,
    RxAttachmentDataMeta,
    RxAttachmentData,
    RxAttachmentWriteData,
    RxStorage,
    RxStorageStatics,
    RxStorageBulkWriteError,
    RxStorageBulkWriteResponse,
    PreparedQuery,
    RxStorageQueryResult,
    RxStorageInstanceCreationParams,
    ChangeStreamOptions,
    ChangeStreamOnceOptions,
    EventBulk,
    ChangeStreamEvent,
    RxStorageChangedDocumentMeta,
    RxStorageChangeEvent,
    RxStorageInstance,

    RxDumpCollectionBase,
    RxDumpDatabaseAny,
    RxDumpDatabaseBase,
    RxError,
    RxErrorItem,
    RxErrorParameters,
    RxGraphQLReplicationPushQueryBuilder,
    RxGraphQLReplicationPullQueryBuilder,
    RxJsonSchema,
    RxLocalDocument,
    RxPlugin,
    RxQuery,
    RxQueryOP,
    RxQueryObject,
    RxQueryOptions,
    RxCouchDBReplicationState,
    RxTypeError,
    ServerOptions,
    SyncOptions,
    SyncOptionsGraphQL,
    MigrationStrategy,
    MigrationStrategies,
    FilledMangoQuery,
    OldRxCollection,
    WithAttachmentsData
} from './types';
