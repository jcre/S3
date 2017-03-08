import assert from 'assert';
import async from 'async';
import { parseString } from 'xml2js';

import { cleanup, DummyRequestLogger, makeAuthInfo } from '../unit/helpers';
import { ds } from '../../lib/data/in_memory/backend';
import bucketPut from '../../lib/api/bucketPut';
import initiateMultipartUpload from '../../lib/api/initiateMultipartUpload';
import objectPut from '../../lib/api/objectPut';
import objectPutCopyPart from '../../lib/api/objectPutCopyPart';
import DummyRequest from '../unit/DummyRequest';
import { metadata } from '../../lib/metadata/in_memory/metadata';
import constants from '../../constants';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';

const bucketName = 'superbucket9999999';
const sourceObjName = 'supersourceobject';
const destObjName = 'copycatobject';
const fileLocationConstraint = 'file';
const memLocationConstraint = 'mem';
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;

const postBody = Buffer.from('I am a body', 'utf8');

let bucketPutReq;
let sourceObjPutReq;
let initiateReq;

describe('Object Part Copy with multiple backends', () => {
    beforeEach(() => {
        cleanup();
        bucketPutReq = new DummyRequest({
            bucketName,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            url: '/',
        });
        sourceObjPutReq = new DummyRequest({
            bucketName,
            namespace,
            objectKey: sourceObjName,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            url: '/',
        }, postBody);
        initiateReq = {
            bucketName,
            namespace,
            objectKey: destObjName,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            url: `/${destObjName}?uploads`,
        };
    });

    it('should copy part to mem based on mpu location', done => {
        initiateReq = {
            bucketName,
            namespace,
            objectKey: destObjName,
            headers: { 'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-scal-location-constraint': 'mem' },
            url: `/${destObjName}?uploads`,
        };
        async.waterfall([
            next => {
                bucketPut(authInfo, bucketPutReq,
                fileLocationConstraint, log, err => {
                    assert.ifError(err, 'Error putting bucket');
                    next(err);
                });
            },
            next => {
                objectPut(authInfo, sourceObjPutReq, undefined, log, err => {
                    assert.ifError(err, 'Error putting source object');
                    next(err);
                });
            },
            next => {
                initiateMultipartUpload(authInfo, initiateReq, log, next);
            },
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuKeys.size, 1);
                parseString(result, next);
            },
        ],
        (err, json) => {
            // Need to build request in here since do not have
            // uploadId until here
            const testUploadId = json.InitiateMultipartUploadResult.
                UploadId[0];
            const copyPartRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey: destObjName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${destObjName}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
            });
            objectPutCopyPart(authInfo, copyPartRequest,
                bucketName, sourceObjName, log, err => {
                    assert.strictEqual(err, null);
                    assert.strictEqual(ds.length, 2);
                    assert.deepStrictEqual(ds[1].value, postBody);
                    done();
                });
        });
    });

    it('should copy part to file based on mpu location', done => {
        initiateReq = {
            bucketName,
            namespace,
            objectKey: destObjName,
            headers: { 'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-scal-location-constraint': 'file' },
            url: `/${destObjName}?uploads`,
        };
        async.waterfall([
            next => {
                bucketPut(authInfo, bucketPutReq,
                memLocationConstraint, log, err => {
                    assert.ifError(err, 'Error putting bucket');
                    next(err);
                });
            },
            next => {
                objectPut(authInfo, sourceObjPutReq, undefined, log, err => {
                    assert.ifError(err, 'Error putting source object');
                    next(err);
                });
            },
            next => {
                initiateMultipartUpload(authInfo, initiateReq, log, next);
            },
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuKeys.size, 1);
                parseString(result, next);
            },
        ],
        (err, json) => {
            const testUploadId = json.InitiateMultipartUploadResult.
                UploadId[0];
            const copyPartRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey: destObjName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${destObjName}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
            });
            objectPutCopyPart(authInfo, copyPartRequest,
                bucketName, sourceObjName, log, err => {
                    assert.strictEqual(err, null);
                    assert.strictEqual(ds.length, 2);
                    done();
                });
        });
    });

    it('should copy part to mem based on bucket location', done => {
        async.waterfall([
            next => {
                bucketPut(authInfo, bucketPutReq,
                memLocationConstraint, log, err => {
                    assert.ifError(err, 'Error putting bucket');
                    next(err);
                });
            },
            next => {
                objectPut(authInfo, sourceObjPutReq, undefined, log, err => {
                    assert.ifError(err, 'Error putting source object');
                    next(err);
                });
            },
            next => {
                initiateMultipartUpload(authInfo, initiateReq, log, next);
            },
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuKeys.size, 1);
                parseString(result, next);
            },
        ],
        (err, json) => {
            const testUploadId = json.InitiateMultipartUploadResult.
                UploadId[0];
            const copyPartRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey: destObjName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${destObjName}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
            });
            objectPutCopyPart(authInfo, copyPartRequest,
                bucketName, sourceObjName, log, err => {
                    assert.strictEqual(err, null);
                    assert.strictEqual(ds.length, 3);
                    assert.deepStrictEqual(ds[2].value, postBody);
                    done();
                });
        });
    });

    it('should copy part to file based on bucket location', done => {
        async.waterfall([
            next => {
                bucketPut(authInfo, bucketPutReq,
                fileLocationConstraint, log, err => {
                    assert.ifError(err, 'Error putting bucket');
                    next(err);
                });
            },
            next => {
                objectPut(authInfo, sourceObjPutReq, undefined, log, err => {
                    assert.ifError(err, 'Error putting source object');
                    next(err);
                });
            },
            next => {
                initiateMultipartUpload(authInfo, initiateReq, log, next);
            },
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuKeys.size, 1);
                parseString(result, next);
            },
        ],
        (err, json) => {
            const testUploadId = json.InitiateMultipartUploadResult.
                UploadId[0];
            const copyPartRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey: destObjName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${destObjName}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
            });
            objectPutCopyPart(authInfo, copyPartRequest,
                bucketName, sourceObjName, log, err => {
                    assert.strictEqual(err, null);
                    assert.deepStrictEqual(ds, []);
                    done();
                });
        });
    });

    it('should copy part to file based on request endpoint', done => {
        sourceObjPutReq = new DummyRequest({
            bucketName,
            namespace,
            objectKey: sourceObjName,
            headers: { 'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-scal-location-constraint': 'mem' },
            url: '/',
        }, postBody);

        async.waterfall([
            next => {
                bucketPut(authInfo, bucketPutReq,
                null, log, err => {
                    assert.ifError(err, 'Error putting bucket');
                    next(err);
                });
            },
            next => {
                objectPut(authInfo, sourceObjPutReq, undefined, log, err => {
                    assert.ifError(err, 'Error putting source object');
                    next(err);
                });
            },
            next => {
                initiateMultipartUpload(authInfo, initiateReq, log, next);
            },
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuKeys.size, 1);
                parseString(result, next);
            },
        ],
        (err, json) => {
            const testUploadId = json.InitiateMultipartUploadResult.
                UploadId[0];
            const copyPartRequest = new DummyRequest({
                bucketName,
                namespace,
                objectKey: destObjName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                parsedHost: 'localhost',
                url: `/${destObjName}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
            });
            objectPutCopyPart(authInfo, copyPartRequest,
                bucketName, sourceObjName, log, err => {
                    assert.strictEqual(err, null);
                    assert.strictEqual(ds.length, 2);
                    done();
                });
        });
    });
});
