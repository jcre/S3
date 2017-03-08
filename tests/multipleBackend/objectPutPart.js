import assert from 'assert';
import async from 'async';
import crypto from 'crypto';
import { parseString } from 'xml2js';

import { cleanup, DummyRequestLogger, makeAuthInfo } from '../unit/helpers';
import { ds } from '../../lib/data/in_memory/backend';
import bucketPut from '../../lib/api/bucketPut';
import initiateMultipartUpload from '../../lib/api/initiateMultipartUpload';
import objectPutPart from '../../lib/api/objectPutPart';
import DummyRequest from '../unit/DummyRequest';
import { metadata } from '../../lib/metadata/in_memory/metadata';
import constants from '../../constants';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';
const postBody = Buffer.from('I am a body', 'utf8');
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;
const bucketPutRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    post: '',
};
let locationConstraint;
let initiateRequest;

describe('objectPutPart API with multiple backends', () => {
    beforeEach(() => {
        cleanup();
        locationConstraint = 'file';
        initiateRequest = {
            bucketName,
            namespace,
            objectName,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            url: `/${objectName}?uploads`,
        };
    });

    it('should upload a part to file based on mpu location', done => {
        locationConstraint = 'mem';
        initiateRequest = {
            bucketName,
            namespace,
            objectName,
            headers: { 'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-scal-location-constraint': 'file' },
            url: `/$${objectName}?uploads`,
        };
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest,
                locationConstraint, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuKeys.size, 1);
                // assert(mpuKeys.keys().next().value
                //     .startsWith(`overview${splitter}${objectName}`));
                parseString(result, next);
            },
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            const calculatedHash = md5Hash.update(bufferBody).digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectName}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, err => {
                assert.strictEqual(err, null);
                const keysInMPUkeyMap = [];
                metadata.keyMaps.get(mpuBucket).forEach((val, key) => {
                    keysInMPUkeyMap.push(key);
                });
                const sortedKeyMap = keysInMPUkeyMap.sort(a => {
                    if (a.slice(0, 8) === 'overview') {
                        return -1;
                    }
                    return 0;
                });
                const partKey = sortedKeyMap[1];
                const partETag = metadata.keyMaps.get(mpuBucket)
                                                 .get(partKey)['content-md5'];
                assert.strictEqual(keysInMPUkeyMap.length, 2);
                assert.strictEqual(partETag, calculatedHash);
                assert.deepStrictEqual(ds, []);
                done();
            });
        });
    });

    it('should put a part to mem based on mpu location', done => {
        initiateRequest = {
            bucketName,
            namespace,
            objectName,
            headers: { 'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-scal-location-constraint': 'mem' },
            url: `/$${objectName}?uploads`,
        };
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest,
                locationConstraint, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuKeys.size, 1);
                parseString(result, next);
            },
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            const calculatedHash = md5Hash.update(bufferBody).digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectName}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, err => {
                assert.strictEqual(err, null);
                assert.deepStrictEqual(ds[1].value, postBody);
                done();
            });
        });
    });

    it('should upload part based on mpu location even if part ' +
        'location constraint is specified ', done => {
        initiateRequest = {
            bucketName,
            namespace,
            objectName,
            headers: { 'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-meta-scal-location-constraint': 'mem' },
            url: `/$${objectName}?uploads`,
        };
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest,
                locationConstraint, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuKeys.size, 1);
                parseString(result, next);
            },
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            const calculatedHash = md5Hash.update(bufferBody).digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectName,
                headers: { 'host': `${bucketName}.s3.amazonaws.com`,
                    'x-amz-meta-scal-location-constraint': 'file' },
                url: `/${objectName}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, err => {
                assert.strictEqual(err, null);
                assert.deepStrictEqual(ds[1].value, postBody);
                done();
            });
        });
    });

    it('should put a part to file based on bucket location', done => {
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest,
                locationConstraint, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuKeys.size, 1);
                parseString(result, next);
            },
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            const calculatedHash = md5Hash.update(bufferBody).digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectName}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, err => {
                assert.strictEqual(err, null);
                assert.deepStrictEqual(ds, []);
                done();
            });
        });
    });

    it('should put a part to mem based on bucket location', done => {
        locationConstraint = 'mem';
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest,
                locationConstraint, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuKeys.size, 1);
                parseString(result, next);
            },
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            const calculatedHash = md5Hash.update(bufferBody).digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectName}?partNumber=1&uploadId=${testUploadId}`,
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, err => {
                assert.strictEqual(err, null);
                assert.deepStrictEqual(ds[1].value, postBody);
                done();
            });
        });
    });

    it('should put a part to file based on request endpoint', done => {
        locationConstraint = null;
        async.waterfall([
            next => bucketPut(authInfo, bucketPutRequest,
                locationConstraint, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                initiateRequest, log, next),
            (result, corsHeaders, next) => {
                const mpuKeys = metadata.keyMaps.get(mpuBucket);
                assert.strictEqual(mpuKeys.size, 1);
                parseString(result, next);
            },
        ],
        (err, json) => {
            // Need to build request in here since do not have uploadId
            // until here
            const testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const md5Hash = crypto.createHash('md5');
            const bufferBody = Buffer.from(postBody);
            const calculatedHash = md5Hash.update(bufferBody).digest('hex');
            const partRequest = new DummyRequest({
                bucketName,
                namespace,
                objectName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: `/${objectName}?partNumber=1&uploadId=${testUploadId}`,
                parsedHost: 'localhost',
                query: {
                    partNumber: '1',
                    uploadId: testUploadId,
                },
                calculatedHash,
            }, postBody);
            objectPutPart(authInfo, partRequest, undefined, log, err => {
                assert.strictEqual(err, null);
                assert.deepStrictEqual(ds, []);
                done();
            });
        });
    });
});

