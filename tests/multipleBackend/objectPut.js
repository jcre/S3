import assert from 'assert';

import { cleanup, DummyRequestLogger, makeAuthInfo } from '../unit/helpers';
import { ds } from '../../lib/data/in_memory/backend';
import bucketPut from '../../lib/api/bucketPut';
import objectPut from '../../lib/api/objectPut';
import DummyRequest from '../unit/DummyRequest';

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const objectName = 'objectName';
let locationConstraint = 'us-east-1';
let bucketPutRequest;
let objPutParams;

describe('objectPutAPI with multiple backends', () => {
    beforeEach(() => {
        cleanup();
        bucketPutRequest = new DummyRequest({
            bucketName,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            url: '/',
        });
        objPutParams = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
            calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
        };
    });

    it('should put an object to mem', done => {
        objPutParams = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-meta-scal-location-constraint': 'mem' },
            url: `/${bucketName}/${objectName}`,
            calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
        };

        const testPutObjectRequest = new DummyRequest(objPutParams,
            postBody);
        bucketPut(authInfo, bucketPutRequest, locationConstraint,
            log, () => {
                objectPut(authInfo, testPutObjectRequest, undefined, log,
                    (err, result) => {
                        assert.strictEqual(err, null, 'Error putting object ' +
                            `${err}`);
                        assert.strictEqual(result, correctMD5);
                        assert.deepStrictEqual(ds[1].value, postBody);
                        done();
                    });
            });
    });

    it('should put an object to file', done => {
        objPutParams = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-meta-scal-location-constraint': 'file' },
            url: `/${bucketName}/${objectName}`,
            calculatedHash: 'vnR+tLdVF79rPPfF+7YvOg==',
        };

        const testPutObjectRequest = new DummyRequest(objPutParams,
            postBody);
        bucketPut(authInfo, bucketPutRequest, locationConstraint,
            log, () => {
                objectPut(authInfo, testPutObjectRequest, undefined, log,
                    (err, result) => {
                        assert.strictEqual(err, null, 'Error putting object ' +
                            `${err}`);
                        assert.strictEqual(result, correctMD5);
                        assert.deepStrictEqual(ds, []);
                        done();
                    });
            });
    });

    it('should put an object to mem based on bucket location', done => {
        const testPutObjectRequest = new DummyRequest(objPutParams,
            postBody);
        locationConstraint = 'mem';
        bucketPut(authInfo, bucketPutRequest, locationConstraint,
            log, () => {
                objectPut(authInfo, testPutObjectRequest, undefined, log,
                    (err, result) => {
                        assert.strictEqual(err, null, 'Error putting object ' +
                            `${err}`);
                        assert.strictEqual(result, correctMD5);
                        assert.deepStrictEqual(ds[1].value, postBody);
                        done();
                    });
            });
    });

    it('should put an object to file based on bucket location', done => {
        const testPutObjectRequest = new DummyRequest(objPutParams,
            postBody);
        locationConstraint = 'file';
        bucketPut(authInfo, bucketPutRequest, locationConstraint,
            log, () => {
                objectPut(authInfo, testPutObjectRequest, undefined, log,
                    (err, result) => {
                        assert.strictEqual(err, null, 'Error putting object ' +
                            `${err}`);
                        assert.strictEqual(result, correctMD5);
                        assert.deepStrictEqual(ds, []);
                        done();
                    });
            });
    });

    it('should put an object to file based on request endpoint', done => {
        const testPutObjectRequest = new DummyRequest(objPutParams,
            postBody);
        bucketPutRequest = new DummyRequest({
            bucketName,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            url: '/',
            parsedHost: 'localhost',
        });
        locationConstraint = null;
        bucketPut(authInfo, bucketPutRequest, locationConstraint,
            log, () => {
                objectPut(authInfo, testPutObjectRequest, undefined, log,
                    (err, result) => {
                        assert.strictEqual(err, null, 'Error putting object ' +
                            `${err}`);
                        assert.strictEqual(result, correctMD5);
                        assert.deepStrictEqual(ds, []);
                        done();
                    });
            });
    });
});

