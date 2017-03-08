import assert from 'assert';
import config from '../../../lib/Config';
import { BackendInfo } from '../../../lib/api/apiUtils/object/BackendInfo';
import { DummyRequestLogger } from '../helpers';

const log = new DummyRequestLogger();
const itSkipIfLegacyConfig = config.regions ? it.skip : it;
const itSkipIfNewConfig = config.locationConstraints ? it.skip : it;
const describeSkipIfLegacyConfig = config.regions ? describe.skip : describe;

const dummyBackendInfo = new BackendInfo('mem', 'file', '127.0.0.1', log);

describe('BackendInfo class', () => {
    describe('areValidBackendParameters', () => {
        it('should return false if objectLocationConstraint is invalid', () => {
            const res = BackendInfo.areValidBackendParameters(
                'notValid', 'file', '127.0.0.1', log);
            assert.equal(res, false);
        });
        it('should return false if bucketLocationConstraint is invalid', () => {
            const res = BackendInfo.areValidBackendParameters(
                'mem', 'notValid', '127.0.0.1', log);
            assert.equal(res, false);
        });
        it('should return false if requestEndpoint is invalid', () => {
            const res = BackendInfo.areValidBackendParameters(
                'mem', 'file', 'notValid', log);
            assert.equal(res, false);
        });
        itSkipIfLegacyConfig('should return true if new config and all ' +
            'backend parameters are valid', () => {
            const res = BackendInfo.areValidBackendParameters(
                'mem', 'file', '127.0.0.1', log);
            assert.equal(res, true);
        });
        itSkipIfNewConfig('should return false if legacy config and all ' +
            'backend parameters are valid', () => {
            const res = BackendInfo.areValidBackendParameters(
                'mem', 'file', '127.0.0.1', log);
            assert.equal(res, false);
        });
    });
    describeSkipIfLegacyConfig('getControllingLocationConstraint', () => {
        it('should return object location constraint', () => {
            const controllingLC =
                dummyBackendInfo.getControllingLocationConstraint();
            assert.strictEqual(controllingLC, 'mem');
        });
    });
    describeSkipIfLegacyConfig('getters', () => {
        it('should return object location constraint', () => {
            const objectLC =
                dummyBackendInfo.getObjectLocationConstraint();
            assert.strictEqual(objectLC, 'mem');
        });
        it('should return bucket location constraint', () => {
            const bucketLC =
                dummyBackendInfo.getBucketLocationConstraint();
            assert.strictEqual(bucketLC, 'file');
        });
        it('should return request endpoint', () => {
            const reqEndpoint =
                dummyBackendInfo.getRequestEndpoint();
            assert.strictEqual(reqEndpoint, '127.0.0.1');
        });
    });
});
