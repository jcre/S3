import assert from 'assert';
import config from '../../../lib/Config';
import parseLC from '../../../lib/data/locationConstraintParser';
import inMemory from '../../../lib/data/in_memory/backend';
import file from '../../../lib/data/file/backend';


const clients = parseLC();

const describeSkipIfLegacyConfig = config.regions ? describe.skip : describe;
const describeSkipIfNewConfig = config.locationConstraints ?
    describe.skip : describe;

describe('locationConstraintParser', () => {
    describeSkipIfNewConfig('if legacy config', () => {
        it('should return empty object', () => {
            assert.deepEqual(clients, {});
        });
    });
    describeSkipIfLegacyConfig('if new config', () => {
        it('should return object containing mem object', () => {
            assert.notEqual(Object.keys(clients).indexOf('mem'), -1);
            assert.strictEqual(typeof clients.mem, 'object');
            assert.deepEqual(clients.mem, inMemory);
        });
        it('should return object containing file object', () => {
            assert.notEqual(Object.keys(clients).indexOf('file'), -1);
            assert.strictEqual(typeof clients.file, 'object');
            assert.deepEqual(clients.file, file);
        });
    });
});
