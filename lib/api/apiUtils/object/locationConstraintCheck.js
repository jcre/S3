import { errors } from 'arsenal';

import config from '../../../Config';
import { BackendInfo } from './BackendInfo';
import constants from '../../../../constants';


/**
 * locationConstraintCheck - if new config, on object put, object copy,
 * or initiate MPU request, gathers object location constraint,
 * bucket locationconstraint, and request endpoint  and checks their validity
 * @param {request} request - normalized request object
 * @param {object} metaHeaders - headers of metadata storage params used in
 * objectCopy api
 * @param {BucketInfo} bucket - metadata BucketInfo instance
 * @param {object} log - Werelogs instance
 * @return {object} - consists of three keys: error, controllingLC, and
 * backendInfo. backendInfo only has value if new config
 */
export default function locationConstraintCheck(request, metaHeaders,
    bucket, log) {
    let backendInfoObj = {};

    if (config.regions) {
        backendInfoObj = {
            err: null,
            controllingLC: config.backends.data,
        };
        return backendInfoObj;
    }

    let objectLocationConstraint;
    if (metaHeaders) {
        objectLocationConstraint =
        metaHeaders[constants.objectLocationConstraintHeader];
    } else {
        objectLocationConstraint = request
        .headers[constants.objectLocationConstraintHeader];
    }
    const bucketLocationConstraint = bucket.getLocationConstraint();
    const requestEndpoint = request.parsedHost;

    if (!BackendInfo.isValidControllingBackendParam(
        objectLocationConstraint, bucketLocationConstraint,
        requestEndpoint, log)) {
        backendInfoObj = {
            err: errors.InvalidArgument,
        };
        return backendInfoObj;
    }
    const backendInfo = new BackendInfo(objectLocationConstraint,
        bucketLocationConstraint, requestEndpoint);
    backendInfoObj = {
        err: null,
        controllingLC: backendInfo.getControllingLocationConstraint(),
        backendInfo,
    };
    return backendInfoObj;
}
