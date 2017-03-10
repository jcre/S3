/**
 * writeContinue - Handles sending a HTTP/1.1 100 Continue message to the
 * client, indicating that the request body should be sent.
 * @param {object} req - http request object
 * @param {object} res - http response object
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @return {undefined}
 */
export default function writeContinue(req, res, authInfo) {
    const { headers, query } = req;
    let authV2 = query.Signature;
    let authV4 = query['X-Amz-Algorithm'];
    const authHeader = headers.authorization;
    if (authHeader) {
        authV2 = authHeader.startsWith('AWS ');
        authV4 = authHeader.startsWith('AWS4');
    }
    const isStreaming = headers['content-type'] === 'application/octet-stream';
    // The Expect field-value is case-insensitive, and as of v5.5, Node handles
    // invalid Expect header values by responding with a 417 (Expectation
    // Failed) status code. Thus, just check for the Expect header's existence.
    if (headers.expect) {
        if (authInfo) {
            if (authInfo.isRequesterPublicUser()) {
                res.writeContinue();
            } else if (authV2 || (authV4 && !isStreaming)) {
                res.writeContinue();
            }
        } else if (authV4 && isStreaming) {
            res.writeContinue();
        }
    }
}
