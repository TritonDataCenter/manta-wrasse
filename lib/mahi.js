// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var mahi = require('mahi');



///--- API

function createMahiClient(opts, cb) {
    var client = mahi.createClient(opts);
    cb(null, client);
}


function loadUser(opts, cb) {
    assert.object(opts, 'options');
    assert.object(opts.job, 'options.job');
    assert.object(opts.log, 'options.log');
    assert.object(opts.mahi, 'options.mahi');
    assert.func(cb, 'callback');

    var log = opts.log;

    log.debug({uuid: opts.job.owner}, 'loadUser: entered');
    opts.mahi.userFromUUID(opts.job.owner, function (err, user) {
        if (err) {
            log.error(err, 'loadUser: error from mahi');
            cb(err);
        } else {
            log.debug({user: user}, 'loadUser: done');
            cb(null, user);
        }
    });
}



///--- Exports

module.exports = {
    createMahiClient: createMahiClient,
    loadUser: loadUser
};
