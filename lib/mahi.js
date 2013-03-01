// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var libmanta = require('libmanta');
var once = require('once');



///--- API

function createMahiClient(opts, cb) {
    cb = once(cb);

    var mahi = libmanta.createMahiClient(opts);
    mahi.once('error', cb);
    mahi.once('connect', function () {
        mahi.removeListener('error', cb);
        cb(null, mahi);
    });
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
