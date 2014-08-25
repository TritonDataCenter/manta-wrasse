/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

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
