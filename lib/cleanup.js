// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var os = require('os');

var assert = require('assert-plus');
var libmanta = require('libmanta');
var once = require('once');



///--- Globals

var HOSTNAME = os.hostname();



///--- Helpers

function purge(opts, cb) {
    opts.marlin.jobDelete(opts.jobId, once(cb));
}



///--- API

function cleanupJobs(opts, cb) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.number(opts.lingerTime, 'options.lingerTime');
    assert.object(opts.marlin, 'options.marlin');
    assert.func(cb, 'callback');

    cb = once(cb);

    opts.log.debug('cleanupJobs: entered');

    var log = opts.log;
    var queue = libmanta.createQueue({
        limit: 10,
        worker: purge
    });
    var res = opts.marlin.jobsList({
        archived: true,
        archivedBefore: opts.lingerTime,
        owner: process.env.WRASSE_JOB_OWNER,
        state: 'done',
        wrasse: HOSTNAME
    });

    queue.once('end', function () {
        log.debug('cleanupJobs: done');
        cb();
    });
    queue.on('error', cb);
    res.on('error', cb);
    res.once('end', queue.close.bind(queue));

    res.on('record', function (r) {
        queue.push({
            jobId: r.key,
            marlin: opts.marlin
        });
    });
}



///--- Exports

module.exports = {
    cleanupJobs: cleanupJobs
};
