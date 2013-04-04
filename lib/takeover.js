// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var os = require('os');

var assert = require('assert-plus');
var libmanta = require('libmanta');
var once = require('once');



///--- Globals

var HOSTNAME = os.hostname();



///--- Helpers

function reset(opts, cb) {
    // timeStarted will still be set, but it's irrelevant as somebody's "find"
    // will pick up the job since the wrasse id is unassigned
    opts.marlin.jobArchiveStart(opts.jobId, {wrasse: undefined}, once(cb));
}



///--- API

function takeoverJobs(opts, cb) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.object(opts.marlin, 'options.marlin');
    assert.number(opts.takeoverTime, 'options.takeoverTime');
    assert.func(cb, 'callback');

    cb = once(cb);

    opts.log.debug('takeoverJobs: entered');

    var log = opts.log;
    var queue = libmanta.createQueue({
        limit: 10,
        worker: reset
    });
    var res = opts.marlin.jobsList({
        archived: false,
        archiveStartedBefore: opts.takeoverTime,
        owner: process.env.WRASSE_JOB_OWNER,
        state: 'done',
        wrasse: true
    });

    queue.once('end', function () {
        log.debug('takeoverJobs: done');
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
    takeoverJobs: takeoverJobs
};
