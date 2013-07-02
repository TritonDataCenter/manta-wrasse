// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var os = require('os');

var assert = require('assert-plus');
var libmanta = require('libmanta');
var once = require('once');



///--- Globals

var HOSTNAME = os.hostname();



///--- Helpers

function reset(opts, cb) {
    cb = once(cb);

    var _opts = {
        wrasse: undefined
    };

    // The fact that we update the job _start_ timestamp is irrelevant,
    // as this path serves only to allow $self to cleanup a job that was
    // abandonded
    if (opts.job.timeArchiveDone)
        _opts.wrasse = HOSTNAME;

    opts.marlin.jobArchiveStart(opts.jobId, _opts, function (err) {
        if (err) {
            cb(err);
        } else {
            opts.log.debug({job: opts.jobId}, 'takeover: job reset');
            cb();
        }
    });
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
        limit: 10000,
        '!mtime': opts.takeoverTime,
        owner: process.env.WRASSE_JOB_OWNER,
        state: 'done'
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
            job: r.value,
            jobId: r.key,
            log: log,
            marlin: opts.marlin
        });
    });
}



///--- Exports

module.exports = {
    takeoverJobs: takeoverJobs
};
