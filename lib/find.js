// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var os = require('os');

var assert = require('assert-plus');
var libmanta = require('libmanta');
var once = require('once');



///--- Globals

var HOSTNAME = os.hostname();



///--- API

function assignToSelf(opts, cb) {
    cb = once(cb);

    var job = opts.job;
    var marlin = opts.marlin;
    var _opts = {
        log: opts.log,
        wrasse: HOSTNAME
    };

    marlin.jobArchiveStart(job.key, _opts, function (err) {
        if (err) {
            cb(err);
        } else {
            opts.jobs.push(opts.job.value);
            cb();
        }
    });
}


function findDoneJobs(opts, cb) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.object(opts.marlin, 'options.marlin');
    assert.func(cb, 'callback');

    cb = once(cb);

    opts.log.debug('findDoneJobs: entered');

    var jobs = [];
    var log = opts.log;
    var marlin = opts.marlin;
    var queue = libmanta.createQueue({
        limit: 10,
        worker: assignToSelf
    });
    var res = marlin.jobsList({
        archived: false,
        owner: process.env.WRASSE_JOB_OWNER,
        state: 'done',
        wrasse: null
    });

    queue.on('error', function (err) {
        log.error(err, 'findDoneJobs: checkMoray error');
        cb(err);
    });

    res.once('error', function (err) {
        log.error(err, 'findDoneJobs: error during search');
        cb(err);
    });

    res.on('record', function (r) {
        queue.push({
            job: r,
            jobs: jobs,
            log: log,
            marlin: marlin
        });
    });

    res.once('end', function () {
        queue.close();
    });

    queue.once('end', function () {
        if (log.debug()) {
            log.debug({
                jobs: jobs.map(function (j) {
                    return (j.jobId);
                })
            }, 'findDoneJobs: done');
        }
        cb(null, jobs);
    });
}



///--- Exports

module.exports = {
    findDoneJobs: findDoneJobs
};
