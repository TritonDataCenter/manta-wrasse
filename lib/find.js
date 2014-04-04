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
        wrasse: HOSTNAME
    };

    // Basically, we just eat errors here, as the worst that happens
    // is we try again in N seconds.  Specifically, this will commonly
    // error as wrasse agents race to take over a job
    marlin.jobArchiveStart(job.key, _opts, function (claim_err) {
        if (!claim_err) {
            marlin.jobFetch(job.key, function (err, record) {
                if (!err)
                    opts.jobs.push(record.value);

                cb();
            });
        } else {
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
        jobId: process.env.WRASSE_JOB_ID,
        limit: 1000,
        owner: process.env.WRASSE_JOB_OWNER,
        state: 'done',
        wrasse: null,
        log: log
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
