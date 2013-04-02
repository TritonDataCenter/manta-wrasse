// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var once = require('once');



///--- API

function findDoneJobs(opts, cb) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.object(opts.marlin, 'options.marlin');
    assert.func(cb, 'callback');

    cb = once(cb);

    var jobs = [];
    var log = opts.log;
    var marlin = opts.marlin;
    var res;

    opts.jobs = [];
    log.debug({marlin: marlin}, 'findDoneJobs: entered');
    res = marlin.jobsList({
        owner: process.env.WRASSE_JOB_OWNER,
        state: 'done'
    });

    res.on('record', function (r) {
        log.debug({
            record: r.value
        }, 'findDoneJobs: job found');
        jobs.push(r.value);
    });

    res.once('error', function (err) {
        log.error(err, 'findDoneJobs: error during search');
        cb(err);
    });

    res.once('end', function () {
        log.debug('findDoneJobs: done');
        cb(null, jobs);
    });
}



///--- Exports

module.exports = {
    findDoneJobs: findDoneJobs
};
