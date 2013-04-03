// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var os = require('os');

var assert = require('assert-plus');
var libmanta = require('libmanta');
var once = require('once');
var vasync = require('vasync');



///--- Globals

var BUCKET = 'wrasse';
var HOSTNAME = os.hostname();



///--- API

function checkMoray(opts, cb) {
    cb = once(cb);

    var j = opts.job;
    var m = opts.moray;
    var now = new Date().getTime();
    var me = {
        worker: HOSTNAME
    };

    function store(etag) {
        m.putObject(BUCKET, j.jobId, me, {etag: etag}, function (err) {
            if (err) {
                cb(err);
            } else {
                opts.jobs.push(j);
                cb();
            }
        });
    }

    m.getObject(BUCKET, j.jobId, function (get_err, obj) {
        if (get_err && get_err.name === 'ObjectNotFoundError') {
            store(null);
        } else if (get_err) {
            cb(get_err);
        } else {
            if ((now - obj._mtime) >= opts.takeoverTime) {
                store(obj._etag);
            } else {
                cb();
            }
        }
    });
}


function findDoneJobs(opts, cb) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.number(opts.lingerTime, 'options.lingerTime');
    assert.object(opts.marlin, 'options.marlin');
    assert.object(opts.moray, 'options.moray');
    assert.number(opts.takeoverTime, 'options.takeoverTime');
    assert.func(cb, 'callback');

    opts.log.debug('findDoneJobs: entered');

    cb = once(cb);

    var jobs = [];
    var log = opts.log;
    var marlin = opts.marlin;
    var queue = libmanta.createQueue({
        limit: 10,
        worker: checkMoray
    });
    var res = marlin.jobsList({
        owner: process.env.WRASSE_JOB_OWNER,
        state: 'done'
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
        log.debug({
            record: r.value
        }, 'findDoneJobs: job found');
        queue.push({
            jobs: jobs,
            log: log,
            lingerTime: opts.lingerTime,
            moray: opts.moray,
            job: r.value,
            takeoverTime: opts.takeoverTime
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
