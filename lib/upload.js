// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var crypto = require('crypto');
var os = require('os');
var util = require('util');

var assert = require('assert-plus');
var once = require('once');
var libmanta = require('libmanta');
var MemoryStream = require('readable-stream/passthrough.js');
var vasync = require('vasync');

var mahi = require('./mahi');



///--- Globals

var sprintf = util.format;

var JOB_ROOT_FMT = '/%s/jobs/%s';
var JOB_INPUT_FMT = JOB_ROOT_FMT + '/in.txt';
var JOB_OUTPUT_FMT = JOB_ROOT_FMT + '/out.txt';
var JOB_ERROR_FMT = JOB_ROOT_FMT + '/err.txt';
var JOB_FAIL_FMT = JOB_ROOT_FMT + '/fail.txt';
var JOB_MANIFEST_FMT = JOB_ROOT_FMT + '/job.json';



///--- Helpers

function streamJob(opts, cb) {
    assert.object(opts, 'options');
    assert.object(opts.job, 'options.job');
    assert.string(opts.key, 'options.key');
    assert.object(opts.log, 'options.log');
    assert.object(opts.marlin, 'options.marlin');
    assert.object(opts.manta, 'options.manta');
    assert.optionalString(opts.type, 'options.type');
    assert.object(opts.user, 'options.user');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = opts.log;
    var put_opts = {
        headers: {
            'access-control-allow-origin': '*'
        },
        type: opts.type || 'text/plain'
    };
    var stream = new MemoryStream();

    log.debug('streamJob: entered');

    opts.manta.put(opts.key, stream, put_opts, function (err) {
        if (err) {
            log.debug({
                err: err,
                key: opts.key
            }, 'streamJob: failed');
            cb(err);
            return;
        }

        log.debug({
            key: opts.key
        }, 'streamJob: done');
        cb();
    });

    return (stream);
}



///--- API

function startHeartbeat(opts, cb) {
    function heartbeat() {
        var _opts = {
            retry: {
                retries: 3
            }
        };
        opts.marlin.jobArchiveHeartbeat(opts.job.jobId, _opts, function (err) {
            if (!err && opts.stop !== true)
                opts.timer = setTimeout(heartbeat, 1000);
        });
    }

    opts.timer = setTimeout(heartbeat, 1000);

    process.nextTick(cb);
}


function loadUser(opts, cb) {
    opts.mahi.userFromUUID(opts.job.owner, function (err, user) {
        if (err) {
            cb(err);
        } else {
            opts.user = user;
            cb();
        }
    });
}


function mkdir(opts, cb) {
    cb = once(cb);

    var key = '/' + opts.user.login + '/jobs/' + opts.job.jobId + '/stor';
    var log = opts.log;
    var _opts = {
        headers: {
            'access-control-allow-origin': '*'
        }
    };

    log.debug({key: key}, 'mkdir: entered');
    opts.manta.mkdirp(key, _opts, function (err) {
        log.debug(err, 'mkdir: done');
        cb(err);
    });
}


function uploadJobManifest(opts, cb) {
    cb = once(cb);

    var data = JSON.stringify(libmanta.translateJob(opts.job));
    var key = sprintf(JOB_MANIFEST_FMT, opts.user.login, opts.job.jobId);
    var log = opts.log;
    var put_opts = {
        headers: {
            'access-control-allow-origin': '*'
        },
        md5: crypto.createHash('md5').update(data).digest('base64'),
        size: Buffer.byteLength(data),
        type: 'application/json'
    };
    var stream = new MemoryStream();

    log.debug({job: opts.job}, 'uploadJobManifest: entered');
    opts.manta.put(key, stream, put_opts, function (err) {
        log.debug(err, 'uploadJobManifest: done');
        cb(err);
    });

    stream.end(data);
}


function uploadJobInputs(opts, cb) {
    cb = once(cb);

    opts.key = sprintf(JOB_INPUT_FMT, opts.user.login, opts.job.jobId);

    var job = opts.job;
    var log = opts.log;
    var res;
    var stream = streamJob(opts, cb);

    log.debug('uploadJobInputs: entered');

    res = opts.marlin.jobFetchInputs(job.jobId, {log: log});

    res.on('key', function (key) {
        stream.write(key + '\n');
    });

    res.once('error', function (err) {
        log.error(err, 'uploadJobInputs: error during search');
        cb(err);
        stream.end();
    });

    res.once('end', stream.end.bind(stream));
}


function uploadJobOutputs(opts, cb) {
    cb = once(cb);

    opts.key = sprintf(JOB_OUTPUT_FMT, opts.user.login, opts.job.jobId);

    var job = opts.job;
    var log = opts.log;
    var res;
    var pi = job.phases.length - 1;
    var stream = streamJob(opts, cb);

    log.debug('uploadJobOutputs: entered');
    res = opts.marlin.jobFetchOutputs(job.jobId, pi, {log: log});

    res.on('key', function (key) {
        log.debug({key: key}, 'uploadJobOutputs: key found');
        stream.write(key + '\n');
    });

    res.once('error', function (err) {
        log.error(err, 'uploadJobOutputs: error during search');
        cb(err);
        stream.end();
    });

    res.once('end', stream.end.bind(stream));
}


function uploadJobFailures(opts, cb) {
    cb = once(cb);

    opts.key = sprintf(JOB_FAIL_FMT, opts.user.login, opts.job.jobId);

    var job = opts.job;
    var log = opts.log;
    var res;
    var stream = streamJob(opts, cb);

    log.debug('uploadJobFailures: entered');
    res = opts.marlin.jobFetchFailedJobInputs(job.jobId, {log: log});

    res.on('key', function (key) {
        log.debug({key: key}, 'uploadJobFailures: key found');
        stream.write(key + '\n');
    });

    res.once('error', function (err) {
        log.error(err, 'uploadJobFailures: error during search');
        cb(err);
        stream.end();
    });

    res.once('end', stream.end.bind(stream));
}


function uploadJobErrors(opts, cb) {
    cb = once(cb);

    opts.key = sprintf(JOB_ERROR_FMT, opts.user.login, opts.job.jobId);
    opts.type = 'application/x-json-stream; type=job-error';

    var job = opts.job;
    var log = opts.log;
    var res;
    var stream = streamJob(opts, cb);

    log.debug('uploadJobErrors: entered');

    res = opts.marlin.jobFetchErrors(job.jobId);

    res.on('err', function (obj) {
        log.debug({obj: obj}, 'uploadJobErrors: err found');
        stream.write(JSON.stringify(obj) + '\n');
    });

    res.once('error', function (err) {
        log.error(err, 'uploadJobErrors: error during search');
        cb(err);
        stream.end();
    });

    res.once('end', stream.end.bind(stream));
}


function deleteLiveEntry(opts, cb) {
    var job = opts.job;
    var key = sprintf('/%s/jobs/%s/live', job.auth.login, job.jobId);

    var log = opts.log;

    log.debug({key: key}, 'deleteLiveEntry: entered');

    opts.manta.unlink(key, function (err) {
        log.debug(err, 'deleteLiveEntry: done');
        if (err && err.name === 'ResourceNotFoundError') {
            cb();
        } else {
            cb(err);
        }
    });
}


function archiveDone(opts, cb) {
    cb = once(cb);

    var log = opts.log;

    log.debug('archiveDone: entered');
    opts.marlin.jobArchiveDone(opts.job.jobId, function (err) {
        if (err) {
            log.error(err, 'archiveDone: failed');
            cb(err);
        } else {
            log.debug('archiveDone:  done');
            opts.marlin.jobFetch(opts.job.jobId, function (err2, record) {
                if (err2) {
                    cb(err2);
                } else {
                    opts.job = record.value;
                    cb();
                }
            });
        }
    });
}



///--- Pipeline

function upload(opts) {
    assert.object(opts, 'options');
    assert.object(opts.log, 'options.log');
    assert.object(opts.marlin, 'options.marlin');
    assert.object(opts.manta, 'options.manta');

    function _upload(job, cb) {
        assert.object(job, 'job');
        assert.func(cb, 'callback');

        cb = once(cb);

        var log = opts.log.child({
            jobId: job.jobId
        }, true);

        var cookie = {
            job: job,
            log: opts.log,
            mahi: opts.mahi,
            manta: opts.manta,
            marlin: opts.marlin,
            moray: opts.moray
        };

        vasync.pipeline({
            funcs: [
                startHeartbeat,
                loadUser,
                mkdir,
                uploadJobManifest,
                uploadJobInputs,
                uploadJobOutputs,
                uploadJobFailures,
                uploadJobErrors,
                deleteLiveEntry,
                archiveDone,
                uploadJobManifest
            ],
            arg: cookie
        }, function onUploadDone(err) {
            clearTimeout(cookie.timer);
            cookie.stop = true;

            if (err) {
                log.error({
                    err: err,
                    job: job.jobId
                }, 'job upload failed');
                cb(err);
            } else {
                log.info({
                    job: job
                }, 'job uploaded');
                cb();
            }
        });
    }

    return (_upload);
}



///--- Exports

module.exports = {
    uploadJobManifest: uploadJobManifest,
    uploadJobInputs: uploadJobInputs,
    uploadJobOutputs: uploadJobOutputs,
    uploadJobErrors: uploadJobErrors,
    uploadJobErrors: uploadJobErrors,
    streamJob: streamJob,
    upload: upload
};
