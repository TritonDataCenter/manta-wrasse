/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var crypto = require('crypto');
var fs = require('fs');
var path = require('path');
var util = require('util');

var assert = require('assert-plus');
var once = require('once');
var libmanta = require('libmanta');
var mkdirp = require('mkdirp');
var MemoryStream = require('stream').PassThrough;
var rimraf = require('rimraf');
var vasync = require('vasync');



///--- Globals

var sprintf = util.format;

var ADMIN_JOB_FMT = '/poseidon/stor/job_archives/%s/%s/%s/%s/%s';
var JOB_ROOT_FMT = '/%s/jobs/%s';
var JOB_INPUT_FMT = JOB_ROOT_FMT + '/in.txt';
var JOB_OUTPUT_FMT = JOB_ROOT_FMT + '/out.txt';
var JOB_ERROR_FMT = JOB_ROOT_FMT + '/err.txt';
var JOB_FAIL_FMT = JOB_ROOT_FMT + '/fail.txt';
var JOB_MANIFEST_FMT = JOB_ROOT_FMT + '/job.json';



///--- Helpers

function pad(n) {
    return (n < 10 ? '0' + n : n);
}


function streamJob(opts, cb) {
    assert.object(opts, 'options');
    assert.object(opts.job, 'options.job');
    assert.string(opts.key, 'options.key');
    assert.object(opts.log, 'options.log');
    assert.object(opts.marlin, 'options.marlin');
    assert.object(opts.manta, 'options.manta');
    assert.optionalString(opts.type, 'options.type');
    assert.func(cb, 'callback');

    cb = once(cb);

    var log = opts.log;
    var token = opts.job.auth.token;
    var put_opts = {
        headers: {
            'access-control-allow-origin': '*',
            'authorization': sprintf('Token %s', token)
        },
        type: opts.type || 'text/plain'
    };
    var stream = new MemoryStream();

    log.debug('streamJob: entered');

    opts.manta.put(opts.key, stream, put_opts, function (err) {
        if (err) {
            if (err.code === 'DirectoryDoesNotExistError') {
                log.debug({
                    err: err,
                    key: opts.key
                }, 'streamJob: job directory not found');
                cb();
                return;
            }

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

//-- Local Filesystem Cache

function createScratchDir(opts, cb) {
    var dir = (opts.scratchDir || '/var/tmp/wrasse') + '/' + opts.job.jobId;
    dir = path.normalize(dir);
    mkdirp(dir, function (err) {
        if (err) {
            opts.log.debug(err, 'mkdir (local) failed');
            cb(err);
        } else {
            opts.work_dir = dir;
            cb();
        }
    });
}


function saveErrors(opts, cb) {
    cb = once(cb);

    var count = 0;
    var last;
    var log = opts.log;
    var p = path.normalize(opts.work_dir + '/err.txt');
    var seen = 0;
    var stream = fs.createWriteStream(p);

    function fetch() {
        var _opts = {
            log: log,
            marker: last
        };
        var res = opts.marlin.jobFetchErrors(opts.job.jobId, _opts);

        res.on('err', function (obj, record) {
            if (record._id === last)
                return;

            count = count || record._count;
            last = record._id;
            seen++;

            stream.write(JSON.stringify(obj) + '\n');
        });

        res.once('error', function (err) {
            log.error(err, 'saveJobErrors: error during search');
            stream.destroy();
            cb(err);
        });

        res.once('end', function () {
            if (seen === count) {
                opts.localErrorsFile = p;
                stream.once('close', cb);
                stream.end();
            } else {
                fetch();
            }
        });
    }

    stream.once('error', cb);
    stream.once('open', fetch);
}


function saveFailures(opts, cb) {
    cb = once(cb);

    var log = opts.log;
    var p = path.normalize(opts.work_dir + '/fail.txt');
    var stream = fs.createWriteStream(p);

    function fetch() {
        var _opts = {
            log: log
        };
        var res = opts.marlin.jobFetchFailedJobInputs(opts.job.jobId, _opts);

        res.on('key', function (key, record) {
            stream.write(key + '\n');
        });

        res.once('error', function (err) {
            log.error(err, 'saveJobFailures: error during search');
            stream.destroy();
            cb(err);
        });

        res.once('end', function () {
            opts.localFailuresFile = p;
            stream.once('close', cb);
            stream.end();
        });
    }

    stream.once('error', cb);
    stream.once('open', fetch);
}


function saveInputs(opts, cb) {
    cb = once(cb);

    var count = 0;
    var last;
    var log = opts.log;
    var p = path.normalize(opts.work_dir + '/in.txt');
    var seen = 0;
    var stream = fs.createWriteStream(p);

    function fetch() {
        var _opts = {
            log: log,
            marker: last
        };
        var res = opts.marlin.jobFetchInputs(opts.job.jobId, _opts);

        res.on('key', function (key, record) {
            if (record._id === last)
                return;

            count = count || record._count;
            last = record._id;
            seen++;

            stream.write(key + '\n');
        });

        res.once('error', function (err) {
            log.error(err, 'saveJobInputs: error during search');
            stream.destroy();
            cb(err);
        });

        res.once('end', function () {
            if (seen >= count) {
                opts.localInputsFile = p;
                stream.once('close', cb);
                stream.end();
            } else {
                fetch();
            }
        });
    }

    stream.once('error', cb);
    stream.once('open', fetch);
}


function saveOutputs(opts, cb) {
    cb = once(cb);

    var count = 0;
    var last;
    var log = opts.log;
    var p = path.normalize(opts.work_dir + '/out.txt');
    var pi = opts.job.phases.length - 1;
    var seen = 0;
    var stream = fs.createWriteStream(p);

    function fetch() {
        var _opts = {
            log: log,
            marker: last
        };
        var res = opts.marlin.jobFetchOutputs(opts.job.jobId, pi, _opts);

        res.on('key', function (key, record) {
            if (record._id === last)
                return;

            count = count || record._count;
            last = record._id;
            seen++;

            stream.write(key + '\n');
        });

        res.once('error', function (err) {
            log.error(err, 'saveJobOutputs: error during search');
            stream.destroy();
            cb(err);
        });

        res.once('end', function () {
            if (seen >= count) {
                opts.localOutputsFile = p;
                stream.once('close', cb);
                stream.end();
            } else {
                fetch();
            }
        });
    }

    stream.once('error', cb);
    stream.once('open', fetch);
}


function startHeartbeat(opts, cb) {
    (function heartbeat() {
        var _opts = {
            retry: {
                retries: 3
            }
        };
        opts.marlin.jobArchiveHeartbeat(opts.job.jobId, _opts, function (err) {
            if (!err && opts.stop !== true)
                opts.timer = setTimeout(heartbeat, 500);
        });
    })();

    process.nextTick(cb);
}


function loadAccount(opts, cb) {
    opts.mahi.getName({
        uuids: [opts.job.owner]
    }, function (err, names) {
        if (err) {
            opts.log.debug(err, 'loadAccount: failed');
            cb(err);
        } else {
            opts.account = names[opts.job.owner];
            if (!opts.account) {
                cb(new Error('loadAccount: account ' +
                        opts.job.owner + ' not found'));
                return;
            }
            cb();
        }
    });
}


function uploadJobManifest(opts, cb) {
    cb = once(cb);

    var data = JSON.stringify(libmanta.translateJob(opts.job));
    var key = sprintf(JOB_MANIFEST_FMT, opts.account, opts.job.jobId);
    var log = opts.log;
    var token = opts.job.auth.token;
    var put_opts = {
        headers: {
            'access-control-allow-origin': '*',
            'authorization': sprintf('Token %s', token)
        },
        md5: crypto.createHash('md5').update(data, 'utf8').digest('base64'),
        size: Buffer.byteLength(data),
        type: 'application/json'
    };
    var stream = new MemoryStream();

    log.debug({job: opts.job}, 'uploadJobManifest: entered');
    opts.manta.put(key, stream, put_opts, function (err) {
        if (err && err.code === 'DirectoryDoesNotExistError') {
            log.debug(err, 'uploadJobManifest: directory not found.');
            cb();
            return;
        }
        log.debug(err, 'uploadJobManifest: done');
        cb(err);
    });

    stream.end(data);
}


function uploadJobManifestPoseidon(opts, cb) {
    cb = once(cb);

    var d = new Date(opts.job.timeCreated);
    var data = JSON.stringify(opts.job);
    var dir = sprintf(ADMIN_JOB_FMT,
                      d.getUTCFullYear(),
                      pad(d.getUTCMonth() + 1),
                      pad(d.getUTCDate()),
                      pad(d.getUTCHours()),
                      opts.job.jobId);
    var dir_opts = {
        headers: {
            'access-control-allow-origin': '*'
        }
    };
    var key = dir + '/job.json';
    var log = opts.log;
    var put_opts = {
        headers: {
            'access-control-allow-origin': '*'
        },
        md5: crypto.createHash('md5').update(data, 'utf8').digest('base64'),
        size: Buffer.byteLength(data),
        type: 'application/json'
    };
    var stream = new MemoryStream();

    log.debug({
        dir: dir,
        job: opts.job,
        key: key
    }, 'uploadJobManifestPoseidon: entered');
    opts.manta.mkdirp(dir, dir_opts, function (mkdir_err) {
        if (mkdir_err) {
            cb(mkdir_err);
            return;
        }

        opts.manta.put(key, stream, put_opts, function (err) {
            log.debug(err, 'uploadJobManifest: done');
            cb(err);
        });

        stream.end(data);
    });
}


function uploadJobInputs(opts, cb) {
    cb = once(cb);

    opts.key = sprintf(JOB_INPUT_FMT, opts.account, opts.job.jobId);

    var log = opts.log;
    var input = fs.createReadStream(opts.localInputsFile);
    var stream = streamJob(opts, cb);

    log.debug('uploadJobInputs: entered');
    input.once('error', cb);
    input.pipe(stream);

    stream.once('close', function () {
        log.debug('uploadJobInputs: done');
        cb();
    });
}


function uploadJobOutputs(opts, cb) {
    cb = once(cb);

    opts.key = sprintf(JOB_OUTPUT_FMT, opts.account, opts.job.jobId);

    var log = opts.log;
    var input = fs.createReadStream(opts.localOutputsFile);
    var stream = streamJob(opts, cb);

    log.debug('uploadJobOutputs: entered');
    input.once('error', cb);
    input.pipe(stream);

    stream.once('close', function () {
        log.debug('uploadJobOutputs: done');
        cb();
    });
}


function uploadJobFailures(opts, cb) {
    cb = once(cb);

    opts.key = sprintf(JOB_FAIL_FMT, opts.account, opts.job.jobId);

    var log = opts.log;
    var input = fs.createReadStream(opts.localFailuresFile);
    var stream = streamJob(opts, cb);

    log.debug('uploadJobFailures: entered');
    input.once('error', cb);
    input.pipe(stream);

    stream.once('close', function () {
        log.debug('uploadJobFailures: done');
        cb();
    });
}


function uploadJobErrors(opts, cb) {
    cb = once(cb);

    opts.key = sprintf(JOB_ERROR_FMT, opts.account, opts.job.jobId);
    opts.type = 'application/x-json-stream; type=job-error';


    var log = opts.log;
    var input = fs.createReadStream(opts.localErrorsFile);
    var stream = streamJob(opts, cb);

    log.debug('uploadJobErrors: entered');
    input.once('error', cb);
    input.pipe(stream);

    stream.once('close', function () {
        log.debug('uploadJobErrors: done');
        cb();
    });
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


function cleanupScratchDir(opts, cb) {
    cb = once(cb);
    rimraf(opts.work_dir, cb);
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
                createScratchDir,
                startHeartbeat,
                loadAccount,
                saveErrors,
                saveFailures,
                saveInputs,
                saveOutputs,
                uploadJobManifest,
                uploadJobOutputs,
                uploadJobErrors,
                uploadJobInputs,
                uploadJobFailures,
                deleteLiveEntry,
                archiveDone,
                uploadJobManifest,
                uploadJobManifestPoseidon,
                cleanupScratchDir
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
    streamJob: streamJob,
    upload: upload
};
