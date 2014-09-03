<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# manta-wrasse

This repository is part of the Joyent Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/joyent/manta) project page.

Wrasse is a service that archives "old" jobs once they are completed into
Manta itself.  This is necessary so the main jobs database can be kept light,
and scaled much more cheaply than if it contained all jobs, heavily indexed,
for all of time.

# Design

Wrasse runs as a single node process in a zone, and can/should be run
redundantly (meaning multiple zone instances).  The wrasse process polls the
[marlin](http://github.com/joyent/manta-marlin) jobs table, looking for jobs
that are in `state=done` and are _unassigned_.  _Unassigned_ means that no
wrasse process has rewritten the job record back indicating that it is working
on the job.

Once an _unassigned_ job is discovered, wrasse immediately tries to "claim" the
job, by recording that `$self` "owns" the job (using moray etags), and if
successful proceeds to "heartbeat" that it is still working on the job.

Once a job is _assigned_ to a particular wrasse, that wrasse then serially
uploads each of the "types" for that job:

- inputs
- outputs
- errors
- failures
- the job record itself

Each of these is fully paginated, and uploaded to a "file" in manta, underneath
the user's `~~/jobs/:jobid` directory.  For example, once a job is archived,
one would see this:

```
$ mls ~~/jobs | head -5
000013ae-0471-c94d-8380-db98f359e752/
001536a9-d5eb-4fc5-a752-a5dc1315e93a/
004da208-2078-c6e4-d133-9fffd1837c80/
0070e24e-2ab7-659e-8a0d-d4276a2c6614/
00855bf2-c09c-47a2-9a13-c41eb82df1da/

$ mls ~~/jobs/00855bf2-c09c-47a2-9a13-c41eb82df1da/
err.txt
fail.txt
in.txt
job.json
out.txt
stor/
```

Note that `~~/jobs/:jobid/stor` is where all outputs are written for the job,
and is not touched by wrasse.

Once a job is archived into Manta, it is left alone in the marlin database for
a configured `linger` time (typically on the order of hours).  Once that time
has elapsed, wrasse will then purge all records associated with the job from the
database.  Note that with Postgres MVCC, this requires an operator to keep close
tabs on "dead tuples" (practically it means a DBA must periodically go kick
vacuum/analyze). At this point, the only way to see details about a job are to
look in the manta "archive" area.

Lastly, to handle the scenario where a wrasse process crashes, all wrasse
instances also poll for "abandoned" archivers.  As mentioned above, after a
wrasse annoints itself as owner of a job, it then proceeeds to "heartbeat" the
job record.  The act of looking for an abandoned job simply means looking for
jobs that have not had an active heartbeat for a configured number of seconds.
When a wrasse takes over a job from another wrasse, it simply starts at the
beginning and goes through the logic listed above.

# Configuration

Configuration of wrasse involves telling it where
[mahi](https://github.com/joyent/mahi) and the
[moray](https://github.com/joyent/moray) that contains the `marlin_jobs_v2`
bucket are, along with the requisite manta information for
[node-manta](https://github.com/joyent/node-manta).

In addition, some tunables affecting poll/takeover time, how many items to
delete in a batch, etc., can be set.

```javascript
{
    "auth": {
        "host": "authcache.coal.joyent.us",
        "port": 6379,
        "options": {
            "enable_offline_queue": false
        }
    },
    "logLevel": "info",
    "manta": {
        "url": "https://manta.coal.joyent.us",
        "user": "mcavage",
        "keyId": "7b:c0:5c:d6:9e:11:0c:76:04:4b:03:c9:11:f2:72:7f",
        "key": "/home/mcavage/.ssh/id_rsa"
    },
    "marlin": {
        "moray": {
            "url": "tcp://1.moray.coal.joyent.us:2020",
            "reconnect": {
                "maxTimeout": 30000,
                "retries": 120
            }
        }
    },
    "lingerTime": 120,
    "takeoverTime": 15,
    "pollInterval": 2,
    "deleteLimit": 2000,
    "pingPort": 1080
}
```

# Hacking on Wrasse

In order to work with wrasse in sandbox, typically one would have a CoaL
installation of Manta (or some other local install), and run the wrasse process
directly out of the git repo by creating a stub config file as above (ensure your
development system's DNS is correctly configured), and shutting down the actual
Manta's wrasse processes; this is not necessary, but helps so you an be sure your
git wrasse is the only one that will see jobs while running.  As with most Manta
services, very verbose logging is available as well.
