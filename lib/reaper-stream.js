/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2015, Joyent, Inc.
 */

module.exports = ReaperStream;

var stream = require('stream');
var util = require('util');
var assert = require('assert-plus');
var utils = require('./utils');
var bunyan = require('bunyan');
var restify = require('restify-clients');
var qs = require('querystring');
var svcTagParser = require('triton-tags/lib/cns-svc-tag');
var EventEmitter = require('events').EventEmitter;

var consts = require('./consts');

var FSM = require('mooremachine').FSM;

/* Attempt to reap VMs that haven't been visited in REAP_TIME seconds. */
var DEFAULT_REAP_TIME = 3600;

function ReaperFSM(strm, opts) {
	assert.object(opts, 'options');

	assert.optionalObject(opts.log, 'options.log');
	var log = opts.log || bunyan.createLogger({name: 'cns'});
	this.log = log.child({stage: 'ReaperStream'});

	assert.object(opts.config, 'options.config');
	assert.object(opts.config.vmapi_opts, 'config.vmapi_opts');
	assert.string(opts.config.vmapi_opts.address, 'vmapi_opts.address');

	assert.optionalObject(opts.agent, 'options.agent');

	assert.object(opts.client, 'options.client');
	this.redis = opts.client;

	this.stream = strm;
	this.remaining = [];
	this.vmuuid = undefined;
	this.shortName = undefined;
	this.lastError = undefined;
	this.services = [];
	this.retries = 3;
	this.onTimer = false;
	this.minSleep = 100;
	this.sleep = 100;
	this.maxSleep = 10000;
	this.reapTime = DEFAULT_REAP_TIME;

	this.client = restify.createJsonClient({
		url: 'http://' + opts.config.vmapi_opts.address,
		agent: opts.agent
	});

	FSM.call(this, 'idle');
}
util.inherits(ReaperFSM, FSM);

ReaperFSM.prototype.fetch = function (uuid) {
	var eve = new EventEmitter();
	var self = this;
	eve.send = function () {
		self.client.get('/vms/' + uuid, function (err, req, res, obj) {
			if (err) {
				eve.emit('error', err);
				return;
			}
			utils.cleanVM(obj);
			obj.origin = 'reaper';
			eve.emit('result', obj);
		});
	};
	return (eve);
};

ReaperFSM.prototype.start = function () {
	this.onTimer = true;
	this.emit('startAsserted');
};

ReaperFSM.prototype.wake = function () {
	this.emit('wakeAsserted');
};

ReaperFSM.prototype.state_idle = function (on, once, timeout) {
	var self = this;
	once(this, 'startAsserted', function () {
		self.gotoState('listVms');
	});
	if (this.onTimer) {
		timeout(this.reapTime*1000, function () {
			self.gotoState('listVms');
		});
	}
};

ReaperFSM.prototype.state_listVms = function (on, once, timeout) {
	var self = this;
	timeout(10000, function () {
		self.lastError = new Error(
		    'Timed out waiting for redis response');
		self.gotoState('listError');
	});
	var req = FSM.wrap(this.redis.keys).call(this.redis, 'vm:*');

	once(req, 'error', function (err) {
		self.lastError = err;
		self.gotoState('listError');
	});
	once(req, 'return', function (keys) {
		for (var i = 0; i < keys.length; ++i) {
			var parts = keys[i].split(':');
			if (parts.length === 2 && parts[0] === 'vm') {
				self.remaining.push(parts[1]);
			}
		}

		self.log.debug('pushed %d candidates for reaping',
		    self.remaining.length);

		self.gotoState('next');
	});

	req.run();
};

ReaperFSM.prototype.state_listError = function (on, once, timeout) {
	var self = this;
	this.log.error(this.lastError,
	    'error while listing VMs in redis, retry in 1s');
	timeout(1000, function () {
		self.gotoState('listVms');
	});
};

ReaperFSM.prototype.state_next = function () {
	var self = this;
	self.retries = 3;
	if (self.remaining.length > 0) {
		self.vmuuid = self.remaining.shift();
		self.gotoState('checkLastVisited');
	} else {
		self.log.debug('reaping complete');
		self.gotoState('idle');
	}
};

ReaperFSM.prototype.state_checkLastVisited = function (on, once, timeout) {
	var self = this;
	var log = self.log.child({uuid: self.vmuuid});
	timeout(1000, function () {
		self.lastError = new Error(
		    'Timed out waiting for redis response');
		self.gotoState('checkError');
	});

	var req = FSM.wrap(self.redis.hget).call(self.redis,
	    'vm:' + self.vmuuid, 'last_visit');

	once(req, 'error', function (err) {
		self.lastError = err;
		self.gotoState('checkError');
	});

	once(req, 'return', function (val) {
		if (val === null) {
			log.warn({queue: self.remaining.length},
			    'vm has no last_visited record, skipping');
			self.gotoState('next');
			return;
		}

		var now = Math.round((new Date()).getTime() / 1000);
		var lastVisited = parseInt(val, 10);
		if (now - lastVisited > self.reapTime) {
			log.trace({queue: self.remaining.length},
			    'reaping, last visited %d sec ago',
			    (now - lastVisited));
			self.gotoState('checkReaped');
		} else {
			self.gotoState('next');
		}
	});

	req.run();
};

ReaperFSM.prototype.state_checkReaped = function (on, once, timeout) {
	var self = this;
	timeout(1000, function () {
		self.lastError = new Error(
		    'Timed out waiting for redis response');
		self.gotoState('checkError');
	});
	var req = FSM.wrap(self.redis.hget).call(self.redis,
	    'vm:' + self.vmuuid, 'reaped');

	once(req, 'error', function (err) {
		self.lastError = err;
		self.gotoState('checkError');
	});

	once(req, 'return', function (val) {
		/*
		 * If we found something, this is the second time we've
		 * visited this VM and it's still destroyed. We can
		 * forget that it existed now.
		 */
		if (val !== null) {
			self.redis.del('vm:' + self.vmuuid);
			self.gotoState('checkShortName');
			return;
		}

		self.gotoState('fetchAndPush');
	});

	req.run();
};

ReaperFSM.prototype.state_checkShortName = function (on, once, timeout) {
	var self = this;
	timeout(1000, function () {
		self.lastError = new Error(
			'Timed out waiting for redis response');
		self.gotoState('checkError');
	});

	var req = FSM.wrap(self.redis.get).call(self.redis,
		'shortname:' + self.shortName);

	once(req, 'error', function (err) {
		self.lastError = err;
		self.gotoState('checkError');
	});

	once(req, 'return', function (val) {
		res = JSON.parse(val);

		if (res.vms.indexOf(self.vmuuid) != -1)
			self.redis.del(self.shortName);

		if (self.services.length > 0)
			self.gotoState('checkServiceName')

		self.gotoState('next');
	});
};

ReaperFSM.prototype.state_checkServiceName = function (on, once, timeout) {
	var self = this;
	timeout(1000, function () {
		self.lastError = new Error(
			'Timed out waiting for redis response');
		self.gotoState('checkError');
	});

	var service = self.services.pop();

	var req = FSM.wrap(self.redis.get).call(self.redis,
		'shortname:' + service);

	once(req, 'error', function (err) {
		self.lastError = err;
		self.gotoState('checkError');
	});

	once(req, 'return', function (val) {
		res = JSON.parse(val);

		var idx = res.vms.indexOf(self.vmuuid);
		if (idx > -1)
			res.vms.splice(idx, 1);

		if (res.vms.length === 0)
			self.redis.del('shortname:' + service);
		else
			self.redis.set('shortname:' + service, JSON.stringify(res));

		if (self.services.length > 0)
			self.gotoState('checkServiceName')
		else
			self.gotoState('next');
	});
};

ReaperFSM.prototype.state_fetchAndPush = function (on, once, timeout) {
	var self = this;
	timeout(5000, function () {
		self.lastError = new Error(
		    'Timed out waiting for VMAPI response');
		self.gotoState('checkError');
	});
	var req = self.fetch(self.vmuuid);
	once(req, 'error', function (err) {
		self.lastError = new Error('Error from VMAPI: ' +
		    err.name + ': ' + err.message);
		self.lastError.name = 'VMAPIError';
		self.lastError.origin = err;
		self.gotoState('checkError');
	});
	once(req, 'result', function (obj) {
		if (obj.state === 'destroyed' || obj.destroyed ||
		    obj.state === 'failed' || obj.state === 'incomplete') {
			self.redis.hset('vm:' + self.vmuuid, 'reaped', 'yes');
		}

		self.shortName = obj.alias;

		if (obj.tags) {
			var SERVICES_TAG = consts.SERVICES_TAG;
			var tag = obj.tags[SERVICES_TAG];
			if (typeof (tag) === 'string') {
				svcs = svcTagParser.parse(tag);
				obj.services = svcs.map(function(r) { return r.name; });
			}
                }

		if (self.stream.push(obj) === false) {
			self.gotoState('sleep_full');
			return;
		}

		self.gotoState('sleep');
	});
	req.send();
};

ReaperFSM.prototype.state_sleep_full = function (on, once, timeout) {
	var self = this;
	timeout(self.sleep, function () {
		/*
		 * Pipeline is full, and stayed full for our entire sleep
		 * interval (we didn't get a wake-up). Increase our sleep
		 * interval to avoid taking up the whole available throughput
		 * of the pipeline.
		 */
		self.sleep *= 2;
		if (self.sleep > self.maxSleep) {
			self.log.warn('reaper backing off to maximum,' +
			    ' pipeline seems to be persistently full' +
			    ' (is this a bug?)');
			self.sleep = self.maxSleep;
		}
		self.gotoState('next');
	});
	once(this, 'wakeAsserted', function () {
		self.gotoState('next');
	});
};

ReaperFSM.prototype.state_sleep = function (on, once, timeout) {
	var self = this;
	/*
	 * If we weren't full, we always wait for our entire sleep interval
	 * and ignore wakeups, so that we don't dominate the pipeline's
	 * available throughput.
	 */
	timeout(self.sleep, function () {
		self.gotoState('next');
	});
	once(this, 'wakeAsserted', function () {
		/*
		 * If the pipeline has emptied out, head down towards our
		 * lowest sleep interval -- it might have been a transient
		 * traffic jam that's cleared up now.
		 */
		self.sleep /= 2;
		if (self.sleep < self.minSleep)
			self.sleep = self.minSleep;
	});
};

ReaperFSM.prototype.state_checkError = function (on, once, timeout) {
	var self = this;
	--(self.retries);
	var log = self.log.child({uuid: self.vmuuid,
	    retries_remaining: self.retries});
	if (self.retries > 0) {
		log.error(self.lastError,
		    'error while checking vm, retrying in 1s');
		timeout(1000, function () {
			self.gotoState('checkLastVisited');
		});
	} else {
		log.error(self.lastError,
		    'error while checking vm, out of retries -- will skip');
		timeout(5000, function () {
			self.gotoState('next');
		});
	}
};

function ReaperStream(opts) {
	this.fsm = new ReaperFSM(this, opts);
	var streamOpts = {
		objectMode: true
	};
	stream.Readable.call(this, streamOpts);
}
util.inherits(ReaperStream, stream.Readable);

ReaperStream.prototype._read = function () {
	this.fsm.start();
	this.fsm.wake();
};

ReaperStream.prototype.start = function () {
	this.fsm.start();
};

ReaperStream.prototype.setReapTime = function (v) {
	assert.number(v, 'reapTime');
	assert.ok(v > 0 && v < 24*3600);
	this.fsm.reapTime = v;
};
