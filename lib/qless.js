/* External imports */
var os    = require('os');
var redis = require('redis');

/* Internal imports */
var lua    = require('./lua').lua;
var job    = require('./job').job;
var config = require('./config').config;
var queue  = require('./queue').queue;

function client(host, port, hostname) {
	this.redis  = redis.createClient(port || 6379, host || 'localhost');
	this.worker = (hostname || os.hostname()) + '-' + process.pid;
	this.config = new config(this);

	/* This is a reference to queues as they're lazily loaded */
	this.__queues = {}

	this._lua = new lua(this.redis);
}

client.prototype.queue = function(name) {
	if (!this.__queues[name]) {
		this.__queues[name] = new queue(name, this, this.worker);
	}
	return this.__queues[name];
}

client.prototype.queues = function(queue, cb, eb) {
	if (typeof(queue) == 'function') {
		eb = cb; cb = queue; queue = undefined;
	}
	if (queue) {
		return this._lua.call('queues', [queue], function(r) {
			cb(JSON.parse(r));
		}, eb);
	} else {
		return this._lua.call('queues', [], function(r) {
			cb(JSON.parse(r));
		}, eb);
	}
}

client.prototype.tracked = function(cb, eb) {
	return this._track.call([], [], function(r) {
		var results = JSON.parse(r);
		for (var index in results['jobs']) {
			results['jobs'][index] = new job(this, results['jobs'][index]);
		}
		cb(results);
	}, eb);
}

client.prototype.failed = function(group, start, limit, cb, eb) {
	if (typeof(group) == 'function') {
		eb = start; cb = group;
		group = null;
	} else if (typeof(start) == 'function') {
		eb = limit; cb = start;
		start = 0; limit = 25;
	} else if (typeof(limit) == 'function') {
		eb = cb; cb = limit;
		limit = 25;
	}
	if (group == null) {
		this._failed.call([], [], function(r) {
			cb(JSON.parse(r));
		}, eb);
	} else {
		this._failed.call([], [group, start, limit], function(r) {
			var results = JSON.parse(r);
			for (var index in results['jobs']) {
				results['jobs'][index] = new job(this, results['jobs'][index]);
			}
			cb(results);
		}, eb);
	}
}

client.prototype.workers = function(worker, cb, eb) {
	if (typeof(worker) == 'function') {
		eb = cb; cb = worker;
		this._workers.call([], [(new Date().getTime() / 1000)], function(r) {
			cb(JSON.parse(r));
		}, eb);
	} else {
		this._workers.call([], [(new Date().gettime() / 1000), worker], function(r) {
			cb(JSON.parse(r));
		}, eb);
	}
}

client.prototype.job = function(id, cb, eb) {
	var self = this;
	self._lua.call('get', [id], function(r) {
		var that = self;
		if (r) {
			cb(new job(that, JSON.parse(r)));
		} else {
			cb(null);
		}
	}, eb);
}

exports.client = client;
