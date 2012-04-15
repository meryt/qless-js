/* External imports */
var os    = require('os');
var redis = require('redis');

/* Internal imports */
var lua    = require('./lua').lua;
var config = require('./config').config;
var queue  = require('./queue').queue;

function client(host, port, hostname) {
	this.redis  = redis.createClient(port || 6379, host || 'localhost');
	this.worker = hostname || os.hostname();
	this.config = new config(this);
	
	/* This is a reference to queues as they're lazily loaded */
	this.__queues = {}
	
	var commands = [
		'cancel', 'complete', 'depends', 'fail', 'failed', 'get', 'getconfig', 'heartbeat',
	    'jobs', 'peek', 'pop', 'put', 'queues', 'setconfig', 'stats', 'track', 'workers'];
	for (var index in commands) {
		this['_' + commands[index]] = new lua(commands[index], this.redis);
	}
}

client.prototype.queue = function(name) {
	if (!this.__queues[name]) {
		this.__queues[name] = new queue(name, this, this.worker);
	}
	return this.__queues[name];
}

client.prototype.queues = function(queue, cb, eb) {
	if (queue) {
		return this._queues.call([], [(new Date().getTime() / 1000), queue], function(r) {
			cb(JSON.parse(r));
		}, eb);
	} else {
		return this._queues.call([], [(new Date().getTime() / 1000)], function(r) {
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
	this._get.call([], [id], function(r) {
		if (r) {
			cb(new job(this, JSON.parse(r)));	
		} else {
			cb(null);
		}
	}, eb);
}    

exports.client = client;