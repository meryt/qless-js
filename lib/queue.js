var uuid = require('node-uuid');
var job  = require('./job').job;

function queue(name, client, worker) {
	this.name   = name;
	this.client = client;
	this.worker = worker;
}

queue.prototype.put = function(klass, data, options, cb, eb) {
	this.client._lua.call('put', [this.worker, this.name,
		options.jid || uuid.v1().replace(/-/g, ''),
		klass,
		JSON.stringify(data),
		options.delay || 0,
		'priority', options.priority || 0,
		'tags', JSON.stringify(options.tags || []),
		'retries', options.retries || 5,
		'depends', JSON.stringify(options.depends || [])
	], cb, eb)
}

/**
 * Pops jobs from the queue.
 *
 * If the optional count arg is specified, the cb will called with a list of
 * 0, 1, or more jobs, up to the number specified.
 *
 * If the optional count arg is omitted, the cb will be called either with
 * an empty object if the queue is empty, or else a single job object.
 */
queue.prototype.pop = function(count, cb, eb) {
	if (typeof(count) == 'function') {
		eb = cb; cb = count;
		var selfclient = this.client;
		this.client._lua.call('pop', [this.name, this.worker, 1], function(r) {
			var result = JSON.parse(r);
			if (result.length) {
				cb(new job(selfclient, result[0]));
			} else {
				cb({});
			}
		}, eb);
	} else {
		this.client._lua.call('pop', [this.name, this.worker, count], function(r) {
			var results = JSON.parse(r);
			for (var index in results) {
				results[index] = new job(selfclient, results[index]);
			}
			cb(results);
		}, eb);
	}
}

/**
 * Get a list of jobs on the queue without removing them from the queue.
 *
 * If the optional count arg is specified, the cb will called with a list of
 * 0, 1, or more jobs, up to the number specified.
 *
 * If the optional count arg is omitted, the cb will be called either with
 * an empty object if the queue is empty, or else a single job object.
 */
queue.prototype.peek = function(count, cb, eb) {
	if (typeof(count) == 'function') {
		eb = cb; cb = count;
		var selfclient = this.client;
		this.client._lua.call('peek', [this.name, 1], function(r) {
			var result = JSON.parse(r);
			if (result.length) {
				cb(new job(selfclient, result[0]));
			} else {
				cb({});
			}
		}, eb);
	} else {
		this.client._lua.call('peek', [this.name, count], function(r) {
			var results = JSON.parse(r);
			for (var index in results) {
				results[index] = new job(selfclient, results[index]);
			}
			cb(results);
		}, eb);
	}
}

queue.prototype.stats = function(date, cb, eb) {
	if (typeof(date) == 'function') {
		eb = cb; cb = date; date = (new Date().getTime() / 1000);
	}
	this.client._lua.call('stats', [this.name, date], function(r) {
		cb(JSON.parse(r));
	}, eb);
}

queue.prototype.running = function(offset, count, cb, eb) {
	if (typeof(offset) == 'function') {
		eb = count; cb = offset; offset = 0; count = 25;
	} else if (typeof(count) == 'function') {
		eb = cb; cb = count; count = 25;
	}
	this.client._lua.call('jobs', ['running', this.name, offset, count], cb, eb);
}

queue.prototype.stalled = function(offset, count, cb, eb) {
	if (typeof(offset) == 'function') {
		eb = count; cb = offset; offset = 0; count = 25;
	} else if (typeof(count) == 'function') {
		eb = cb; cb = count; count = 25;
	}
	this.client._lua.call('jobs', ['stalled', this.name, offset, count], cb, eb);
}

queue.prototype.scheduled = function(offset, count, cb, eb) {
	if (typeof(offset) == 'function') {
		eb = count; cb = offset; offset = 0; count = 25;
	} else if (typeof(count) == 'function') {
		eb = cb; cb = count; count = 25;
	}
	this.client._lua.call('jobs', ['scheduled', this.name, offset, count], cb, eb);
}

queue.prototype.depends = function(offset, count, cb, eb) {
	if (typeof(offset) == 'function') {
		eb = count; cb = offset; offset = 0; count = 25;
	} else if (typeof(count) == 'function') {
		eb = cb; cb = count; count = 25;
	}
	this.client._lua.call('jobs', ['depends', this.name, offset, count], cb, eb);
}

queue.prototype.recurring = function(offset, count, cb, eb) {
	if (typeof(offset) == 'function') {
		eb = count; cb = offset; offset = 0; count = 25;
	} else if (typeof(count) == 'function') {
		eb = cb; cb = count; count = 25;
	}
	this.client._lua.call('jobs', ['recurring', this.name, offset, count], cb, eb);
}

queue.prototype.length = function(cb, eb) {
	this.client.redis.multi()
		.zcard('ql:q:' + this.name + '-locks')
		.zcard('ql:q:' + this.name + '-work')
		.zcard('ql:q:' + this.name + '-scheduled')
		.exec(function(err, r) {
			err ? (eb || console.warn)(err) : (cb || function(){})(r[0] + r[1] + r[2]);
		});
}

queue.prototype.pause = function(cb, eb) {
	this.client._lua.call('pause', [this.name], cb, eb);
}

queue.prototype.unpause = function(cb, eb) {
	this.client._lua.call('unpause', [this.name], cb, eb);
}

queue.prototype.counts = function(cb, eb) {
	this.client.queues(this.name, cb, eb);
}

exports.queue = queue;