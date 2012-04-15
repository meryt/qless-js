var uuid = require('node-uuid');
var job  = require('./job').job;

function queue(name, client, worker) {
	this.name   = name;
	this.client = client;
	this.worker = worker;
}

queue.prototype.put = function(klass, data, options, cb, eb) {
	this.client._put.call([this.name], [
		options.jid || uuid.v1().replace(/-/g, ''),
		klass,
		JSON.stringify(data),
		(new Date().getTime() / 1000),
		options.delay || 0,
		'priority', options.priority || 0,
		'tags', JSON.stringify(options.tags || []),
		'retries', options.retries || 5,
		'depends', JSON.stringify(options.depends || [])
	], cb, eb)
}

queue.prototype.pop = function(count, cb, eb) {
	if (typeof(count) == 'function') {
		eb = cb; cb = count;
		this.client._pop.call([this.name], [this.worker, 1, (new Date().getTime() / 1000)], function(r) {
			cb(new job(JSON.parse(r)));
		}, eb);
	} else {
		this.client._pop.call([this.name], [this.worker, 1, (new Date().getTime() / 1000)], function(r) {
			var results = JSON.parse(r);
			for (var index in results) {
				results[index] = new job(results[index]);
			}
			cb(results);
		}, eb);
	}
}

queue.prototype.peek = function(count, cb, eb) {
	if (typeof(count) == 'function') {
		eb = cb; cb = count;
		this.client._peek.call([this.name], [1, (new Date().getTime() / 1000)], function(r) {
			cb(new job(JSON.parse(r)));
		}, eb);
	} else {
		this.client._peek.call([this.name], [1, (new Date().getTime() / 1000)], function(r) {
			var results = JSON.parse(r);
			for (var index in results) {
				results[index] = new job(results[index]);
			}
			cb(results);
		}, eb);
	}
}

queue.prototype.stats = function(date, cb, eb) {
	if (typeof(date) == 'function') {
		eb = cb; cb = date; date = (new Date().getTime() / 1000);
	}
	this.client._stats.call([], [this.name, date], function(r) {
		cb(JSON.parse(r));
	}, eb);
}

queue.prototype.running = function(offset, count, cb, eb) {
	if (typeof(offset) == 'function') {
		eb = count; cb = offset; offset = 0; count = 25;
	} else if (typeof(count) == 'function') {
		eb = cb; cb = count; count = 25;
	}
	this.client._jobs([], ['running', (new Date().getTime() / 1000), this.name, offset, count], cb, eb);
}

queue.prototype.stalled = function(offset, count, cb, eb) {
	if (typeof(offset) == 'function') {
		eb = count; cb = offset; offset = 0; count = 25;
	} else if (typeof(count) == 'function') {
		eb = cb; cb = count; count = 25;
	}
	this.client._jobs([], ['stalled', (new Date().getTime() / 1000), this.name, offset, count], cb, eb);
}

queue.prototype.scheduled = function(offset, count, cb, eb) {
	if (typeof(offset) == 'function') {
		eb = count; cb = offset; offset = 0; count = 25;
	} else if (typeof(count) == 'function') {
		eb = cb; cb = count; count = 25;
	}
	this.client._jobs([], ['scheduled', (new Date().getTime() / 1000), this.name, offset, count], cb, eb);
}

queue.prototype.depends = function(offset, count, cb, eb) {
	if (typeof(offset) == 'function') {
		eb = count; cb = offset; offset = 0; count = 25;
	} else if (typeof(count) == 'function') {
		eb = cb; cb = count; count = 25;
	}
	this.client._jobs([], ['depends', (new Date().getTime() / 1000), this.name, offset, count], cb, eb);
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

exports.queue = queue;