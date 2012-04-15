var fs    = require('fs')

/*
 * The Lua script reader / invoker
 */
function lua(name, redis) {
	this.name  = name;
	this.redis = redis;
	this.sha   = null;
}

lua.prototype.reload = function(callback) {
	data = fs.readFileSync('qless-core/' + this.name + '.lua')
	var l = this;
	this.redis.send_command('script', ['load', data], function(err, reply) {
		l.sha = reply;
		console.log('Loaded script: ' + l.sha);
		callback();
	});
}

lua.prototype.call = function(keys, args, cb, eb) {
	if (this.sha === null) {
		var l = this;
		this.reload(function() {
			l.call(keys, args, cb, eb);
		});
	} else {
		this.redis.send_command('evalsha', [this.sha, keys.length].concat(keys, args), function(err, reply) {
			err ? (eb || console.warn)(err) : (cb || function() {})(reply);
		});
	}
}

exports.lua = lua;