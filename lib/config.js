function config(client) {
	this.client = client;
}

config.prototype.all = function(cb, eb) {
	return this.client._getconfig.call([], [], function(response) {
		cb(JSON.parse(response));
	}, eb);
}

config.prototype.get = function(key, cb, eb) {
	return this.client._getconfig.call([], [key], cb, eb);
}

config.prototype.set = function(key, value, cb, eb) {
	return this.client._setconfig.call([], [key, value], cb, eb);
}

config.prototype.unset = function(key, cb, eb) {
	return this.client._setconfig.call([], [key], cb, eb);
}

exports.config = config;