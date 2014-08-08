var qless = require('../qless');
var assert = require('assert');

before(function() {
    client = new qless.client();
    client.redis.send_command('flushdb', []);
});
after(function() {
    client.redis.send_command('flushdb', []);
});

assert.isEmptyList = function(foo) {
    return (Array.isArray(foo) && foo.length == 0);
}

describe('Job', function() {
    it('should initialize job with empty lists for tags, dependents, and dependencies', function(done) {
        client.queue('foo').put('FooJob', {'myprop':'myvalue'}, {'jid': 'myjid'}, function() {
            client.job('myjid', function(result) {
                assert.isEmptyList(result.dependencies);
                assert.isEmptyList(result.dependents);
                assert.isEmptyList(result.tags);

                done();
            })
        })
    })
})
