var qless = require('../qless');
var assert = require('assert');

before(function() {
    client = new qless.client();
    client.redis.send_command('flushdb', []);
});
after(function() {
    client.redis.send_command('flushdb', []);
});


describe('Client', function() {
    describe('queues', function() {
        it('should start empty', function(done) {
            client.queues(function(result) {
                assert.deepEqual(result, {});
                done();
            }, function(err) { console.log(err); })
        })
    })
})

