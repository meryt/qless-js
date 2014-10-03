var qless = require('../qless');
var qlessjob = require('../job');
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
    it('constructor should allow setting arbitrary properties', function(done) {
        myjob = new qlessjob.job(client, {'colour': 'pink', 'flavour': 'cotton candy'});
        assert.equal(myjob.colour, 'pink');
        assert.equal(myjob.flavour, 'cotton candy');
        done();
    })

    describe('fail()', function() {
        it('should fail the job', function(done) {
            client.queue('foo').put('FooJob', {}, {"jid": "my2jid"}, function() {
                client.queue('foo').pop(function(result) {
                    result.fail('mygroup', 'failed because of reasons', function(result2) {
                        client.failed(function(result3) {
                            assert(typeof(result3) != 'undefined');
                            done();
                        })
                    })
                })
            })
        })
    })

    describe('complete()', function() {
        it('should complete the job', function(done) {
            client.queue('foo').put('FooJob', {}, {"jid": "my3jid"}, function() {
                client.queue('foo').pop(function(result) {
                    result.complete(function(result2) {
                        // TODO how to verify it was marked as complete
                        done();
                    })
                })
            })
        })
    })

})
