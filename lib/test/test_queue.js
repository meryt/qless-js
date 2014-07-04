var qless = require('../qless');
var assert = require('assert');

before(function() {
    client = new qless.client();
    client.redis.send_command('flushdb', []);
})
after(function() {
    client.redis.send_command('flushdb', []);
})

describe('queue initial counts:', function () {
    it('should have no depends', function(done) {
        client.queue('baz').depends(function(result) {
            assert.deepEqual(result, []);
            done();
        })
    })
    it('should have nothing scheduled', function(done) {
        client.queue('baz').scheduled(function(result) {
            assert.deepEqual(result, []);
            done();
        })
    })
    it('should have nothing running', function(done) {
        client.queue('baz').running(function(result) {
            assert.deepEqual(result, []);
            done();
        })
    })
    it('should have nothing stalled', function(done) {
        client.queue('baz').stalled(function(result) {
            assert.deepEqual(result, []);
            done();
        })
    })
    it('should have nothing recurring', function(done) {
        client.queue('baz').recurring(function(result) {
            assert.deepEqual(result, []);
            done();
        })
    })
})

describe('queues()', function() {
    it('should show correct counts after adding one job', function(done) {
        client.queue('foo').put('FooJob', {}, {}, function() {
            client.queues('foo', function(result) {
                var q = result;
                assert.equal(q.name, 'foo');
                assert.equal(q.paused, false);
                assert.equal(q.depends, 0);
                assert.equal(q.scheduled, 0);
                assert.equal(q.running, 0);
                assert.equal(q.stalled, 0);
                assert.equal(q.recurring, 0);
                assert.equal(q.waiting, 1);
                done();
            })
        })
    })
})