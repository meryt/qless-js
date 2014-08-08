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
        it('should allow named queue creation', function(done) {
            assert.notEqual(client.queue('foo'), undefined);
            done();
        })
        it('should create queues in an empty state', function(done) {
            client.queue('bar').put('FooJobClass', {}, {}, function() {
                client.queues('bar', function(result) {
                    var q = result;
                    assert.equal(q.name, 'bar');
                    assert.equal(q.scheduled, 0);
                    assert.equal(q.waiting, 1);
                    assert.equal(q.depends, 0);
                    assert.equal(q.running, 0);
                    assert.equal(q.stalled, 0);
                    assert.equal(q.recurring, 0);
                    assert.equal(q.paused, false);
                    done();
                })
            })
        })
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
        it('should include stats on all queues if none specified by name', function(done) {
            client.queue('bar').put('BarJob', {}, {}, function() {
                client.queue('baz').put('BazJob', {}, {}, function() {
                    client.queues(function(result) {
                        assert.equal(result.length, 3);
                        done();
                    })
                })
            })
        })
    })

})
