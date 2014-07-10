var qless = require('../qless');
var assert = require('assert');

before(function() {
    client = new qless.client();
    client.redis.send_command('flushdb', []);
    client.worker = 'unit-test-worker';
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

describe('peek()', function() {
    it('without count param should return an empty object rather than list', function(done) {
        client.queue('boo').peek(function(result) {
            assert.deepEqual(result, {});
            done();
        })
    })
    it('should return the job we just added', function(done) {
        client.queue('boo').put('BooJob', {}, {jid: 'deadbeef'}, function() {
            client.queue('boo').peek(1, function(result) {
                assert.equal(result.length, 1);
                assert.equal(result[0].jid, 'deadbeef');
                done();
            })
        })
    })
    it('without count param should return an object rather than list', function(done) {
        client.queue('boo').peek(function(result) {
            assert.equal(result.jid, 'deadbeef');
            done();
        })
    })
    it('should return both jobs we added if we ask for two', function(done) {
        client.queue('boo').put('BooJob', {}, {jid: 'beadbead'}, function() {
            client.queue('boo').peek(2, function(result) {
                assert.equal(result.length, 2);
                var jids = result.map(function(it) { return it.jid; });
                assert(jids.indexOf('beadbead') >= 0);
                assert(jids.indexOf('deadbeef') >= 0);
                done();
            })
        })
    })
    it('should return two jobs we added if we ask for two, even if there are more', function(done) {
        client.queue('boo').put('BooJob', {}, {jid: 'beefdeed'}, function() {
            client.queue('boo').peek(2, function(result) {
                assert.equal(result.length, 2);
                done();
            })
        })
    })
})

describe('pop()', function() {
    it('without count param, empty queue should return an empty object rather than list', function(done) {
        client.queue('bob').pop(function(result) {
            assert.deepEqual(result, {});
            done();
        })
    })
    it('with count param, empty queue should return an empty list', function(done) {
        client.queue('bob').pop(1, function(result) {
            assert.deepEqual(result, []);
            done();
        })
    })
    it('should return the job we just added', function(done) {
        client.queue('bob').put('BobJob', {}, {jid: 'beebdeed'}, function() {
            client.queue('bob').pop(1, function(result) {
                assert.equal(result.length, 1);
                assert.equal(result[0].jid, 'beebdeed');
                done();
            })
        })
    })
    it('after popping only job, queue should be empty', function(done) {
        client.queue('bob').pop(1, function(result) {
            assert.deepEqual(result, []);
            done();
        })
    })
    it('without count param, non-empty queue should return an object rather than list', function(done) {
        client.queue('bob').put('BobJob', {}, {jid: 'bedddedd'}, function() {
            client.queue('bob').pop(1, function(result) {
                assert.equal(result.length, 1);
                assert.equal(result[0].jid, 'bedddedd');
                done();
            })
        })
    })
    it('should return two jobs we added if we ask for two, even if there are more', function(done) {
        client.queue('boo').put('BooJob', {}, {jid: 'beefdeed'}, function() {
            client.queue('boo').pop(2, function(result) {
                assert.equal(result.length, 2);
                done();
            })
        })
    })
    it('should return all the jobs on the queue without error, even if we ask for too many, leaving the queue empty', function(done) {
        client.queues('boo', function(result) {
            var q = result;
            var num_waiting = q.waiting;
            client.queue('boo').pop(num_waiting + 3, function(result) {
                assert.equal(result.length, num_waiting);
                client.queues('boo', function(result) {
                    assert.equal(result.waiting, 0);
                    done();
                })
            })
        })
    })
})

describe('stats()', function() {
    it('should return some stats', function(done) {
        client.queue('boo').stats(function(result) {
            // "retries" is one of the properties of the result object,
            // just check that an object is returned and has one of
            // the known properties.
            assert("retries" in result);
            done();
        })
    })
})

describe('pause() and unpause()', function() {
    it('should start unpaused', function(done) {
        client.queues('hello kitty', function(result) {
            var q = result;
            assert.equal(q.paused, false);
            done();
        })
    })
    it('should be paused after calling pause', function(done) {
        client.queue('hello kitty').pause(function(result) {
            client.queues('hello kitty', function (res) {
                assert(res.paused);
                done();
            })
        })
    })
    it('should be unpaused after calling unpause', function(done) {
        client.queue('hello kitty').unpause(function(result) {
            client.queues('hello kitty', function (res) {
                assert.equal(res.paused, false);
                done();
            })
        })
    })
})
