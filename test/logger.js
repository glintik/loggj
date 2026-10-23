/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

var { describe, it, mock } = require('node:test');
var assert = require('assert');
var execFile = require('child_process').execFile;
var path = require('path');

var rufus = require('../');
var Logger = rufus.Logger;

var __counter = 1;
function unique() {
  return "u-" + __counter++;
}

function spyHandler(level, callback) {
  if(typeof level !== 'number') level = 0;

  var stub = mock.fn(callback);

  return {
    handle: stub,
    level: level
  };
}

describe('Logger', function() {
  it('should return the same logger when created with the same name', function() {
    var name = unique();

    var logger1 = new Logger(name);
    var logger2 = new Logger(name);

    assert.strictEqual(logger1, logger2);
  });

  it('should allow add handlers and remove them', function() {
    var logger = new Logger(unique());
    var handler = new rufus.handlers.Null();

    logger.addHandler(handler);
    assert.strictEqual(logger.getHandlers().length, 1);

    logger.removeHandler(handler);
    assert.strictEqual(logger.getHandlers().length, 0);
  });

  it('should set by default for child logger log level of its direct parent', function() {
    var parentName = unique();
    var parent = new Logger(parentName);
    parent.setLevel('error');

    var childName = parentName + '.' + unique();
    var child = new Logger(childName);

    assert.strictEqual(child.getEffectiveLevel(), rufus.ERROR);
  });

  it('should make record', function() {
    var name = unique();
    var logger = new Logger(name);

    var msg = 'foo';
    var level = rufus.DEBUG;
    var err = new Error();
    var args = [ msg, err ];
    var record = logger.makeRecord(name, 'DEBUG', msg, args.slice());

    assert.strictEqual(record.name, name);
    assert.strictEqual(record.level, level);
    assert.strictEqual(record.message, msg);
    assert.strictEqual(record.levelname, 'DEBUG');
    assert.strictEqual(record.pid, process.pid);
    assert.strictEqual(record.args.length, args.length - 1); //it is because last arg is Error
    assert.strictEqual(record.args[0], msg);
    assert.strictEqual(record.err, err);
  });

  it('should take Error subclass instance as record error', function() {
    var name = unique();
    var logger = new Logger(name);

    class MyError extends Error {}
    var err = new MyError('boom');
    var record = logger.makeRecord(name, 'ERROR', 'foo', ['foo', err]);

    assert.strictEqual(record.err, err);
    assert.deepStrictEqual(record.args, ['foo']);
  });

  it('should take non-native Error instance as record error', function() {
    var name = unique();
    var logger = new Logger(name);

    var err = Object.create(Error.prototype);
    var record = logger.makeRecord(name, 'ERROR', 'foo', ['foo', err]);

    assert.strictEqual(record.err, err);
    assert.deepStrictEqual(record.args, ['foo']);
  });

  it('should not take error-like object or not last Error as record error', function() {
    var name = unique();
    var logger = new Logger(name);

    var errorLike = { message: 'boom', stack: 'stack' };
    var record = logger.makeRecord(name, 'ERROR', 'foo', ['foo', errorLike]);
    assert.strictEqual(record.err, undefined);
    assert.deepStrictEqual(record.args, ['foo', errorLike]);

    var err = new Error('boom');
    record = logger.makeRecord(name, 'ERROR', 'foo', [err, 'foo']);
    assert.strictEqual(record.err, undefined);
    assert.deepStrictEqual(record.args, [err, 'foo']);
  });

  /*it('should call its handlers', function() {
    var logger = new Logger(unique());
    logger.propagate = false;

    var handler = spyHandler();
    logger.addHandler(handler);

    logger.log(rufus.INFO, 'foo');
    assert.ok(handler.handle.mock.callCount() > 0);

    logger.info('foo');
    assert.strictEqual(handler.handle.mock.callCount(), 2);
  });*/

  it('should call parent handlers when propagate is true', function() {
    var parentName = unique();
    var parent = new Logger(parentName);
    parent.propagate = false; //stop there

    var childName = parentName + '.' + unique();
    var child = new Logger(childName);

    var parentHandler = spyHandler();
    var childHandler = spyHandler();

    parent.addHandler(parentHandler);
    child.addHandler(childHandler);

    child.info('foo');

    assert.strictEqual(childHandler.handle.mock.callCount(), 1);
    assert.strictEqual(parentHandler.handle.mock.callCount(), 1);
  });

  it('should call handlers which level is more then record level', function() {
    var logger = new Logger(unique());
    logger.propagate = false;

    var handlerError = spyHandler(rufus.ERROR);
    var handlerTrace = spyHandler(rufus.TRACE);

    logger.addHandler(handlerError).addHandler(handlerTrace);

    logger.info('foo');

    assert.strictEqual(handlerTrace.handle.mock.callCount(), 1);
    assert.strictEqual(handlerError.handle.mock.callCount(), 0);
  });

  it('should call handlers when record level more or eql logger level', function() {
    var logger = new Logger(unique());
    logger.propagate = false;
    logger.setLevel(rufus.INFO);

    var handler = spyHandler();
    logger.addHandler(handler);

    logger.info('foo');
    logger.error('foo');
    logger.trace('foo');

    assert.strictEqual(handler.handle.mock.callCount(), 2);
  });

  it('should allow to filter log records', function() {
    var logger = new Logger(unique());
    logger.propagate = false;

    var handler = spyHandler();
    logger.addHandler(handler);

    logger.addFilter(rufus.makeFilter(/^foo/));

    // message not pass filter
    logger.info('bar');
    assert.strictEqual(handler.handle.mock.callCount(), 0);

    logger.info('fooooo');
    assert.strictEqual(handler.handle.mock.callCount(), 1);
  });

  it('should return promise that rejects when one of handlers fail', function() {
    var logger = new Logger(unique());
    logger.propagate = false;

    var h = new rufus.handlers.Null();
    h.emit = function(record, callback) {
      throw new Error('foo');
    };

    logger.addHandler(h);

    return assert.rejects(logger.info('bar'), { message: 'foo' });
  });
});

function runErrorScript(args) {
  var script = path.join(__dirname, 'util', 'error.js');
  return new Promise(function(resolve) {
    execFile(process.execPath, [script].concat(args), function(err, stdout, stderr) {
      resolve({ code: err ? err.code : 0, stdout: stdout, stderr: stderr });
    });
  });
}

describe('Logger handleExceptions', function() {
  it('should catch uncaughtErrors and exit', async function() {
    var res = await runErrorScript([]);
    assert.strictEqual(res.stderr, 'root.ERROR - Uncaught exception handled');
    assert.strictEqual(res.stdout, '');
    assert.strictEqual(res.code, 1);
  });

  it('should not exit if exitOnError is false', async function() {
    var res = await runErrorScript(['--noexit']);
    assert.strictEqual(res.stderr, 'root.ERROR - Uncaught exception handled');
    assert.strictEqual(res.stdout, 'root.INFO - noexit');
    assert.strictEqual(res.code, 0);
  });
});
