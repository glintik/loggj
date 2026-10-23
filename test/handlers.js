/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

var { describe, it } = require('node:test');
var assert = require('assert');
var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var os = require('os');
var path = require('path');

var rufus = require('../');

var NOW = Date.now();
var counter = 1;
function tmp() {
  return path.join(os.tmpdir(),
    'rufus-' + NOW + '-' + process.pid + '-' + (counter++));
}

function rec(msg, level) {
  return { message: msg, args: [msg], timestamp: new Date() };
}

function bytes(x) {
  var b = Buffer.alloc(x);
  b[0] = '<'.charCodeAt(0);
  b[b.length - 1] = '>'.charCodeAt(0);
  b.fill('a', 1, b.length - 1);
  return b.toString();
}

describe('Handler', function () {
  describe('constructor', function () {
    it('should accept options', function () {
      var h = new rufus.Handler({ level: rufus.ERROR });
      assert.strictEqual(h.level, rufus.ERROR);
    });

    it('should accept a level', function () {
      var h = new rufus.Handler(rufus.WARN);
      assert.strictEqual(h.level, rufus.WARN);
    });
  });

  describe('handle', function () {
    it('requires emit to accept a callback argument', function () {
      var h = new rufus.Handler();
      h.emit = function () {
      };

      assert.throws(h.handle.bind(h), function (err) {
        return err.message === 'Handler.emit requires a callback argument';
      });

      h = new rufus.Handler();
      h.emit = function (record, callback) {
        record = callback;
      };
      assert.doesNotThrow(h.handle.bind(h));
    });

    it('should use filters on record', function () {
      var h = new rufus.Handler();
      var lastRecord;
      h.emit = function (record, callback) {
        lastRecord = record;
        callback();
      };

      h.addFilter(rufus.makeFilter('foo'));
      return h.handle({ name: 'foo' }).then(function () {
        assert.strictEqual(lastRecord.name, 'foo');

        return h.handle({ name: 'foobar' });
      }).then(function () {
          assert.notEqual(lastRecord.name, 'foobar');
        });
    });
  });

  describe('emit', function () {
    it('must be overriden by subclasses', function () {
      var h = new rufus.Handler();
      assert.throws(h.emit);
    });
  });
});

describe('Stream', function () {
  describe('constructor', function () {
    it('should accept options', function () {
      var stream = {};
      var handler = new rufus.handlers.Stream({
        level: rufus.INFO,
        stream: stream
      });

      assert.strictEqual(handler.level, rufus.INFO);
      assert.strictEqual(handler._stream, stream);
    });

    it('should accept just a stream', function () {
      var stream = {};
      var handler = new rufus.handlers.Stream(stream);

      assert.strictEqual(handler.level, rufus.NOTSET);
      assert.strictEqual(handler._stream, stream);
    });
  });

  describe('emit', function () {
    it('should write message to stream', function () {
      var out;
      var stream = {
        write: function (msg, fn) {
          out = msg;
          fn();
        }
      };

      var handler = new rufus.handlers.Stream({
        stream: stream,
        formatter: new rufus.Formatter('%message%n')
      });
      return handler.handle({ message: 'foo', args: ['foo'] }).then(function () {
        assert.strictEqual(out, 'foo\n');
      });
    });

    it('should wait for flush on slow streams', function () {
      var out;
      var stream = new EventEmitter();
      stream.write = function write(data, fn) {
        setTimeout(function () {
          out = data;
          fn();
        }, 1);
      };
      var handler = new rufus.handlers.Stream({
        stream: stream,
        formatter: new rufus.Formatter('%message%n')
      });
      return handler.handle({ message: 'secret', args: ['secret'] }).then(function () {
        assert.strictEqual(out, 'secret\n');
      });
    });
  });
});

describe('File', function () {
  describe('constructor', function () {
    it('should accept options', function () {
      var filename = tmp();
      var handler = new rufus.handlers.File({
        level: rufus.WARN,
        file: filename
      });

      assert.strictEqual(handler.level, rufus.WARN);
      assert.strictEqual(handler._file, filename);
    });

    it('should accept a filename', function () {
      var filename = tmp();
      var handler = new rufus.handlers.File(filename);

      assert.strictEqual(handler._file, filename);
    });
  });

  describe('handle', function () {
    it('should write to the file', function () {
      var filename = tmp();
      var handler = new rufus.handlers.File({
        file: filename,
        formatter: new rufus.Formatter('%message%n')
      });
      return handler.handle({ message: 'recon', args: ['recon'] }).then(function () {
        assert.equal(fs.readFileSync(filename).toString(), 'recon\n');
      });
    });
  });
});

describe('Console', function () {
  describe('constructor', function () {
    it('should use stdout and stderr', function () {
      var h = new rufus.handlers.Console();
      assert.strictEqual(h._out._stream, process.stdout);
      assert.strictEqual(h._err._stream, process.stderr);
    });
  });

  describe('handle', function () {
    it('should send low priority messages to stdout', function () {
      var h = new rufus.handlers.Console({
        formatter: new rufus.Formatter('%message%n'),
        colorize: false
      });
      var val;
      h._out._stream = {
        write: function (out, callback) {
          val = out;
          callback();
          return true;
        }
      };

      return h.handle({ level: rufus.INFO, message: 'oscar mike', args: ['oscar mike'] }).then(function () {
        assert.strictEqual(val, 'oscar mike\n');
      });
    });

    it('should send warn and higher messages to stderr', function () {
      var h = new rufus.handlers.Console({
        formatter: new rufus.Formatter('%message%n'),
        colorize: false
      });
      var val;
      h._err._stream = {
        write: function (out, callback) {
          val = out;
          callback();
          return true;
        }
      };

      return h.handle({ level: rufus.WARN, message: 'mayday', args: ['mayday'] }).then(function () {
        assert.strictEqual(val, 'mayday\n');
      });
    });

    it('should colorize messages by level', async function () {
      var h = new rufus.handlers.Console({
        formatter: new rufus.Formatter('%message%n')
      });
      var out = [];
      h._out._stream = h._err._stream = {
        write: function (data, callback) {
          out.push(data);
          callback();
          return true;
        }
      };

      var expected = [
        [rufus.VERBOSE, '\u001b[36mmsg\n\u001b[39m'],
        [rufus.DEBUG, '\u001b[34mmsg\n\u001b[39m'],
        [rufus.INFO, '\u001b[32mmsg\n\u001b[39m'],
        [rufus.WARN, '\u001b[33mmsg\n\u001b[39m'],
        [rufus.ERROR, '\u001b[31mmsg\n\u001b[39m'],
        [rufus.CRITICAL, '\u001b[35mmsg\n\u001b[39m']
      ];
      for (var e of expected) {
        await h.handle({ level: e[0], message: 'msg', args: ['msg'] });
      }
      assert.deepStrictEqual(out, expected.map(function (e) { return e[1]; }));
    });

    it('should keep colors of colored message parts', async function () {
      var h = new rufus.handlers.Console({
        formatter: new rufus.Formatter('%message')
      });
      var val;
      h._out._stream = {
        write: function (data, callback) {
          val = data;
          callback();
          return true;
        }
      };

      var msg = 'a \u001b[31mred\u001b[39m b';
      await h.handle({ level: rufus.INFO, message: msg, args: [msg] });
      assert.strictEqual(val, '\u001b[32ma \u001b[31mred\u001b[32m b\u001b[39m');
    });

    it('should not colorize empty output', async function () {
      var h = new rufus.handlers.Console({
        formatter: new rufus.Formatter('%message')
      });
      var val;
      h._out._stream = {
        write: function (data, callback) {
          val = data;
          callback();
          return true;
        }
      };

      await h.handle({ level: rufus.INFO, message: '', args: [''] });
      assert.strictEqual(val, '');
    });

    it('should skip output with ttyonly when stdout is not a TTY', async function () {
      var h = new rufus.handlers.Console({ ttyonly: true });
      var written = false;
      h._out._stream = h._err._stream = {
        write: function (data, callback) {
          written = true;
          callback();
          return true;
        }
      };

      var isTTY = process.stdout.isTTY;
      process.stdout.isTTY = false;
      try {
        await h.handle({ level: rufus.INFO, message: 'msg', args: ['msg'] });
        await h.handle({ level: rufus.ERROR, message: 'msg', args: ['msg'] });
      } finally {
        process.stdout.isTTY = isTTY;
      }
      assert.strictEqual(written, false);
    });
  });
});

describe('RotatingFileHandler', function () {
  function sleep(ms) {
    return new Promise(function (resolve) {
      setTimeout(resolve, ms);
    });
  }

  function tmpDir() {
    return fs.mkdtempSync(path.join(os.tmpdir(), 'rufus-rotating-'));
  }

  function list(dir) {
    return fs.readdirSync(dir).sort();
  }

  function contents(dir) {
    return list(dir).map(function (f) {
      return fs.readFileSync(path.join(dir, f)).toString();
    }).join('');
  }

  // file size is checked not often than once per 50ms, so writes are spread in time
  async function write(handler, sizes) {
    for (var size of sizes) {
      await handler.handle(rec(bytes(size)));
      await sleep(100);
    }
  }

  async function close(handler, dir) {
    clearTimeout(handler._rotateTimeout);
    await sleep(100);
    clearTimeout(handler._rotateTimeout);
    handler._stream.end();
    fs.rmSync(dir, { recursive: true, force: true });
  }

  it('with maxSize should create new files', { timeout: 10000 }, async function () {
    var dir = tmpDir();
    var filename = path.join(dir, 'app.log');
    var handler = new rufus.handlers.Rotating({
      file: filename,
      maxSize: 64,
      maxFiles: 10,
      formatter: new rufus.Formatter('%message%n')
    });

    assert.strictEqual(handler._file, filename);
    var sizes = [30, 40, 50, 60, 45];
    await write(handler, sizes);

    var files = list(dir);
    assert.ok(files.includes('app.log.1'), files.join());
    assert.ok(files.every(function (f) { return /^app\.log(\.\d+)?$/.test(f); }), files.join());
    // nothing lost or broken
    var written = sizes.map(function (size) { return bytes(size) + '\n'; });
    assert.deepStrictEqual(contents(dir).split('\n').slice(0, -1).sort(),
      written.map(function (r) { return r.slice(0, -1); }).sort());

    await close(handler, dir);
  });

  it('with maxFiles should not keep more than max old files', { timeout: 10000 }, async function () {
    var dir = tmpDir();
    var filename = path.join(dir, 'app.log');
    var handler = new rufus.handlers.Rotating({
      file: filename,
      maxSize: 64,
      maxFiles: 3,
      formatter: new rufus.Formatter('%message%n')
    });

    await write(handler, [50, 50, 50, 50, 50, 50, 50, 50, 50, 50]);

    assert.deepStrictEqual(list(dir), ['app.log', 'app.log.1', 'app.log.2', 'app.log.3']);
    contents(dir).split('\n').slice(0, -1).forEach(function (line) {
      assert.strictEqual(line, bytes(50));
    });

    await close(handler, dir);
  });

  it('should continue to write to the main file after rotation', { timeout: 10000 }, async function () {
    var dir = tmpDir();
    var filename = path.join(dir, 'app.log');
    var handler = new rufus.handlers.Rotating({
      file: filename,
      maxSize: 64,
      formatter: new rufus.Formatter('%message%n')
    });

    for (var i = 0; i < 10 && !fs.existsSync(filename + '.1'); i++) {
      await write(handler, [40]);
    }
    assert.ok(fs.existsSync(filename + '.1'));

    await write(handler, [10]);
    assert.strictEqual(fs.readFileSync(filename).toString(), bytes(10) + '\n');

    await close(handler, dir);
  });

  it('should recreate removed file', { timeout: 10000 }, async function () {
    var dir = tmpDir();
    var filename = path.join(dir, 'app.log');
    var handler = new rufus.handlers.Rotating({
      file: filename,
      maxSize: 64,
      formatter: new rufus.Formatter('%message%n')
    });

    await write(handler, [10]);
    fs.unlinkSync(filename);

    assert.deepStrictEqual(await handler._getData(), [0]);
    assert.ok(fs.existsSync(filename));

    await write(handler, [20]);
    assert.strictEqual(fs.readFileSync(filename).toString(), bytes(20) + '\n');

    await close(handler, dir);
  });

  it('with time rule should rotate files by time', { timeout: 10000 }, async function () {
    var dir = tmpDir();
    var filename = path.join(dir, 'app.log');
    var handler = new rufus.handlers.Rotating({
      file: filename,
      rule: 'time',
      timeRate: 'everysecond',
      maxFiles: 2,
      formatter: new rufus.Formatter('%message%n')
    });

    for (var i = 0; i < 5; i++) {
      await write(handler, [20]);
      await sleep(600);
    }
    clearTimeout(handler._rotateTimeout);
    await sleep(100);

    var files = list(dir);
    var archives = files.filter(function (f) { return f !== 'app.log'; });
    assert.ok(files.includes('app.log'), files.join());
    assert.ok(archives.length >= 1 && archives.length <= 2, files.join());
    archives.forEach(function (f) {
      assert.match(f, /^app\.log-\d{14}$/);
    });

    await close(handler, dir);
  });
});
