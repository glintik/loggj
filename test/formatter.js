/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
/*global describe: true, it:true*/

var assert = require('assert');

var EOL = require('os').EOL;

var rufus = require('../');

describe('Formatter', function() {
  it('should accept a format string', function() {
    var formatter = new rufus.Formatter('%level');
    assert.strictEqual(formatter._format, '%level');
  });

  it('should accept options', function() {
    var formatter = new rufus.Formatter({
      format: '%level'
    });

    assert.strictEqual(formatter._format, '%level');
  });

  it('should output an Error stack', function() {
    var formatter = new rufus.Formatter('%logger: %message%n%err');
    var e = new Error('boom');
    var msg = 'oh noes:';
    var name = 'foo';

    //similar produced with Logger.makeRecord
    var record = {
      name: name,
      message: msg,
      args: [msg],
      err: e
    };

    assert.strictEqual(formatter.format(record), name + ': ' + msg + EOL + e.stack + EOL);
  });

  it('should allow to set date format', function() {
    var formatter = new rufus.Formatter({
      format: '%date{%Y-%m}'
    });

    var d = new Date();
    var record = {
      timestamp: d
    };

    function pad(val) {
      if (val > 9) {
        return val;
      }
      return '0' + val;
    }
    var expected = d.getFullYear() + '-' + pad(d.getMonth() + 1);
    assert.strictEqual(formatter.format(record), expected);
  });

  it('should format as json', function() {
    var formatter = new rufus.Formatter({
      format: 'json'
    });

    var e = new Error('boom');

    let message = 'some message';
    var record = {
      name: 'some name',
      message,
      args: [message, 1, 'abc'],
      levelname: 'INFO',
      pid: 123,
      timestamp: new Date(),
      err: e,
    };

    let recordExpected = Object.assign({}, record, {
      err: e.stack,
      message: `${message} 1 abc`
    });
    delete recordExpected.args;

    assert.strictEqual(formatter.format(record), JSON.stringify(recordExpected) + EOL);
  });
});
