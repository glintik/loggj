'use strict';

var fs = require('fs');
var util = require('util');
var lockFile = require('lockfile');
var path = require('path');
var Promise = require('bluebird');

var stat = Promise.promisify(fs.stat);
var readdir = Promise.promisify(fs.readdir);

var compileFormat = require('../utils/compileFormat');

var FileHandler = require('./file');
var FileRemover = require('../utils/file-remover').FileRemover;

const DEBUG = false; // If true - log debug data to console
const ERROR = true; // If true - log errors to console
const maxSizeStatFileInterval = 100; // How frequently check file size for maxSize limit

function debug() {
    if (DEBUG) {
        let a = Array.prototype.slice.apply(arguments);
        a.unshift(`${new Date().toISOString()} [${process.pid}]`);
        console.log.apply(this, a);
    }
}

function error() {
    if (ERROR) {
        let a = Array.prototype.slice.apply(arguments);
        a.unshift(`${new Date().toISOString()} [ ${process.pid} ] ERROR: `);
        console.log.apply(this, a);
    }
}

function bytes(n) {
    var b = 0;

    var map = {
        b: 1,
        kb: 1 << 10,
        mb: 1 << 20,
        gb: 1 << 30
    };

    n.replace(/(\d+)(gb|mb|kb|b)/g, function (_, size, unit) {
        b += map[unit] * parseInt(size, 10);
        return _;
    });
    return b;
}

var timeRates = {
    yearly: function (prev) {
        //at the begining of next year
        return new Date(prev.getFullYear() + 1, 0);
    },
    monthly: function (prev) {
        //at then begining of next month
        return new Date(prev.getFullYear(), prev.getMonth() + 1);
    },
    weekly: function (prev) {
        //begining of next week (as 0 it is Sunday, so next week begins at sunday)
        return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate() + 7 - prev.getDay());
    },
    daily: function (prev) {
        //begining of next day
        return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate() + 1);
    },
    hourly: function (prev) {
        return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours() + 1);
    },
    everyminute: function (prev) {
        return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours(), prev.getMinutes() + 1);
    },
    everysecond: function (prev) {
        return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours(), prev.getMinutes(), prev.getSeconds() + 1);
    },
    every3seconds: function (prev) {
        return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours(), prev.getMinutes(), prev.getSeconds() + 3);
    }
};

var rotatePeriods = {
    yearly: function (prev) {
        return new Date(prev.getFullYear() - 1, 0);
    },
    monthly: function (prev) {
        return new Date(prev.getFullYear(), prev.getMonth() - 1);
    },
    weekly: function (prev) {
        //begining of next week (as 0 it is Sunday, so next week begins at sunday)
        return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate() - 7 + prev.getDay());
    },
    daily: function (prev) {
        //begining of next day
        return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate() - 1);
    },
    hourly: function (prev) {
        return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours() - 1);
    },
    everyminute: function (prev) {
        return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours(), prev.getMinutes() - 1);
    },
    everysecond: function (prev) {
        return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours(), prev.getMinutes(), prev.getSeconds() - 1);
    },
    every3seconds: function (prev) {
        return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours(), prev.getMinutes(), prev.getSeconds() - 3);
    }
};

function RotatingFileHandler(options) {
    FileHandler.call(this, options);
    this._buffer = [];

    if (typeof options.maxSize === 'string') {
        options.maxSize = bytes(options.maxSize);
    }
    if ('maxSize' in options) {
        this._maxSize = options.maxSize;
    }
    if ('timeRate' in options) {
        this._timeRate = options.timeRate;
        var that = this;
        this._rotatePeriod = rotatePeriods[this._timeRate] || function (prev) {
                return new Date(prev.getTime() - that._timeRate);
            };
        this._nextRotate = timeRates[this._timeRate] || function (prev) {
                return new Date(prev.getTime() + that._timeRate);
            };

    }

    this._oldFile = options.oldFile || this._fileFormat(this._file);
    this.fileNameFormat = compileFormat(this._oldFile, '%Y%m%d%H%M%S');

    if (options.maxFiles) {
        this._remover = new FileRemover({
            fileFormat: this._oldFile,
            defaultDateFormat: '%Y%m%d%H%M%S',
            keepFiles: options.maxFiles
        });
    }
    this._options = options;
}

util.inherits(RotatingFileHandler, FileHandler);

RotatingFileHandler.prototype.emit = function emit(record, callback) {
    var that = this;
    new Promise((resolve, reject) => {
        if (! that._nextRotateCheckTime || that._nextRotateCheckTime - Date.now() < 0) {
            that._nextRotateCheckTime = that._maxSize ? new Date(Date.now() + maxSizeStatFileInterval) : that._nextRotate(new Date());
            that.shouldRotate()
                .then(function (result) {
                    return result ? that.rotate() : Promise.fulfilled();
                }).then(function () {
                    if (that._isEnded) {
                        that.reopen();
                        that._isEnded = false;
                    }
                    resolve();
                });
        } else {
            resolve();
        }
    }).then(() => {
        that._write(that.format(record), callback);
    });
};

FileHandler.prototype.reopen = function reopen() {
    let that = this;
    this._birthtime = undefined;
    this._prevSize = undefined;
    return new Promise((resolve, reject) => {
        this._stream.end();
        that._stream = that._open();
        debug('Stream reopened');
        resolve();
    });
};

RotatingFileHandler.prototype._deleteOldFiles = function () {
    return this._remover ? this._remover.deleteOldFiles() : Promise.fulfilled();
};

RotatingFileHandler.prototype.shouldRotate = function () {
    var that = this;
    debug(`ShouldRotate check`);
    return this._getData().then(function (t) {
        if (!t) {
            return false;
        }
        if (that._timeRate){
            if (that._rotateAt == undefined) {
                that._rotateAt = that._nextRotate(new Date());
                that._rotateStartPeriod = that._rotatePeriod(that._rotateAt);
                that.reopen();
            }
            if (parseInt(that._rotateAt.getTime() / 1000) <= parseInt(nowTime / 1000)){
                debug(`Time exceeded: ${timeExceed}, _rotateAt: ${that._rotateAt.toISOString()}, nowTime: ${new Date(nowTime).toISOString()}, ${that._rotateAt.getTime() - nowTime}`);
                return true;
            }
        } else {
            if (t[0] > that._maxSize){
                debug(`Size exceeded: ${sizeExceed}, maxSize: ${that._maxSize}, already wrote: ${t[0]}`);
                return true;
            }
        }
        return false;
    }, () => {
        return false;
    });
};

RotatingFileHandler.prototype._getData = function () {
    let that = this;
    let size;
    if ((this._maxSize) || (this._timeRate)) {
        debug('Get file stat data');
        return stat(this._file).then(function (stat) {
            if (that._timeRate && that._rotateAt == undefined) {
                that._rotateAt = that._nextRotate(new Date());
                that._rotateStartPeriod = that._rotatePeriod(that._rotateAt);
            }
            debug(`that._birthtime: ${that._birthtime}, stat.birthtime: ${stat.birthtime}`);
            debug(`that._prevSize: ${that._prevSize}, stat.size: ${stat.size}`);
            if (that._birthtime == undefined) {
                that._birthtime = stat.birthtime;
            }
            if (that._prevSize == undefined) {
                that._prevSize = stat.size;
            }
            if (that._birthtime.getTime() != stat.birthtime.getTime() || that._prevSize > stat.size) {
                debug('Log rotated, reopen');
                that._birthtime = stat.birthtime;
                that._prevSize = stat.size;
                return that.reopen();
            }
            that._prevSize = stat.size;
            return [stat.size];
        }).catch((err) => {
            if (err.code == 'ENOENT') {
                debug('No file, reopening..');
                return that.reopen().then(that._getData());
            } else {
                error('_getData', err);
            }
        });
    } else {
        return Promise.rejected();
    }
};

RotatingFileHandler.prototype.rotate = function _rotate() {
    debug('  -- ROTATE ON ');
    var that = this;
    let fileRotate = that._file + ".rotate";
    return new Promise(function (resolve, reject) {
        debug('.Trying to lock');
        // retry wait is 10..50ms, 5 retries
        lockFile.lock(fileRotate, {stale: 1000, retries: 5, retryWait: Math.floor(Math.random() * 40) + 10}, function (err) {
            if (err) {
                debug('rotate reject');
                reject(err);
            } else {
                resolve();
            }
        });
    }).then(function () {
        debug('.Got lock on:', fileRotate);
        return stat(that._file).then(function (stat) {
            if (that._timeRate && stat.birthtime.getTime() - that._rotateAt.getTime() >= 0 ||
                that._maxSize && stat.size < that._maxSize) {
                debug('Already rotated, skipping');
                if (that._timeRate){
                    that._rotateAt = that._nextRotate(new Date());
                    that._rotateStartPeriod = that._rotatePeriod(that._rotateAt);
                }
                return that.reopen();
            }
            that._timeRate && debug(' -- rotating, birthtime:', stat.birthtime, ', rotateAt: ', that._rotateAt);
            that._isEnded = true;

            return (new Promise(function (resolve, reject) {
                that._stream.end(resolve);
            })).then(new Promise(function (resolve, reject) {
                resolve(that._timeRate ? that._renameByTime() : that._renameBySize());
            })).then(function () {
                that._stream = that._open();
                if (that._timeRate) {
                    that._rotateAt = that._nextRotate(new Date());
                    that._rotateStartPeriod = that._rotatePeriod(that._rotateAt);
                    return that._deleteOldFiles();
                } else {
                    return this;
                }
            }).then(function () {
                return that._getData();
            });
        }, (r) => {
            return Promise.reject(r);
        }).then(function () {
            debug('.Unlock file:', fileRotate);
            lockFile.unlockSync(fileRotate);
            debug('  -- ROTATE OK ');
            return Promise.fulfilled();
        });
    }, function (err) {
        debug('.Lock rejected');
        debug('  -- ROTATE OFF ');
        return that.reopen();
    });
};

RotatingFileHandler.prototype._fileFormat = function (file) {
    var name = file;
    if (this._timeRate) name += '-%d';
    if (this._maxSize) name += '.%i';
    return name;
};

RotatingFileHandler.prototype._write = function write(data, callback) {
    this._stream.write(data, callback);
};

RotatingFileHandler.prototype._renameByTime = function _renameByTime() {
    var name = this._file;
    var newName = this.fileNameFormat({timestamp: this._rotateStartPeriod});
    debug('_renameByTime ', name, 'to ', newName);
    fs.renameSync(name, newName);
    return Promise.fulfilled();
};

RotatingFileHandler.prototype._renameBySize = function _renameBySize() {
    var that = this;
    return readdir(path.dirname(this._file)).then((files) => {
        let m;
        let fileBasename = path.basename(this._file);
        let fileDirname = path.dirname(this._file);
        let unsortedList = [];
        for (let i = 0; i < files.length; i++) {
            if (files[i].indexOf(fileBasename) == 0 && (m = files[i].substr(fileBasename.length).match(/^\.?(\d{0,5})?$/))) {
                if (m[1] == undefined) {
                    m[1] = 0;
                }
                unsortedList.push({i: parseInt(m[1]), f: files[i]});
            }
        }
        if (unsortedList.length > 0) {
            let sortedList = unsortedList.sort((a, b) => {
                if (a.i > b.i) {
                    return 1;
                }
                if (a.i < b.i) {
                    return -1;
                }
                return 0;
            });
            for (let i = sortedList.length - 1; i >= 0; i--) {
                let filename = fileDirname + path.sep + sortedList[i].f;
                if (i > this._options.maxFiles) {
                    debug('delete ', filename);
                    fs.unlinkSync(filename);
                } else if (i < this._options.maxFiles) {
                    let newFilename = fileDirname + path.sep + fileBasename + "." + (sortedList[i].i + 1);
                    debug('_renameBySize ', filename, 'to ', newFilename);
                    fs.renameSync(filename, newFilename);
                }
                if (i == 0) {
                    that.reopen();
                }
            }
        }
        return Promise.fulfilled();
    });
};

module.exports = RotatingFileHandler;
