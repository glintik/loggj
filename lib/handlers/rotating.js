'use strict';

const fs = require('fs').promises;
var util = require('util');
var lockFile = require('lockfile');
var path = require('path');

var stat = fs.stat;
var readdir = fs.readdir;

var compileFormat = require('../utils/compileFormat');

var FileHandler = require('./file');
var FileRemover = require('../utils/file-remover').FileRemover;

const DEBUG = false; // If true - log debug data to console
const ERROR = true; // If true - log errors to console
const maxSizeStatFileInterval = 50; // How frequently check file size for maxSize limit

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

var rotates = {
    yearly: {
        timeRate: function (prev) {
            //at the begining of next year
            return new Date(prev.getFullYear() + 1, 0);
        },
        rotatePeriod: function (prev) {
            //at the begining of next year
            return new Date(prev.getFullYear() - 1, 0);
        },
        format: '%Y'
    },
    monthly: {
        timeRate: function (prev) {
            //at then begining of next month
            return new Date(prev.getFullYear(), prev.getMonth() + 1);
        },
        rotatePeriod: function (prev) {
            //at then begining of next month
            return new Date(prev.getFullYear(), prev.getMonth() - 1);
        },
        format: '%Y%m'
    },
    weekly: {
        timeRate: function (prev) {
            //begining of next week (as 0 it is Sunday, so next week begins at sunday)
            return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate() + 7 - prev.getDay());
        },
        rotatePeriod: function (prev) {
            //begining of next week (as 0 it is Sunday, so next week begins at sunday)
            return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate() - 7 + prev.getDay());
        },
        format: '%Y%m%d'
    },
    daily: {
        timeRate: function (prev) {
            //begining of next day
            return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate() + 1);
        },
        rotatePeriod: function (prev) {
            return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate() - 1);
        },
        format: '%Y%m%d'
    },
    hourly: {
        timeRate: function (prev) {
            return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours() + 1);
        },
        rotatePeriod: function (prev) {
            return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours() - 1);
        },
        format: '%Y%m%d%H'
    },
    everyminute: {
        timeRate: function (prev) {
            return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours(), prev.getMinutes() + 1);
        },
        rotatePeriod: function (prev) {
            return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours(), prev.getMinutes() - 1);
        },
        format: '%Y%m%d%H%M'
    },
    everysecond: {
        timeRate: function (prev) {
            return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours(), prev.getMinutes(), prev.getSeconds() + 1);
        },
        rotatePeriod: function (prev) {
            return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours(), prev.getMinutes(), prev.getSeconds() - 1);
        },
        format: '%Y%m%d%H%M%S'
    },
    every3seconds: {
        timeRate: function (prev) {
            return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours(), prev.getMinutes(), prev.getSeconds() + 3);
        },
        rotatePeriod: function (prev) {
            return new Date(prev.getFullYear(), prev.getMonth(), prev.getDate(), prev.getHours(), prev.getMinutes(), prev.getSeconds() - 3);
        },
        format: '%Y%m%d%H%M%S'
    }
};

function RotatingFileHandler(options) {
    FileHandler.call(this, options);
    this.isRuleSize = function () {
        return this._rule == 'size';
    };

    this.isRuleTime = function () {
        return this._rule == 'time';
    };

    var that = this;

    if (typeof options.maxSize === 'string') {
        options.maxSize = bytes(options.maxSize);
    }
    if ('maxSize' in options) {
        this._maxSize = options.maxSize;
    }
    this._rule = options.rule || 'size';
    this._oldFile = options.oldFile || this._fileFormat(this._file);

    this._options = options;

    this._setRotateTimeout = function () {
        let timeoutValue = that._nextRotate(new Date).getTime() - Date.now();
        if (timeoutValue > 0 && timeoutValue <= 2147483647){
            debug(that._file, 'Set timeout for rotate:', timeoutValue);
            that._rotateTimeout = setTimeout(() => {
                debug(that._file, 'Rotate by timeout');
                that.doRotate.call(that, true, that._setRotateTimeout);
            }, timeoutValue).unref();
        }
    };

    let defaultDateFormat = '%Y%m%d%H%M%S';
    if (this.isRuleTime()) {
        this._timeRate = options.timeRate;
        this._rotatePeriod = rotates[this._timeRate].rotatePeriod || function (prev) {
                return new Date(prev.getTime() - that._timeRate);
            };
        this._nextRotate = rotates[this._timeRate].timeRate || function (prev) {
                return new Date(prev.getTime() + that._timeRate);
            };
        if (rotates[this._timeRate].format){
            defaultDateFormat = rotates[this._timeRate].format;
        }

        this.fileNameFormat = compileFormat(this._oldFile, defaultDateFormat);
        that._rotateAt = that._nextRotate(new Date());
        that._rotateStartPeriod = that._rotatePeriod(that._rotateAt);

        this._setRotateTimeout.call(that);
    }

    if (options.maxFiles) {
        this._remover = new FileRemover({
            fileFormat: this._oldFile,
            defaultDateFormat: defaultDateFormat,
            keepFiles: options.maxFiles
        });
    }
}

util.inherits(RotatingFileHandler, FileHandler);

RotatingFileHandler.prototype.emit = function emit(record, callback) {
    var that = this;
    this._write(that.format(record), callback);
    new Promise((resolve, reject) => {
        if (this.isRuleSize() && (! that._nextRotateCheckTime || that._nextRotateCheckTime - Date.now() < 0)) {
            that._nextRotateCheckTime = new Date(Date.now() + maxSizeStatFileInterval);
            that.reopen().then(() => {
                that.doRotate(false, resolve);
            }, () => {
                return Promise.resolve();
            });
        } else {
            resolve();
        }
    }).catch((err) => {
        error('Write error', err);
        if (callback){
            callback();
        }
    });
};

FileHandler.prototype.doRotate = function (force, cb) {
    var that = this;
    that.shouldRotate(force).then(function (result) {
        if (result) {
            // We need to start rotating now
            if (!that._rotatingNow) {
                that._rotatingNow = true;
                return that.rotate().then(() => {
                    that._rotatingNow = false;
                    return Promise.resolve();
                });
            } else {
                debug('Rotating now already, skipping..');
            }
        }
        return Promise.resolve();
    }).then(function () {
        if (cb) { cb(); }
    }).catch((err) => {
        error('Error in doRotate: ', err);
        if (cb) { cb(); }
    });
};

FileHandler.prototype.reopen = function reopen() {
    let that = this;
    this._prevCheckTime = undefined;
    this._prevSize = undefined;
    return new Promise((resolve, reject) => {
        that._stream.end();
        that._stream = that._open();
        debug('Stream reopened');
        resolve();
    });
};

RotatingFileHandler.prototype._deleteOldFiles = function () {
    return this._remover ? this._remover.deleteOldFiles() : Promise.resolve();
};

RotatingFileHandler.prototype.shouldRotate = function (force) {
    var that = this;
    debug(this._file, `ShouldRotate check`);
    return this._getData().then(function (t) {
        if (!t) {
            return false;
        }
        let nowTime = Date.now();
        if (that.isRuleTime()){
            if (that._rotateAt == undefined) {
                that._rotateAt = that._nextRotate(new Date());
                that._rotateStartPeriod = that._rotatePeriod(that._rotateAt);
            }
            debug(`that._prevCheckTime: ${that._prevCheckTime}, that._rotateStartPeriod: ${that._rotateStartPeriod.toISOString()}, _rotateAt: ${that._rotateAt.toISOString()}, nowTime: ${new Date(nowTime).toISOString()}, ${that._rotateAt.getTime() - nowTime}`);

            if (force){
                debug('Forced rotate');
                return true;
            }
            if (parseInt(that._rotateAt.getTime() / 1000) <= parseInt(nowTime / 1000)){
                debug(`Time exceeded, _rotateAt: ${that._rotateAt.toISOString()}, nowTime: ${new Date(nowTime).toISOString()}, ${that._rotateAt.getTime() - nowTime}`);
                return true;
            } else if (parseInt(that._prevCheckTime.getTime() / 1000) < parseInt(that._rotateStartPeriod.getTime() / 1000)){
                debug(`Old file ${that._file}, need rotate, that._prevCheckTime: ${that._prevCheckTime}, that._rotateStartPeriod: ${that._rotateStartPeriod.toISOString()}`);
                return true;
            }
        } else {
            if (t[0] > that._maxSize){
                debug(`Size exceeded, maxSize: ${that._maxSize}, already wrote: ${t[0]}`);
                return true;
            }
        }
        return false;
    }, (err) => {
        error('Error in shouldRotate:', err);
        return false;
    });
};

RotatingFileHandler.prototype._getData = function () {
    let that = this;
    let size;
    debug('Get file stat data');
    return stat(this._file).then(function (stat) {
        if (that.isRuleTime() && that._rotateAt == undefined) {
            that._rotateAt = that._nextRotate(new Date());
            that._rotateStartPeriod = that._rotatePeriod(that._rotateAt);
        }
        if (that._prevCheckTime == undefined) {
            that._prevCheckTime = new Date();
        }
        debug(`that._prevCheckTime: ${that._prevCheckTime}`);
        debug(`that._prevSize: ${that._prevSize}, stat.size: ${stat.size}`);
        if (that._prevSize == undefined) {
            that._prevSize = stat.size;
        }
        if (that.isRuleSize()){
            if (that._prevSize > stat.size) {
                that._prevSize = stat.size;
                debug('Log rotated, reopen');
                return that.reopen();
            }
        }
        that._prevSize = stat.size;
        return Promise.resolve([stat.size]);
    }).catch((err) => {
        if (err.code == 'ENOENT') {
            debug('No file, reopening..');
            return that.reopen().then(that._getData());
        } else {
            error('_getData', err);
            return that.reopen();
        }
    });
};

RotatingFileHandler.prototype.rotate = function () {
    debug('  -- ROTATE ON ');
    var that = this;
    let fileRotate = that._file + ".rotate";
    return new Promise(function (resolve, reject) {
        debug('.Trying to lock');
        // retry wait is 10..30ms, 5 retries
        lockFile.lock(fileRotate, {stale: 1000 * 1000, retries: 5, retryWait: Math.floor(Math.random() * 20) + 10}, function (err) {
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
            if (that.isRuleTime()){
                try {
                    debug('Check ' + that.fileNameFormat({timestamp: that._rotateStartPeriod}));
                    fs.accessSync(that.fileNameFormat({timestamp: that._rotateStartPeriod}));
                    debug("Already rotated");
                    return that.reopen();
                } catch (e) {
                    if (e.code != 'ENOENT'){
                        error(e);
                    }
                }
                debug(' -- rotating, rotateAt: ', that._rotateAt);
            }
            debug(`rename ${that._file} to ${that._file}.tmp`);
            fs.renameSync(that._file, that._file + ".tmp");
            that._isEnded = true;

            return that.reopen().then(() => {
                if (that.isRuleTime()){
                    return that._renameByTime();
                } else {
                    return that._renameBySize();
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
            if (that.isRuleTime()) {
                that._rotateAt = that._nextRotate(new Date());
                that._rotateStartPeriod = that._rotatePeriod(that._rotateAt);
            }
            return Promise.resolve();
        });
    }, (e) => {
        debug('.Lock rejected');
        debug('  -- ROTATE OFF ');
        if (that.isRuleTime()) {
            that._rotateAt = that._nextRotate(new Date());
            that._rotateStartPeriod = that._rotatePeriod(that._rotateAt);
        }

        return that.reopen();
    }).catch((e) => {
        error('Error in rotate:', e);
        debug('.Unlock file:', fileRotate);
        lockFile.unlockSync(fileRotate);
        debug('  -- ROTATE NOT OK ');
        return Promise.resolve();
    });
};

RotatingFileHandler.prototype._fileFormat = function (file) {
    var name = file;
    if (this._rule == 'time'){
        name += '-%d';
    } else if (this._rule == 'size'){
        name += '.%i';
    }
    return name;
};

RotatingFileHandler.prototype._write = function write(data, callback) {
    this._stream.write(data, callback);
};

RotatingFileHandler.prototype._renameByTime = function _renameByTime() {
    var name = this._file + ".tmp";
    var newName = this.fileNameFormat({timestamp: this._rotateStartPeriod});

    try {
        debug('_renameByTime ', name, 'to ', newName);
        fs.renameSync(name, newName);
    } catch (e) {
        error(`Can not rename ${name} to ${newName}`, e);
    }

    return this._deleteOldFiles();
};

RotatingFileHandler.prototype._renameBySize = function () {
    var that = this;
    return readdir(path.dirname(this._file)).then((files) => {
        let m;
        let fileBasename = path.basename(that._file);
        let fileDirname = path.dirname(that._file);
        let unsortedList = [];
        for (let i = 0; i < files.length; i++) {
            if (files[i].indexOf(fileBasename) == 0 && (m = files[i].substr(fileBasename.length).match(/^\.(\d{0,5}|tmp)$/))) {
                unsortedList.push({i: m[1] == 'tmp' ? 'tmp' : parseInt(m[1]), f: files[i]});
            }
        }
        if (unsortedList.length > 0) {
            let sortedList = unsortedList.sort((a, b) => {
                if (a.i == 'tmp'){ return -1; }
                if (b.i == 'tmp'){ return 1; }
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
                if (i >= this._options.maxFiles) {
                    debug('delete ', filename);
                    fs.unlinkSync(filename);
                    continue;
                }
                let newExt = sortedList[i].i == 'tmp' ? '1' : sortedList[i].i + 1;
                let newFilename = fileDirname + path.sep + fileBasename + "." + newExt;
                debug('_renameBySize ', filename, 'to ', newFilename);
                fs.renameSync(filename, newFilename);
            }
        }
        return Promise.resolve();
    });
};

module.exports = RotatingFileHandler;
