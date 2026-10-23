# ChangeLog

## 2.0.0 - 2026-10-23

### Breaking changes

 - Node.js >= 22.12 is required (`engines` field added). Node 20 and older are not supported.
 - `chalk` dependency removed. ConsoleHandler colorizes messages with built-in `util.styleText`, escape codes are the same as before.
   loggj no longer sets `chalk.enabled = true` on a shared `chalk@1` instance.
 - Last argument of a log call which is `instanceof Error` but not a native error goes to `record.err` again on every Node version
   (in 1.0.11 on Node 23+ it stayed in `record.args`).

### Changes

 - Fix Node 23+ support: `makeFilter(RegExp)` and RegExp filters in `config()` threw `TypeError` because `util.isRegExp` was removed.
 - Replace `util.isError` workaround with `util.types.isNativeError` and `instanceof Error`.
 - Fix `config()`: logger `filters` by name always threw `There is no filter with name`.
 - Fix RotatingFileHandler: when log file was removed, it was reopened but its size was not read, so rotation check was skipped.
 - `stack-trace` dependency removed, call sites for `loggj.console()` are taken with V8 stack trace API directly.
 - `lockfile` updated to ^1.0.4. It is the only runtime dependency now.
 - Tests moved to built-in `node:test`, `mocha` and `sinon` removed, no dev dependencies left. Added `npm run coverage`.
 - Restored tests for RotatingFileHandler and `handleExceptions`, added tests for ConsoleHandler colors and `ttyonly`.

## 0.5.7 - 2014-12-18

 - Fix console handler and how used levels

## 0.5.6 - 2014-02-03

 - Bug in rotating handler
 - Stable and define log remover

## 0.5.5 - 2014-01-30

 - Typo in file handler .reopen

## 0.5.4 - 2014-01-30

 - Make rotating handler work now both with size based and time based rotations.

## 0.5.3 - 2014-01-28

 - Some fixes for formats

## 0.5.2 - 2014-01-27

 - Time based log rotation

## 0.5.1 - 2014-01-25

- Fix last tests
- Small cleanup for file handler

## 0.5.0 - 2013-11-13

- General cleanup
- Remove timeout for handlers
- Improve perfomance
- Use util.format instead of own printf
- More accurate console replacement

## v0.3.0 - 2013-10-24

- added intel.handlers.Rotating
- added ignore options to intel.console()
- added chalk.enabled when colorize is used
- added padding and truncation to printf
- added Logger#handleExceptions to catch uncaught exceptions
- added stack traces when an exception is logged
- fork intel to support it myself
- changed Promises from Q to bluebird, significantly faster
- fixed Console handler from using accepting format options
- make format and dates compiled. Perfomance stability with any format

# inter Changelog

## v0.2.0 - 2013-10-04

- added Filters for Handlers and Loggers
- added Handler timeout option
- added pid to LogRecord
- added configuration using JSON
- changed Promises to LazyPromises
- changed printf to faster, smaller printf
- changed internal forEach to faster while loops
- removed node v0.6 support (it didn't work anyways)

## v0.1.0 - 2013-09-30

- Initial release.
