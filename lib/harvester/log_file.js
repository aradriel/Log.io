/* LogFile model used by LogHarvester */

var fs = require('fs');
var __ = require('underscore');
var util = require('util');
var cp = require('child_process');
var spawn = cp.spawn;
var exec = cp.execFile;
var LOG_LINEBREAK = "\n";
var HISTORY_LENGTH = 100000;

var LogFile = function(path, label, harvester) {
  this.label = label;
  this.path = path;
  this.harvester = harvester;
  this._enabled = false;
}

LogFile.prototype = {

  // Watch file for changes, send log messages to LogFile
  watch: function() {
    var log_file = this;

    // fs.watchFile() uses inotify on linux systems
    fs.watchFile(this.path, function(curr, prev) {
      if (log_file.harvester.connected && curr.size > prev.size) {
        log_file.ping();

        if (log_file._enabled) {
          // Read changed lines
          var stream = fs.createReadStream(log_file.path, {
            encoding: log_file.harvester._conf.encoding,
            start: prev.size,
            end: curr.size
          });

          // Send log messages to LogServer
          stream.on('data', function (data) {
            var lines = data.split(LOG_LINEBREAK);
            __(lines).each(function(msg, i) {
              // Ignore last element, will either be empty or partial line
              if (i<lines.length-1) {
                log_file.send_log(msg);
              }
            });
          });
        }
      }
    });
  },

  // Begin sending log messages to LogServer
  enable: function() {
    this._enabled = true;
  },

  // Stop sending log changes to LogServer
  disable: function() {
    this._enabled = false;
  },

  // Sends log message to server
  send_log: function(message) {
    this.harvester._send(this.harvester._conf.message_type, {
      node: this.harvester._conf.node,
      log_file: this.label,
      msg: message
    });
    this.harvester.messages_sent++;
  },

  // Sends all lines from the last 100000 characters of file
  send_history: function(client_id, history_id) {
    var length = HISTORY_LENGTH;
    var lines = [];
    
    // Read from file, create array of lines
    // TODO: Notify server/client if file doesn't exist
    try {
      var stat = fs.statSync(this.path);
      var fd = fs.openSync(this.path, 'r');
      var text = fs.readSync(fd, length, Math.max(0, stat.size - length));
      lines = text[0].split(LOG_LINEBREAK).reverse();
    } catch(err) {}

    // Send log lines to LogServer
    this.harvester._send('history_response', {
      node: this.harvester._conf.node,
      history_id: history_id,
      client_id: client_id,
      log_file: this.label,
      lines: lines
    });
  },

  // Sends ping to LogServer
  ping: function() {
    this.harvester._send('ping', {
      node: this.harvester._conf.node,
      log_file: this.label
    });
  }
}

var RemoteLogFile = function(options, label, harvester) {
  this.label = label;
  this.host = options.host;
  this.path = options.path;
  this.harvester = harvester;
  this._enabled = false;
}
util.inherits(RemoteLogFile, LogFile);

RemoteLogFile.prototype.watch = function(curr, prev) {
  var log_file = this;

  log_file.ssh = spawn('ssh', [log_file.host, 'tail -1f ' + log_file.path]);
  log_file.ssh.stdout.setEncoding('utf8');
  if (log_file._enabled) log_file.enable();
}

RemoteLogFile.prototype.send_history = function(client_id, history_id) {
  var log_file = this;
  var length = HISTORY_LENGTH;
  var lines = [];

  exec(
    '/usr/bin/env',
    ['ssh', log_file.host, 'tail -' + length + ' ' + log_file.path],
    function(error, stdout, stderr) {
      lines = stdout.split(LOG_LINEBREAK).reverse();

      // Send log lines to LogServer
      log_file.harvester._send('history_response', {
        node: log_file.harvester._conf.node,
        history_id: history_id,
        client_id: client_id,
        log_file: log_file.label,
        lines: lines
      });
    }
  );
}

RemoteLogFile.prototype.receive_data = function(data) {
  var log_file = this;
  this.ping();
  var lines = data.split(LOG_LINEBREAK);
  __(lines).each(function(msg, i) {
    // Ignore last element, will either be empty or partial line
    if (i<lines.length-1) {
      log_file.send_log(msg);
    }
  });
}

RemoteLogFile.prototype.enable = function() {
  LogFile.prototype.enable.call(this);
  this.ssh.stdout.on('data', this.receive_data.bind(this));
}

RemoteLogFile.prototype.disable = function() {
  LogFile.prototype.disable.call(this);
  this.ssh.stdout.removeListener('data', this.receive_data.bind(this));
}

module.exports = {
  LogFile: LogFile,
  RemoteLogFile: RemoteLogFile,
  HISTORY_LENGTH: HISTORY_LENGTH
}