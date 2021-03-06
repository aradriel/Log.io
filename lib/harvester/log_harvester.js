/* Log Harvester
 * Runs on machine/instance with log files
 * Connects to LogServer, sends log_messages and pings via socket.io
 * Listens to LogServer for enable/disable & log_history requests
 */

var __ = require('underscore');
var io = require('socket.io-client');
var lf = require('./log_file.js');
var logging = require('../logging.js');

var STATUS_INTERVAL = 60 * 1000; // 60 seconds
var RECONNECT_INTERVAL = 5 * 1000; // 5 seconds
var HEARTBEAT_PERIOD = 20 * 1000; // 20 seconds
var HEARTBEAT_FAILS = 3; // Reconnect after 3 missed heartbeats

// LogHarvester gets registered as a Node on the LogServer.
// Contains config information, LogFile pool, and socket
var LogHarvester = function(conf) {
  if (!conf) { throw Error("Missing configuration"); }
  if (!conf.server) { throw Error("Missing server configuration"); }
  if (!conf.log_file_paths) { conf.log_file_paths = {} };
  this._log = new logging.Logger();
  this._conf = conf;
  this._conf.server = conf.server;
  this._conf.log_file_paths = conf.log_file_paths;
  this._conf.node = conf.instance_name;
  this._conf.encoding = conf.encoding || 'utf8';
  this._conf.message_type = conf.server.message_type || 'log';
  this._conf.status_frequency = conf.status_frequency || STATUS_INTERVAL;
  this.last_heartbeat = null;
  this.messages_sent = 0;
  this.connected = false;
  this.reconnecting = false;
  this.log_files = {};
  var harvester = this;

  // Create LogFile models, add to harvester pool
  __(harvester._conf.log_file_paths).each(function(path, label) {
    if (typeof path == 'string') {
      var log_file = new lf.LogFile(path, label, harvester);
    } else {
      var log_file = new lf.RemoteLogFile(path, label, harvester);
    }
    harvester.log_files[label] = log_file;
  });
}

LogHarvester.prototype = {

  // Create socket, bind callbacks, connect to server
  connect: function() {
    var harvester = this;
    var uri = harvester._conf.server.host + ":" + harvester._conf.server.port;
    harvester.socket = io.connect(uri);

    // TODO(msmathers): Ensure this works once socket.io bug has been fixed...
    // https://github.com/LearnBoost/socket.io/issues/473
    harvester.socket.on('error', function(reason) {
      harvester.reconnect();
    });

    // Register announcement callback
    harvester.socket.on('connect', function() {
      harvester._log.info("Connected to server, sending announcement...");
      harvester.announce();
      harvester.connected = true;
      harvester.reconnecting = false;
      harvester.last_heartbeat = new Date().getTime();
    });

    // Server heartbeat
    harvester.socket.on('heartbeat', function() {
      harvester._log.info("Received server heartbeat");
      harvester.last_heartbeat = new Date().getTime();
      return;
    });

    // Node with same label already exists on server, kill process
    harvester.socket.on('node_already_exists', function() {
      harvester._log.error("ERROR: A node of the same name is already registered");
      harvester._log.error("with the log server. Change this harvester's instance_name.");
      harvester._log.error("Exiting.");
      process.exit(1);
    });

    function _log_file_helper(message, func) {
      var log_file = harvester.log_files[message.log_file];
      if (log_file == null) { return; }
      return func(log_file);
    }

    // Begin sending log messages to server
    harvester.socket.on('enable_log', function(message) {
      _log_file_helper(message, function(log_file) {
        harvester._log.info("Enabling log file: " + log_file.label);
        log_file.enable();
      });
    });

    // Stop sending log messages to server
    harvester.socket.on('disable_log', function(message) {
      _log_file_helper(message, function(log_file) {
        harvester._log.info("Disabling log file: " + log_file.label);
        log_file.disable();
      });
    });

    // Respond to history request from WebClient
    harvester.socket.on('history_request', function(message) {
      _log_file_helper(message, function(log_file) {
        log_file.send_history(message.client_id, message.history_id);
      });
    });
  },

  // Run log harvester
  run: function() {
    var harvester = this;
    harvester.connect();

    // Begin watching log files
    __(harvester.log_files).each(function(log_file, label) {
      log_file.watch();
      harvester._log.info("Watching: " + log_file.path + " (" + label + ")");
    });

    // Check for heartbeat every HEARTBEAT_PERIOD, reconnect if necessary
    setInterval(function() {
      var delta = ((new Date().getTime()) - harvester.last_heartbeat);
      if (delta > (HEARTBEAT_PERIOD * HEARTBEAT_FAILS)) {
        harvester._log.warn("Failed heartbeat check, reconnecting...");
        harvester.connected = false;
        harvester.reconnect();
      }
    }, HEARTBEAT_PERIOD);

    // Print status every minute
    setInterval(function() {
      harvester._log.info("Watching " + __(harvester.log_files).size()
        + " log files, " + " sent " + harvester.messages_sent
        + " log messages.");
    }, harvester._conf.status_frequency);
  },

  // Sends announcement to LogServer
  announce: function() {
    this._send('announce_node', {
      client_type:'node',
      logs: __(this.log_files).keys(),
      label: this._conf.node
    });
  },

  // Reconnect helper, retry until connection established
  reconnect: function(force) {
    if (!force && this.reconnecting) { return; }
    this.reconnecting = true;
    this._log.info("Reconnecting to server...");
    var harvester = this;
    setTimeout(function() {
      if (harvester.connected) { return; }
      harvester.connect();
      setTimeout(function() {
        if (!harvester.connected) {
          harvester.reconnect(true);
        }
      }, RECONNECT_INTERVAL/2)
    }, RECONNECT_INTERVAL);
  },

  // Sends message to LogServer, gracefully handles connection failure
  _send: function(event, message) {
    try {
      this.socket.emit(event, message);
    // If server is down, a non-writeable stream error is thrown.
    } catch(err) {
      this._log.error("ERROR: Unable to send message over socket.");
      this.connected = false;
      this.reconnect();
    }
  }
}

exports.LogHarvester = LogHarvester;
