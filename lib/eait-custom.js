var genuuid = require('uuid').v4;
var fs = require('fs');
var bunyan = require('bunyan');
var path = require('path');
var redis = require('redis');
var assert = require('assert-plus');
var utils = require('./utils');
var Zone = require('./zone-builder');
var vasync = require('vasync');

/*
 * EaitCustom is a utility class to help implement some custom
 * features required for the EAIT CNS infrastructure.
 *
 * Specifically:
 * 	1) import custom records that don't belong to a vm 
 * 	2) handle shortnames
 */
module.exports = EaitCustom;

function EaitCustom(opts) {
	assert.object(opts);
	assert.object(opts.client);
	assert.object(opts.config);
	this.config = opts.config;
	this.client = opts.client;
	this.parsedRecords = undefined;
}

/*
 * Handle removal of custom records:
 * we maintain a list of eait custom records under eait:[zonename]
 * we also maintain a list of owned shortnames
 * both of these need to be cleared when a record is deleted
 */
EaitCustom.prototype.doRemoves = function (zname, cb) {
	if (this.parsedRecords === undefined) {
		var err = new Error(
			'No parsed records. Nothing to remove');
		cb(err);
	}

	var self = this;
	
	getRms(zname, function (err, data) {
		if (err)
			cb(err)
		
		data.forEach(function(d) { doRemove(d); });
		cb(null);
	});

	function getRms(zname, cb) {
		self.client.smembers('eait:' + zname, function (err, val) {
			var del = [];
			
			if (err)
				cb(err);
			
			val.forEach(function(zn) {
				if (!self.parsedRecords[zname][zn])
					del.push(zn);
			});
			
			cb(null, del);
		});
	}

	function doRemove (record, rcb) {
		self.client.hdel('zone:' + zname, record);
		self.client.srem('eait:' + zname, record);
		self.client.del('shortname:' + record);
	}
};

EaitCustom.prototype.doAdds = function (zname, cb) {
	if (this.parsedRecords === undefined) {
		var err = new Error(
			'No parsed records. Nothing to add.');
		cb(err);
		return;
	}

	var self = this;
	var newSerial = utils.nextSerial();
	var records = this.parsedRecords[zname];

	vasync.forEachPipeline({
		func: doAdd,
		inputs: Object.keys(records)

	}, function (err) {
		if (err)
			cb(err);
		/* Bump the serial */
		self.client.set('zone:' + zname + ':latest', newSerial);
		cb(null);
	});

	function doAdd(entry, acb) {
		var shortName = {
			"type": "shortname",
			"owner": "EAIT",
			"vms": []
		};
		self.client.hset('zone:' + zname, entry, JSON.stringify(records[entry]));
		self.client.sadd('eait:' + zname, entry, JSON.stringify(records[entry]));
		self.client.set('shortname:' + entry, JSON.stringify(shortName));
		acb(null);
	}
}

/*
 * generate new custom records that can be added to cns
 * newZones should be in the format:
 *
 *   "uqcloud.net": [
 *     {
 *       "name": "custom-record",
 *       "records": [
 *         { "A": [ "0.0.0.0" ] }
 *       ]
 *     }
 *   ]
 */
EaitCustom.prototype.generateRecords = function (newZones, cb) {
	var self = this;
	this.parsedRecords = {};

	vasync.forEachPipeline({
		func: doZone,
		inputs: Object.keys(newZones)
	}, function(err) {
		cb(err);
	});

	function doZone(zname, zcb) {
		var addRecords = [];
		vasync.forEachPipeline({
			func: doEntry,
			inputs: newZones[zname]
		}, function (err) {
			self.parsedRecords[zname] = addRecords;
			zcb(null);
		});

		function doEntry(entry, ecb) {
			addRecords[entry.name] = [];
			vasync.forEachPipeline({ 
				func: doRecord,
				inputs: entry.records
			}, ecb);

			function doRecord(rec) {
				var constructor = Object.keys(rec)[0];
				var newRec = {
					"constructor": constructor,
					"args": rec[constructor],
					"id": genuuid()
				};
				addRecords[entry.name].push(newRec);
				ecb(null);
			}
		}
	}
}
