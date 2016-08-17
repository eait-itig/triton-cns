/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2015, Joyent, Inc.
 */

var fs = require('fs');
var redis = require('redis');
var vasync = require('vasync');
var EaitCustom = require('./lib/eait-custom');
var config = require('./lib/config');
var path = require('path');

var confPath = path.join(__dirname, 'etc', 'config.json');
var conf = config.parse(confPath);
var client = redis.createClient(conf.redis_opts);

var eaitZonesPath = path.join(__dirname, 'etc', 'eait_zones.json');
var eaitZones = JSON.parse(fs.readFileSync(eaitZonesPath));

var opts = {
	config: conf,
	client: client
};

/* add new records as specified in eait_zones.json */
var ec = new EaitCustom(opts);
ec.generateRecords(eaitZones, function(err) {
	var doRemoves = ec.doRemoves.bind(ec);
	var doAdds = ec.doAdds.bind(ec);

	vasync.forEachPipeline({
		func: doRemoves,
		inputs: Object.keys(ec.parsedRecords)
	}, function (err) {
		vasync.forEachPipeline({
			func: doAdds,
			inputs: Object.keys(ec.parsedRecords)
		}, function (err2) {
			if (err2 === null)
				console.log("Zones added.");
			ec.client.quit();
		});
	});
});
