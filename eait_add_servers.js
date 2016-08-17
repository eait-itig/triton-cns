var genuuid = require('uuid').v4;
var fs = require('fs');
var bunyan = require('bunyan');
var path = require('path');
var redis = require('redis');
var utils = require('./lib/utils');
var vasync = require('vasync');
var restify = require('restify-clients');

var fs = require('fs');
var redis = require('redis');
var vasync = require('vasync');
var EaitCustom = require('./lib/eait-custom');
var config = require('./lib/config');
var path = require('path');

var confPath = path.join(__dirname, 'etc', 'config.json');
var conf = config.parse(confPath);
var client = redis.createClient(conf.redis_opts);

/* update server */
var cnapi = restify.createJsonClient({
	url: 'http://' + conf.cnapi_opts.address,
});

var napi = restify.createJsonClient({ 
	url: 'http://' + conf.napi_opts.address
});

var opts = {
        config: conf,
        client: client
};

var ec = new EaitCustom(opts);

cnapi.get('/servers', function (err, req, res, objs) {
	var newRecs = {};
	var zone = "zones.eait.uq.edu.au";
	newRecs[zone] = [];

	vasync.forEachPipeline({
		func: getNic,
		inputs: objs
	}, function (nerr) {
		ec.generateRecords(newRecs, function(nerr) {
			var doAdds = ec.doAdds.bind(ec);
			vasync.forEachPipeline({
				func: doAdds,
				inputs: Object.keys(ec.parsedRecords)
			}, function (nerr) {
				console.log("Server records added.");
				ec.client.quit();
			});
		});
	});

	function getNic(rec, cb) {
                napi.get('/nics?belongs_to_uuid=' + rec.uuid + '&nic_tag=admin',
                        function (err2, req2, res2, objs2) {
			if (err2)
				cb(err2);

                        var aRec = { "A": [ objs2[0].ip ] };
                        newRecs[zone].push({
                                "name": rec.hostname,
                                "records": [ aRec ]
                        });
                        
                        newRecs[zone].push({
                                "name": rec.uuid,
                                "records": [ aRec ]
                        });

			cb(null);
                });
	};
});
