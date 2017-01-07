var Web3 = require('web3');
var async = require('async');
var request = require('request');
var crypto = require('crypto');
var cloudant = require('cloudant')({url: process.env.CLOUDANT_URL});
var deedsdb = cloudant.db.use('deeds');
var fs = require('fs');
var simplify = require('simplify-geojson');
var bounding = require('geojson-extent');
var mapshaper = require('mapshaper');
var web3 = new Web3();

web3.setProvider(new web3.providers.HttpProvider('http://localhost:8000'));
// console.log(web3.eth.coinbase);
// 'http://localhost:8500/bzzr:/73efbdca6cca0f33b97a830bfa794b27c64316646f94c55437232473b0616f35'

var deedAbi = [{"constant":true,"inputs":[{"name":"","type":"uint256"}],"name":"nextDeeds","outputs":[{"name":"","type":"address"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"_child","type":"address"}],"name":"addChild","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"status","outputs":[{"name":"","type":"uint8"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"provisional_time","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":false,"inputs":[],"name":"commit","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"_newDeed","type":"address"}],"name":"transferSingle","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"_swarm_id","type":"bytes32"}],"name":"configure_deed","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"numPreviousDeeds","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":false,"inputs":[],"name":"expire","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"registry","outputs":[{"name":"","type":"address"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"name":"","type":"address"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"_parent","type":"address"}],"name":"addParent","outputs":[],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"swarm_id","outputs":[{"name":"","type":"bytes32"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"numNextDeeds","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":true,"inputs":[{"name":"","type":"uint256"}],"name":"previousDeeds","outputs":[{"name":"","type":"address"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"dead_time","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"deed_name","outputs":[{"name":"","type":"bytes32"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"live_time","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"inputs":[{"name":"_previousDeed","type":"address"},{"name":"_owner","type":"address"},{"name":"_deed_name","type":"bytes32"}],"payable":false,"type":"constructor"}];
 
var abi = [{"constant":true,"inputs":[{"name":"","type":"uint256"}],"name":"theRegister","outputs":[{"name":"","type":"address"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"deedCount","outputs":[{"name":"","type":"uint256"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"_existing_deedid","type":"address"},{"name":"_newowner","type":"address"}],"name":"transferSingle","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"_existing_deedid","type":"address"},{"name":"_swarm_id1","type":"bytes32"},{"name":"_swarm_id2","type":"bytes32"},{"name":"_deed_name1","type":"bytes32"},{"name":"_deed_name2","type":"bytes32"}],"name":"split","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"_existing_deedid1","type":"address"},{"name":"_existing_deedid2","type":"address"},{"name":"_swarm_id","type":"bytes32"},{"name":"_deed_name","type":"bytes32"}],"name":"join","outputs":[],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"_swarm_id","type":"bytes32"},{"name":"_deed_name","type":"bytes32"}],"name":"createDeed","outputs":[],"payable":false,"type":"function"},{"inputs":[],"payable":false,"type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"name":"_deedid","type":"address"}],"name":"Log_CreateDeed","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"_deedid","type":"address"}],"name":"Log_TransferSingle","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"_deedid1","type":"address"},{"indexed":false,"name":"_deedid2","type":"address"}],"name":"Log_Split","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"_deedid","type":"address"}],"name":"Log_Join","type":"event"}];

var addr = '0x0e776256edd65b0b873330fb38207ddf8d28698f';

function hex_to_ascii(str1) {  
  var hex  = str1.toString().replace(/0x/,'');  
  var str = '';  
  for (var n = 0; n < hex.length; n += 2) {
    var ascii = parseInt(hex.substr(n, 2), 16);
    if (ascii > 0 ) {
      str += String.fromCharCode(ascii);
    } else {
     break;
    }  
  }  
  return str;  
};


var extractDeed = function(deedid, callback) {
  console.log('Extracting deed', deedid);
  var deed = web3.eth.contract(deedAbi).at(deedid);

  async.parallel([
    function(done) {
      deed.owner(done);
    },
    function(done) {
      deed.swarm_id(done);
    },
    function(done) {
     done(null, null);
    },
    function(done) {
      deed.provisional_time(done);
    },
    function(done) {
      deed.live_time(done);
    },
    function(done) {
      deed.dead_time(done);
    },
    function(done) {
      deed.status(done);
    },
    function(done) {
      deed.numNextDeeds(done)
    },
    function(done) {
      deed.numPreviousDeeds(done)
    },
    function(done) {
      deed.deed_name(done);
    }
  ], function(err, data) {
   
    callback(err, {
      deedid: deedid,
      owner: data[0],
      swarm_id: data[1].toString(),
      provisional_time: parseInt(data[3].toString()),
      live_time: parseInt(data[4].toString()),
      dead_time: parseInt(data[5].toString()),
      status: parseInt(data[6].toString()),
      numNextDeeds: parseInt(data[7].toString()),
      numPreviousDeeds: parseInt(data[8].toString()),
      deed_name: hex_to_ascii(data[9])
    });
  });
};

var extractPreviousDeeds = function(deedobj, callback) {
  console.log('extractPreviousDeeds', deedobj.deedid);
  deedobj.previousDeeds = [];
  var deed = web3.eth.contract(deedAbi).at(deedobj.deedid);
  var numPreviousDeeds = deedobj.numPreviousDeeds;

  // a deed always has previous because that is how they
  // are created. It can only have 1 or 2 previous deeds
  if (numPreviousDeeds == 1) {
    deed.previousDeeds(0, function(err, data) {
      if (data != '0x0000000000000000000000000000000000000000') {
        deedobj.previousDeeds.push(data);
      }
      else {
        deedobj.numPreviousDeeds = 0;
      };
      callback(null, deedobj);
    });
  } else if (numPreviousDeeds == 2) {
     deed.previousDeeds(0, function(err, data) {
      deedobj.previousDeeds.push(data);
      deed.previousDeeds(1, function(err, data) { 
        deedobj.previousDeeds.push(data);
        callback(null, deedobj);
      });
    });
  } else {
    callback(null, deedobj);
  }
};

var extractNextDeeds = function(deedobj, callback) {
  console.log('extractNextDeeds', deedobj.deedid);
  deedobj.nextDeeds = [];
  var deed = web3.eth.contract(deedAbi).at(deedobj.deedid);
  var numNextDeeds = deedobj.numNextDeeds;

  // a deed can have 0, 1 or 2 next deeds
  if (numNextDeeds == 1) {
    deed.nextDeeds(0, function(err, data) {
      deedobj.nextDeeds.push(data);
      callback(null, deedobj);
    });
  } else if (numNextDeeds == 2) {
     deed.nextDeeds(0, function(err, data) {
      deedobj.nextDeeds.push(data);
      deed.nextDeeds(1, function(err, data) { 
        deedobj.nextDeeds.push(data);
        callback(null, deedobj);
      });
    });
  } else {
    callback(null, deedobj);
  }
};


var fullExtractDeed = function(deedid, callback) {
  console.log('fullExtractDeed', deedid);
  extractDeed(deedid, function(err, data) {
    extractPreviousDeeds(data, function(err, data) {
      extractNextDeeds(data, function (err,data) {
        var url = 'http://localhost:8500/bzzr:/' + data.swarm_id.replace(/^0x/,'');

        request(url, function(err,response,manifest) {    //pull down the url
          var obj = {};

          if (!err && response.statusCode === 200) {
            manifest = JSON.parse(manifest);
            var filename = null;
// {"entries":[{"hash":"2d23a5020879ee7c32f0064ebc34ee0214505800079810a1a657d4be5473cbfe","path":"sadc.geojson"},{"hash":"8b3544f5f4cb338b90e50cc09ce3fec9fd9bb9b1f5ab58490c991c05f22a9464","contentType":"image/jpeg","path":"sadc.jpg"},{"hash":"59dacfed22472658df87562216e9d678051e1f11f85ff1078fd7b35cff96d7a5","contentType":"text/plain; charset=utf-8","path":"sadc.txt"}]}
            for(var i  in manifest.entries) {
              var entry = manifest.entries[i];
              if (entry.path.match(/\.geojson$/)) {
                filename= entry.path;
                break;
              }
            }
            if (filename) {
              var url = 'http://localhost:8500/bzz:/' + data.swarm_id.replace(/^0x/,'') + '/' + filename;
              request(url, function(err, response, geojson) {
                if (!err && response.statusCode === 200) {
                  data.valid_url = true;
                  data.url_size = geojson.length;
                  data.valid_hash = true;
                  try {                   //now, is the thing we got actually json
                    var obj = JSON.parse(geojson);
                    data.bounding_box = bounding(obj);
                    data.longitude = (data.bounding_box[0]+data.bounding_box[2])/2;
                    data.latitude = (data.bounding_box[1]+data.bounding_box[3])/2;
                  } catch (e) {                     //now it is not!
                  }
                  var doc = obj;
                  doc.deed = data;
                  doc._id = deedid;
                  doc.deed.manifest = manifest;
                  return callback(null, doc);
                } else  {
                  data.valid_url = false;
                  data.valid_hash = false;
                  data.url_size = 0;
                  var doc = {};
                  doc.deed = data;
                  doc._id = deedid;
                  return callback(null, doc);

                }
              });

            } else {
             data.valid_url = false;
             var doc = {};
             doc.deed = data;
             doc._id = deedid;
             return callback(null, doc);


            }

     
          } else {
            data.valid_url = false;
            var doc = {};
            doc.deed = data;
            doc._id = deedid;
            return callback(null, doc);
          }

/*
          if (!err){    // the http request worked
            data.valid_url = true;
            data.url_size = geojson.length;
            var computed_hash = crypto.createHash("md5").update(geojson).digest("hex");
            if (computed_hash == data.claim_hash){  // validating the hash
              data.valid_hash = true;
            }
            else {
              data.valid_hash = false;
            }
            try {                   //now, is the thing we got actually json
              var obj = JSON.parse(geojson);
              data.bounding_box = bounding(obj);
              data.longitude = (data.bounding_box[0]+data.bounding_box[2])/2;
              data.latitude = (data.bounding_box[1]+data.bounding_box[3])/2;
            } catch (e) {                     //now it is not!
            }
          }
          else {     //could not retrieve url.. http failed
            data.valid_url = false;
          };

*/

/* // simplify geojson
          if (doc.type) {
            mapshaper.applyCommands('-simplify 1%', JSON.stringify(doc), function (err, simpledata) {
              if (err) {
                callback(null, doc);
              } else {
                var doc = JSON.parse(simpledata);
                console.log('Simplified', deedid);
                doc.deed = thedeed;
                doc._id = deedid;
                console.log(Object.keys(doc));
                callback(null, doc);
              } 
            });
          } else {
            callback(null, doc);
          }*/
        });
      });
    });
  });
};


/*
fullExtractDeed('0xbe31c7e405b0076e713d5e6d4b6e8e1aba4b012e', function(err, data) {
  console.log('the deed contains', err, data);
});
*/


var q = async.queue(function(payload, callback) {
  fullExtractDeed(payload, function(err, data) {
    //console.log('the deed contains', payload, data);
    if (data.deed && data.deed.status !==2) {     //i.e. it is not dead, so you want to spider its parents because they will have changed
      for (var i =0; i<data.deed.numPreviousDeeds; i++){
        q.push(data.deed.previousDeeds[i]);     // so it adds the parent deeds to the queue for re-spidering
      }
    }
    data._id = data._id.replace(/^0x/,'');
    deedsdb.get(data._id, function(err, original){
      if(!err) {
        data._rev = original._rev;
      }
      deedsdb.insert(data, function(err, data) {
        callback(null, null);
      });
    });
  });
}, 1);


var landreg = web3.eth.contract(abi).at(addr);
landreg.deedCount(function(err, data) {
  if (err) {
    console.log('could not get deed count');
    return;
  }

  var deedCount = parseInt(data.toString());
  var cursor = 0;
  if (fs.existsSync("cursor.txt")) {
    var str = fs.readFileSync("cursor.txt",{encoding:"utf8"});
    if (str.length > 0) {
      cursor = parseInt(str);  
    }
  }  
  for(var i = cursor; i < deedCount; i++) {
    landreg.theRegister(i, function(err, data) {
      var addr = data.toString();
      q.push(addr);
    });
  }
  fs.writeFileSync("cursor.txt", deedCount.toString());
  
});

