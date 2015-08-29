var request = require('request');
var cradle = require('cradle');
var async = require('async');

var c = new(cradle.Connection);
var db = c.database('donations');

db.exists(function(err, exists) {
  if (err) {
    console.log('err', err);
  } else if (exists) {
    console.log('db exists.');
  } else {
    console.log('database does not exist.');
    db.create(function(err) {
      if (err) console.log(err);
      process.exit(1);
    });
  }
});

var fetchedUrls = [];

// harvest stuff
var numOfRecords, start, jsonData, uri, end;

// create a crawl queue
var crawlQueue = async.queue(fetchDocs, 5);
// assign a finish callback
crawlQueue.drain = function() {
  console.log('all items have been processed');
}

start = 48700;
end = 48750;
// end = 48718;
while (start < end) {
  uri = 'http://search.electoralcommission.org.uk/api/search/Donations?start='+start+'&rows=50&query=&sort=ECRef&order=asc&et=pp&et=ppm&et=tp&et=perpar&et=rd&date=Reported&from=&to=&prePoll=false&postPoll=true&quarters=2015Q1234&quarters=2014Q1234&quarters=2013Q1234&quarters=2012Q1234&quarters=2011Q1234&quarters=2010Q1234&quarters=2009Q1234&quarters=2008Q1234&quarters=2007Q1234&quarters=2006Q1234&quarters=2005Q1234&quarters=2004Q1234&quarters=2003Q1234&quarters=2002Q1234&quarters=2001Q1234&donorStatus=individual&donorStatus=tradeunion&donorStatus=company&donorStatus=unincorporatedassociation&donorStatus=publicfund&donorStatus=other&donorStatus=registeredpoliticalparty&donorStatus=friendlysociety&donorStatus=trust&donorStatus=limitedliabilitypartnership&donorStatus=impermissibledonor&donorStatus=na&donorStatus=unidentifiabledonor&donorStatus=buildingsociety&period=1485&period=1487&period=1480&period=1481&period=1477&period=1478&period=1476&period=1474&period=1471&period=1473&period=1466&period=463&period=1465&period=460&period=447&period=444&period=442&period=438&period=434&period=409&period=427&period=403&period=288&period=302&period=304&period=300&period=280&period=218&period=206&period=208&period=137&period=138&period=128&period=73&period=69&period=61&period=63&period=50&period=40&period=39&period=5';
  crawlQueue.push(uri);
  start += 50;
}

function fetchDocs(uri, callback) {
  request(uri, function(err, response, body) {
    if(err) return callback(new Error(err));

    if(response.statusCode !== 200){
      return callback(new Error(err, response.statusCode));
    }
    console.log('fetching: ' + uri);
    fetchedUrls.push(uri);
    docs = JSON.parse(body);
    insertDocs(docs);
    callback();
  });
}

function insertDocs(docs) {
  db.save(docs['Result'], function(err, body) {
    if (err) console.log(err);
    console.log('added 50 docs');
  });
}