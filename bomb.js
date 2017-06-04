var csv = require('csv-stream');
var request = require('request');
var through2 = require('through2');
var JSONStream = require('JSONStream');
var level = require('level');
var uuid = require('node-uuid');
var fs = require('fs');
var diff = require('changeset');

var util = require('./util');
var obecStudent = require('./parser/obec_student');
var config = require('./config');

var count = 0;

var stream = fs.createReadStream('./dmcSampleData/52030000_59_1.csv')
  .pipe(csv.createStream())
  .pipe(obecStudent.createParser('2016', '1'))
  .pipe(through2.obj(function(chunk1, encode, callback1) {
    util.get_dbs('obecStudents', function(err, db) {
      if (err) {
        console.log({
          'ok': false,
          'message': err
        });
      } else {
        
        var objRead = {};
            var keys = ['class', 'cid', 'host_id', 'year', 'semester'];
            keys.forEach(function(key) {
              objRead[key] = chunk1[key];
            });
            
        db.indexes['cid'].createIndexStream({
            "start": [chunk1.cid],
            "end": [chunk1.cid + "xFF"],
            "limit": -1,
            "include_doc": true
          })
          .on('data', function(data) {
            console.log(data);
          })
      }
    });
    callback1();
  }))