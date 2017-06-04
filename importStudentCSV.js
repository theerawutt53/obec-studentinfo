var csv = require('csv-stream');
var request = require('request');
var through2 = require('through2');
var JSONStream = require('JSONStream');
var level = require('level');
var uuid = require('node-uuid');
var fs = require('fs');

var util = require('./util');
var config = require('./config');
var obecStudent = require('./parser/obec_student');

fs.createReadStream('./dmcSampleData/52030000_59_1.csv')
  .pipe(csv.createStream())
  .pipe(obecStudent.createParser('2016', '1'))
  .pipe(through2.obj(function(chunk, encode, callback) {
    util.get_dbs('obecStudents', function(err, db) {
      if (err) {
        console.log({
          'ok': false,
          'message': err
        });
      } else {
        db.indexes['cid'].createIndexStream({
            "start": [chunk.cid],
            "end": [chunk.cid + "xFF"],
            "limit": -1,
            "include_doc": true
          })
          .on('data', function(data) {
            if (data.length == 0) {
              chunk['_obecStudent'] = null;
              self.push(chunk);
              callback();
            } else {
              chunk['_obecStudent'] = data[0].value.key;
              chunk['_obectStudent_doc'] = data[0].value.doc;
              if (data.length == 1) {
                self.push(chunk);
                callback();
              } else {
                console.log('' + data.length + ' records', chunk.cid);
                console.log(data);
              }
            }
          })
          .on('end', function() {
            //console.log('Stream ended');
          })
      }
    });
  }))

  /*
  .pipe(through2.obj(function(chunk,encode,callback) {
    var self = this;
    if(chunk._obecStudent == null) {
      var obj = {};
      var keys = ['class','cid','host_id','year','semester'];
      keys.forEach(function(key) {
        obj[key] = chunk[key];
      });
      request({
        method:'POST',
        url:config.db_url+'/dbs/obecStudents',
        json:true,
        headers: {
          'Authorization':'JWT '+config.token
        },
        body:obj
      },function(err,response,body) {
        console.log('insert',chunk.cid);
        self.push(chunk);
        callback();
      });
    } else {
      console.log('not new',chunk._obecStudent);
    }
  }))
  */

  .on('data', function(data) {
    console.log(data);
  });