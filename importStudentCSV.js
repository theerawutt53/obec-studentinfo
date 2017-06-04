var csv = require('csv-stream');
var request = require('request');
var through2 = require('through2');
var JSONStream = require('JSONStream');
var level = require('level');
var uuid = require('node-uuid');
var fs = require('fs');
var diff = require('changeset');
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
  .pipe(through2.obj(function(chunkReadFile, encode, callbackRead) {
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
          objRead[key] = chunkReadFile[key];
        });
        
        db.indexes['cid'].createIndexStream({
            "start": [chunkReadFile.cid],
            "end": [chunkReadFile.cid + "xFF"],
            "limit": -1,
            "include_doc": true
          })
          .pipe(through2.obj(function(chunkQuery, encode, callbackQuery) {
            console.log(chunkReadFile.cid);
            console.log(chunkQuery);
            if(!chunkQuery){
              util.put('obecStudents', doc_id, objRead, function(result) {
                count++;
                console.log('--count--', count);
              });
            }else{
              var changes = diff(objRead, chunkQuery.value.doc);
              console.log(changes.length);
              /*
              if (changes.length == 0) {
                var str = uuid.v1();
                var doc_id = str.split('-').join('');

                var obj = {};
                var keys = ['class', 'cid', 'host_id', 'year', 'semester'];
                keys.forEach(function(key) {
                  obj[key] = chunkReadFile[key];
                });

                util.put('obecStudents', doc_id, obj, function(result) {
                  count++;
                  console.log('--count--', count);
                });
              }
             */
            } 
            callbackQuery();
          }));
      }
    });
    callbackRead();
  }))
  
  /*
  var csv = require('csv-stream');
var request = require('request');
var through2 = require('through2');
var JSONStream = require('JSONStream');
var level = require('level');
var uuid = require('node-uuid');
var fs = require('fs');


var util = require('./util');
var obecStudent = require('./parser/obec_student');
var config = require('./config');

var count = 0;

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
            console.log(data);
            if (data.length == 0) {
              console.log('if',chunk.cid);
              chunk['_obecStudent'] = null;
              self.push(chunk);
              callback();
            } else {
              console.log('else',chunk.cid);
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

  .pipe(through2.obj(function(chunk,encode,callback) {
    var self = this;
    if(chunk._obecStudent == null) {
      var str = uuid.v1();
      var doc_id = str.split('-').join('');
  
      var obj = {};
      var keys = ['class','cid','host_id','year','semester'];
      keys.forEach(function(key) {
        obj[key] = chunk[key];
      });      
      
      util.put('obecStudents', doc_id, obj, function (result) {
        self.push(chunk);        
        count++;
        console.log('--count--', count);
        callback();
      });

    } else {
      console.log('not new',chunk._obecStudent);
    }
  }))

  .on('data', function(data) {
    console.log(data);
  });
  
  */