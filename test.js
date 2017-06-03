var csv = require('csv-stream');
var request = require('request');
var through2 = require('through2');
var fs = require('fs');

var obecStudent = require('./parser/obec_student');
var config = require('./config');

fs.createReadStream('../../dmc/dmcSampleData/52030000_59_1.csv')
  .pipe(csv.createStream())
  .pipe(obecStudent.createParser('2016','1'))
  .pipe(through2.obj(function(chunk,encode,callback) {
    var self = this;
    request({ 
      method:'POST',
      url:config.db_url+'/query/obecStudents/cid',
      json:true,
      headers: {
        'Authorization':'JWT '+config.token
      },
      body:{'match':[chunk.cid],'include_doc':true}
    },function(err,response,body) {
      if(body.length==0) {
        chunk['_obecStudent']=null;
        self.push(chunk);
        callback();
      } else {
        chunk['_obecStudent']=body[0].value.key;
        chunk['_obectStudent_doc']=body[0].value.doc;
        if(body.length == 1) {
          self.push(chunk);
          callback();
        } else {
          console.log(''+body.length+' records',chunk.cid);
          console.log(body);
        }
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
  .on('data',function(data) {
    console.log(data);
  });
