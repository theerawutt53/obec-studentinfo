var through2 = require('through2');
var stream = require('stream');
var gutil = require('gulp-util');
var request = require('request');
var jsonStream = require('JSONStream');
var levelup = require('levelup');
var levelindex = require('leveldb-index');
var sublevel = require('level-sublevel');

module.exports = function(opts) {
  return through2.obj(function(file,enc,callback) {
    
    var db = sublevel(levelup(opts.db_path,{'valueEncoding':'utf8'}));
    db = levelindex(db);
    var indexing = 0;
    opts.index.attributes.forEach(function(attr) {
      indexing++;
      db.ensureIndex(attr.name, attr.map, function() {
        indexing--;
      });
    });
    
    while(indexing>0);
 
    var self = this;
    var bufferStream = new stream.PassThrough();  
    var content = [];
    var _new = 0;
    bufferStream.end(file.contents);
    bufferStream.pipe(jsonStream.parse('*'))
     .pipe(through2.obj(function(chunk,enc,cb) {
       var _this = this;
       var _tmp = [];
       db.indexes['cid'].createIndexStream({
        start: [chunk.cid],
        end: [chunk.cid + "xFF"],
        limit: -1,
        include_doc: true
       }).on('data',function(student) {
         _tmp.push(student);
       }).on('end',function() {
         chunk._previous = _tmp;
         if(_tmp.length==0) {
           _new++;
         }
         content.push(chunk);
         cb();
       });
     }))
     .on('finish',function() {
       gutil.log(file.path,_new,'/',content.length);
       db.close(function() {
         file.contents = new Buffer(JSON.stringify(content,null,2));
         callback(null,file);
       });
     });
    
  });
};
