var through2 = require('through2');
var stream = require('stream');
var gutil = require('gulp-util');
var uuid = require('node-uuid');
var request = require('request');
var jsonStream = require('JSONStream');
var levelup = require('levelup');
var levelindex = require('leveldb-index');
var sublevel = require('level-sublevel');
var diff = require('changeset');

module.exports = function(opts) {
  return through2.obj(function(file,enc,callback) {
    
    var db = sublevel(levelup(opts.db_path,{'valueEncoding':'json'}));
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
        start: [chunk.obec.cid],
        end: [chunk.obec.cid + "xFF"],
        limit: -1,
        include_doc: true
       }).on('data',function(student) {
         _tmp.push(student);
       }).on('end',function() {
         if(_tmp.length==0) {
           _new++;
           var str = uuid.v1();
           var doc_id = str.split('-').join('');
           db.put(doc_id,chunk.obec,function(err) {
             if(err) {
               callback(new gutil.PluginError('check_exists', 'Cannot put data to DB'));
             } else {
               content.push(chunk);
               cb();
             }
           });
         } else { 
           if(_tmp.length == 1) {
             var changes = diff(_tmp[0].value.doc,chunk.obec);
             console.log('changed',changes);
             content.push(chunk);
             cb();
           } else {
             callback(new gutil.PluginError('check_exists', 'Duplicate Records',chunk.obec.cid));
           }
         }
       });
     }))
     .on('finish',function() {
       gutil.log(file.path,_new,'/',content.length);
       db.close(function() {
         file.contents = new Buffer(JSON.stringify(content,null,2));
       //  console.log(JSON.stringify(content,null,2));
         callback(null,file);
       });
     });
    
  });
};
