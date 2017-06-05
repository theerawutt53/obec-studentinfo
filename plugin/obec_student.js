var through2 = require('through2');
var stream = require('stream');
var gutil = require('gulp-util');
var csv = require('csv-stream');

var fieldMapping = {
  'รหัสโรงเรียน':'host_id',
  'เลขประจำตัวประชาชน':'cid',
  'ชั้น':'class',
  'ห้อง':'room',
  'รหัสนักเรียน':'student_id',
  'น้ำหนัก':'weight',
  'ส่วนสูง':'height',
  'ความด้อยโอกาส':'welfare'
};

module.exports = function(year,semester) {
  return through2.obj(function(file,enc,callback) {
    var self = this;
    var bufferStream = new stream.PassThrough();  
    var content = [];
    bufferStream.end(file.contents);
    bufferStream.pipe(csv.createStream())
     .pipe(through2.obj(function(chunk,enc,cb) {
       var data = {'raw':chunk};
       // data['year']=year;
       // data['semester']=semester;
       for(var key in fieldMapping) {
         if(chunk[key]) {
           data[fieldMapping[key]]=chunk[key];
         }
       }
       content.push(data);
       cb();
     }))
     .on('finish',function() {
       gutil.log('obec_student',file.path,content.length,'records');
       file.contents = new Buffer(JSON.stringify(content));
       callback(null,file);
     });
    
  });
};
