var through2 = require('through2');
var stream = require('stream');
var gutil = require('gulp-util');
var csv = require('csv-stream');

var fieldMapping = {
  'รหัสโรงเรียน':'obec.host_id',
  'เลขประจำตัวประชาชน':'obec.cid',
  'ชั้น':'obec.class',
  'ห้อง':'obec.room',
  'รหัสนักเรียน':'profile.student_id',
  'คำนำหน้าชื่อ':'profile.title',
  'ชื่อ':'profile.name',
  'นามสกุล':'profile.lastname',
  'วันเกิด':'profile.dob',
  'หมู่โลหิต':'profile.blood',
  'น้ำหนัก':'obec.weight',
  'ส่วนสูง':'obec.height',
  'ความด้อยโอกาส':'obec.welfare'
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
           var _key = fieldMapping[key].split('.');
           if(!data[_key[0]]) data[_key[0]]={};
           data[_key[0]][_key[1]]=chunk[key];
         }
       }
       content.push(data);
       cb();
     }))
     .on('finish',function() {
       gutil.log('obec_student',file.path,content.length,'records');
       file.contents = new Buffer(JSON.stringify(content,null,2));
       callback(null,file);
     });
    
  });
};
