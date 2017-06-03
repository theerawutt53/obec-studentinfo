var through2 = require('through2');

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

var createParser = function(year,semester) {
  return through2.obj(function(chunk,enc,callback) {
    var data = {'raw':chunk};
    data['year']=year;
    data['semester']=semester;
    for(var key in fieldMapping) {
      if(chunk[key]) {
        data[fieldMapping[key]]=chunk[key];
      }
    }
    this.push(data);
    callback();
  });
};


module.exports.createParser = createParser;
