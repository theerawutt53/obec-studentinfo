var util = require('./util');

var count = 0;

util.get_dbs('obecStudents', function(err, db) {
  if (err) {
    console.log({
      'ok': false,
      'message': err
    });
  } else {
    db.createReadStream()
      .on('data', function(data) {
        console.log(data);
      })
      .on('end', function() {
        console.log('Stream ended');
      })
  }
});