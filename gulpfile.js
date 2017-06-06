var gulp = require('gulp');
var changed = require('gulp-changed');
var parser = require('./plugin/obec_student');
var check_exists = require('./plugin/check_exists');

var config = require('./config');

gulp.task('toJSON',function() {
  return gulp.src('../../dmc/dmcSampleData/*.csv')
    .pipe(changed('../../dmc/build/exists'))
    .pipe(parser())
    .pipe(check_exists({
       db_path:'./databases/obecStudents',
       index:config.index.obecStudents
    }))
    .pipe(gulp.dest('../../dmc/build/exists'));
});

gulp.task('default',['toJSON']);
