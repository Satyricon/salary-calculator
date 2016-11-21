/* jshint strict:true */
/* jshint node: true */
/* jshint esnext: true */
'use strict';

let csv = require('csv-parser');
let fs = require('fs');
let _ = require("lodash");

let salaries = {};

fs.createReadStream('HourList201403.csv')
  .pipe(csv())
  .on('data', function (data) {
    let name = data["Person Name"], id = data["Person ID"], date = data.Date, start = data.Start, end = data.End;

    updateSalary(name, id, date, start, end);


  })
  .on('end', function () {
    console.log("done");
});

function updateSalary(name, id, date, start, end) {
  if(salaries[name]) {

  }
  else {
    
  }
}

function afterSix(time) {

}

{
  "Yuri Barseghyan": {
    "id": 1,
    "totalSalary": 1234.56,
    "overtimeCompensation": 234.56,
    "eveningWorkCompensation": 100.00,
    "12.14.2015": {
      "normalHours": 4.25,
      "eveningHours": 3.75,
      "overtime25": 2,
      "overtime50": 2,
      "overtimeDouble": 3.5
    }
  }
}
