const mysql = require("mysql");
const xlsx = require("xlsx");
var AWS = require("aws-sdk");
var s3 = new AWS.S3();
exports.handler = async function(event, context, callback) {
  await tableService(event);
};

const tableService = async function(event) {
  var src_bkt = event.Records[0].s3.bucket.name;
  var src_key = event.Records[0].s3.object.key;
  console.log("the bucket is", src_bkt);
  console.log("the src_key is", src_key);
  var params = {
    Bucket: src_bkt,
    Key: src_key
  };
  var fileBuffer;
  await new Promise((resolve, reject) => {
    s3.getObject(params, (err, data) => {
      if (err) {
        console.log("err", err.stack);
        reject(err);
      } else {
        fileBuffer = new Buffer(data.Body);
        resolve(data.Body);
      }
    });
  });

  console.log(fileBuffer.toString("utf8"));
  var data = fileBuffer
    .toString("utf8")
    .split("\n")
    .map(e => e.replace(/['"]+/g, ""))
    .map(e => e.split(";").map(e => e.trim()));
    var lineArray =[];
    data.map((infoArray,index)=>{
      console.log(index)
      var line = infoArray.join(",");
      lineArray.push(index==0 ? "data:text/csv;charset=utf-8"+line:line)
    })
    var csvContent = lineArray.join('\n')
    console.log("the csv content",csvContent)
  console.log("the data is", data);
  //await tableDAO(src_key, data);
};

const tableDAO = async function(tableName, data) {
  var tab = tableName.toString().split(".");
  const table = tab[0];
  console.log("table from bucket", table);
  const tableCheck = `'${table}'`;
  console.log("tableCheck", tableCheck);
  const connection = mysql.createConnection({
    host: "testdbmysql.c3oaofxcjfzr.us-east-2.rds.amazonaws.com",
    user: "root",
    password: "password",
    database: "testdatabase"
  });

  console.log("connecting.......");
  await new Promise((resolve, reject) => {
    connection.connect(err => {
      if (err) {
        reject(err);
      }
      resolve();
    });
  });
  console.log("connected");

  // var isTablePresent;
  // await new Promise((resolve, reject) => {
  //   connection.query(
  //     "select 1 from information_schema.tables where table_name=" + tableCheck,
  //     function(err, results, fields) {
  //       console.log("results for table check", results);
  //       if (err) {
  //         console.log(err);
  //         isTablePresent = 0;
  //         console.log("inside err block");
  //         reject();
  //       } else {
  //         if (
  //           results == undefined ||
  //           results == [] ||
  //           results == null ||
  //           results.length == 0
  //         ) {
  //           isTablePresent = 0;
  //           console.log("inside undefined block");
  //           resolve();
  //         } else {
  //           isTablePresent = 1;
  //           console.log("inside created block");
  //           resolve();
  //         }
  //       }
  //     }
  //   );
  // });

  var header = data.shift();
  // if (isTablePresent == 0) {
  //   
  //   var firstData = data[0];
  //   var headerStr = "";
  //   for(var j=0;j<header.length;j++){
  //     console.log(typeof firstData[j])
  //     console.log("data in 1st line",firstData[j])
  //     headerStr += getColumnDef(firstData[j],header[j]);
  //     console.log(headerStr);
  //   }
  //   console.log(headerStr)
  //   // header.forEach(ele => {
  //   //   headerStr += getColumnDef(ele);
  //   // });
  //   headerStr = headerStr.slice(0, -1);
  //   console.log("header String" + headerStr);
  //   console.log("header", header);
  //   await new Promise((resolve, reject) => {
  //     connection.query(`create table ${table} (${headerStr})`, function(
  //       err,
  //       results,
  //       fields
  //     ) {
  //       if (err) {
  //         console.log(err);
  //         reject(err);
  //       } else {
  //         console.log("table created successfully");
  //         resolve();
  //       }
  //     });
  //   });
  // } else {
  //   console.log("table already exists");
  // }

  data.pop();
  console.log(data);
  await new Promise((resolve, reject) => {
    connection.query(
      `insert into ${table}(${header.join()}) values ?`,
      [data],
      (err, results, fields) => {
        if (err) {
          console.log("error in insertion");
          reject();
        } else {
          console.log("data inserted successfully");
          resolve();
        }
      }
    );
  });
};

function getColumnDef(col,head) {
  return `${head} ${getType(col)},`;
}

function getType(col) {
  switch (typeof col) {
    case "string":
      return "VARCHAR (255)";

    case "number":
      return "INT";

    case "boolean":
      return "bool";

    default:
      throw "NOT KNOWN";
  }
}

//"""name"";""email"";""password"";""address""

// var isTablePresent;
// await new Promise((resolve, reject) => {
//   connection.query(
//     "select 1 from information_schema.tables where table_name=" +
//       checkTableName,
//     function(err, results, fields) {
//       console.log("results for table check", results);
//       if (err) {
//         console.log(err);
//         isTablePresent = 0;
//         console.log("inside err block");
//         reject();
//       } else {
//         if (
//           results == undefined ||
//           results == [] ||
//           results == null ||
//           results.length == 0
//         ) {
//           isTablePresent = 0;
//           console.log("inside undefined block");
//           resolve();
//         } else {
//           isTablePresent = 1;
//           console.log("inside created block");
//           resolve();
//         }
//       }
//     }
//   );
// });

// if (isTablePresent == 0) {
//   await new Promise((resolve, reject) => {
//     connection.query(
//       "create table " +
//         tableName +
//         "(ID int NOT NULL AUTO_INCREMENT PRIMARY KEY)",
//       function(err, results, fields) {
//         if (err) {
//           console.log(err);
//           reject(err);
//         } else {
//           console.log("table created successfully");
//           resolve();
//         }
//       }
//     );
//   });
// } else {
//   console.log("table already exists");
// }
// const existingColums = [];
// await new Promise((resolve, reject) => {
//   connection.query(
//     `select column_name from information_schema.columns where table_name = ${checkTableName}`,
//     function(err, results, fields) {
//       if (err) {
//         console.log(err);
//         reject(err);
//       } else {
//         console.log(results);
//         Object.keys(results).forEach(key => {
//           var row = results[key];
//           console.log(row.COLUMN_NAME);
//           existingColums.push(row.COLUMN_NAME);
//         });
//         resolve();
//       }
//     }
//   );
// });
// console.log("the existing columns", existingColums);
// header = "ID," + header;
// console.log("headers", header);
// //header = Array.from(header).filter(val=>!existingColums.includes(val));
// header = header.toString().split(",");
// header = header.filter(val => !existingColums.includes(val));
// console.log("after split and filter", header);
// console.log("size of the header array", header.length);

// if (header.length > 0) {
//   await new Promise((resolve, reject) => {
//     header.forEach(column => {
//       connection.query(
//         `ALTER TABLE ${tableName} ADD COLUMN ${getColumnDef(column)}`,
//         function(err, results, fields) {
//           if (err) {
//             console.log(err);
//             reject();
//           } else {
//             console.log("Added Headers successfully", results);
//             resolve();
//           }
//         }
//       );
//     });
//   });
// }
// //header.shift();
// console.log(header);
// await new Promise((resolve, reject) => {
//   data
//     .filter(dt => dt.length > 0)
//     .forEach(async dt => {
//       connection.query(
//         `INSERT INTO ${tableName}(${header.join(",")}) VALUES('${dt
//           .split(",")
//           .slice(-Math.abs(header.length))
//           .join("','")}')`,
//         function(err, results, fields) {
//           if (err) {
//             console.log(err);
//             reject();
//           } else {
//             console.log("Added Data successfully ");
//             resolve();
//           }
//         }
//       );
//     });
// });

// await new Promise((resolve, reject) => {
//   connection.end(err => {
//     if (err) {
//       reject(err);
//     }
//     resolve();
//   });
// });

// function getColumnDef(col) {
//   return `${col} ${getType(col)}`;
// }

// function getType(col) {
//   switch (typeof col) {
//     case "string":
//       return "VARCHAR (255)";

//     case "number":
//       return "INT";

//     case "boolean":
//       return "bool";

//     default:
//       throw "NOT KNOWN";
//   }
// }

// context.succeed("Success");
