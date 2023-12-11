//user prompts
const { on } = require('events');
const readline = require('readline');

//read csv
const fs = require('fs');
const { parse } = require('csv-parse');

//insert csv into db
const sqlite3 = require("sqlite3").verbose();
const filepath = "./population.db";

//write csv
const { stringify } = require("csv-stringify");


doItAgain();

function doItAgain(){
    const prompt = 'Press:\n1: for reading CSV\n2: for inserting CSV into sqlite3 DB\n3: for writing CSV\n4: for a test CSV write\n5: to append test CSV\n6: to exit\n';
    const r1 = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
    });

    r1.question(prompt, userInput => {
        switch(userInput) {
            case '1':
                readCSV();
                break;
            case '2':
                insertCSVDB();
                break;
            case '3':
                writeCSV();
                break;
            case '4':
                testWrite();
                break;
            case '5':
                appendTestCSV();
                r1.close();
                doItAgain();
            case '6':
                testOut(userInput);
                break;
            default:
                console.log('Didn\'t recognize input:', userInput);
                r1.close();
                doItAgain();
        }
        r1.close();
    });
}


function appendTestCSV() {
  const filename = "testWrite.csv";
  const writableStream = fs.createWriteStream(filename);

  //sample CSV data
  const d = new Date().toISOString();
  const testMac = '7C-7A-91-8C-2A-80';
  const testLat = 50.121592508766014;
  const testLng = 8.671025307969714;
  const testUnc = 33;

  const stringifier = stringify({ delimiter: ',' });
  stringifier.write([ d, testMac, testLat, testLng, testUnc ]);
  stringifier.pipe(writableStream);

  stringifier.end();
  writableStream.close();
  console.log("Finished appending test CSV data");
}

function testWrite() {
  const filename = "testWrite.csv";
  const writableStream = fs.createWriteStream(filename);
  const columns = [
    "timestamp",
    "clientMac",
    "latitude",
    "longitude",
    "uncertainty",
  ];

  //sample CSV data
  const d = new Date().toISOString();
  const testMac = '7C-7A-91-8C-2A-80';
  const testLat = 50.121592508766014;
  const testLng = 8.671025307969714;
  const testUnc = 33;

  const stringifier = stringify({ header: true, columns: columns });
  stringifier.write([ d, testMac, testLat, testLng, testUnc ]);
  stringifier.pipe(writableStream);

  stringifier.end();
  writableStream.close();
  console.log("Finished inserting test CSV data");
}

function writeCSV() {
  const filename = "saved_from_db.csv";
  const writableStream = fs.createWriteStream(filename);
  const columns = [
    "year_month",
    "month_of_release",
    "passenger_type",
    "direction",
    "sex",
    "age",
    "estimate",
  ];

  const db = connectToDatabase();
  const stringifier = stringify({ header: true, columns: columns });
  db.each(`select * from migration`, (error, row) => {
    if (error) {
      return console.log(error.message);
    }
    stringifier.write(row);
  });
  stringifier.pipe(writableStream);

  stringifier.end();
  writableStream.close();
  console.log("Finished writing data");
}

function insertCSVDB() {
  const db = connectToDatabase();
  fs.createReadStream("./migration_data.csv")
  .pipe(parse({ delimiter: ",", from_line: 2 }))
  .on("data", function (row) {
    db.serialize(function () {
      db.run(
        `INSERT INTO migration VALUES (?, ?, ? , ?, ?, ?, ?)`,
        [row[0], row[1], row[2], row[3], row[4], row[5], row[6]],
        function (error) {
          if (error) {
            return console.log(error.message);
          }
          console.log(`Inserted a row with the id: ${this.lastID}`);
        }
      );
    });
  });
}

function connectToDatabase() {
    if (fs.existsSync(filepath)) {
      return new sqlite3.Database(filepath);
    } else {
      const db = new sqlite3.Database(filepath, (error) => {
        if (error) {
          return console.error(error.message);
        }
        createTable(db);
        console.log("Connected to the database successfully");
      });
      return db;
    }
}

function createTable(db) {
  db.exec(`
    CREATE TABLE migration
    (
      year_month       VARCHAR(10),
      month_of_release VARCHAR(10),
      passenger_type   VARCHAR(50),
      direction        VARCHAR(20),
      sex              VARCHAR(10),
      age              VARCHAR(50),
      estimate         INT
    )
  `);
}

function readCSV() {
  //reading csv file
  fs.createReadStream("./migration_data.csv")
  .pipe(parse({ delimiter: ",", from_line: 2 }))
  .on("data", (row) => {
      console.log(row);
  })
  .on("end", () => {
      console.log("finished");
  })
  .on("error", (error) => {
      console.log(error.message);
      return;
  });
}

function testOut(userInput) {
  const d = new Date().toISOString();
  console.log('this is what happens when you press', userInput, d);
}