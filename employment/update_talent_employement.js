const fs = require("fs");
const fastCsv = require("fast-csv");
const Pool = require("pg").Pool;

const options = {
  columns: true,
  //objectMode: true,
  //delimiter: ",",
  //quote: null,
  //headers: true,
  //renameHeaders: false,
};

const data = [];

fs.createReadStream("emp1.csv")

  .pipe(fastCsv.parse(options))

  .on("error", (error) => {
    console.log(error);
  })

  .on("data", (row) => {
    data.push(row);
  })

  .on("end", (rowCount) => {
    // remove the first line: header
    data.shift();

    console.log(rowCount);
    console.log(data.length);
    //console.log(data[0]);
    //console.log(data);

    // create a new connection to the database
  const pool = new Pool({
    host: "hirebeat-profile-instance.c9lge1nbo9rx.us-east-1.rds.amazonaws.com",
    user: "postgres",
    database: "hirebeat_profiles",
    password: "4xrSs3KSskkYb1mIBmTB",
    port: 5432
  });

    pool.connect((err, client, done) => {
    if (err) throw err;

    try{
        insertNewState(data,client)
       
    }catch(err){
      console.log(err)
    }
    
    });

});

async function insertNewState(data,client) {
  
  var rowCount=0;
  for (let row of data) {
    try{
      console.log("row=> ",rowCount++)

      const select_talent_emp_id = `SELECT * FROM talent_employment WHERE 
      talent_id = (SELECT id FROM talent WHERE person_name='`+ row[0].split(",")[0] +`') AND
      company_id = (SELECT id FROM company WHERE company_name='`+ row[2].split(",")[0] +`') AND
      position_id = (SELECT id FROM position WHERE position_name='`+ row[1].split(",")[0] +`') `;
      console.log("insertd ",select_talent_emp_id)
      const found_id = await client.query(select_talent_emp_id);
      console.log("insertd ",found_id.rows[0])

      if(found_id.rows[0] != undefined){
        if(found_id.rows[0].id != undefined){
          const update_talent_emp = `UPDATE talent_employment SET is_current = '`+ true +`' WHERE id = `+ found_id.rows[0].id +``;
          const update_id = await client.query(update_talent_emp);
          console.log("insertd ",update_id.rows[0])
        }
      }
      

    }catch(ex){
      console.log(ex.stack)
    }
    
  }
}

