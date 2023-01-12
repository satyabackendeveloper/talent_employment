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

fs.createReadStream("emp_location.csv")

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
  const insert_location = 'INSERT INTO location (id, country_id, region_id, state_id, city_id, zipcode, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id'
  
  const queryTotalLocation = "SELECT id FROM company ORDER BY id DESC LIMIT 1";
  const totalLocation = await client.query(queryTotalLocation);
  console.log(totalLocation.rows[0].id)
  var insertCount = totalLocation.rows[0].id;
  insertCount++;

  var rowCount=0;
  for (let row of data) {
    try{
      console.log("row=> ",rowCount++)

      const select_city_id = `SELECT id FROM city WHERE city_name='`+ row[3].split(",")[0] +`'`;
      const result_city = await client.query(select_city_id);
      
      const select_state_id = `SELECT id FROM state WHERE state_name='`+ row[4].split(",")[0] +`'`;
      const result_state = await client.query(select_state_id);
      
      const select_country_id = `SELECT id FROM country WHERE country_name='`+ row[5].split(",")[0] +`'`; 
      const result_country = await client.query(select_country_id);
      
      console.log("query ",select_city_id)
      console.log("query ",select_state_id)
      console.log("query ",select_country_id)

      const select_location_id = `SELECT * FROM location WHERE 
      city_id = ('`+ result_city.rows[0].id +`') AND
      state_id = ('`+ result_state.rows[0].id +`') AND
      country_id = ('`+ result_country.rows[0].id +`') `;

      console.log("query ",select_location_id)
      const location_found_id = await client.query(select_location_id);
      console.log("found ",location_found_id.rows[0])

      if(location_found_id.rows[0] != undefined){
        if(location_found_id.rows[0].id != undefined){

          // find and update lcoation in table
          const select_talent_emp_id = `SELECT * FROM talent_employment WHERE 
          talent_id = (SELECT id FROM talent WHERE person_name='`+ row[0].split(",")[0] +`') AND
          company_id = (SELECT id FROM company WHERE company_name='`+ row[2].split(",")[0] +`') AND
          position_id = (SELECT id FROM position WHERE position_name='`+ row[1].split(",")[0] +`') `;
          console.log("query ",select_talent_emp_id)
          const found_id = await client.query(select_talent_emp_id);
          console.log("found ",found_id.rows[0])
          if(found_id.rows[0] != undefined){
            if(found_id.rows[0].id != undefined){
              const update_talent_emp = `UPDATE talent_employment SET location_id = '`+ location_found_id.rows[0].id +`' WHERE id = `+ found_id.rows[0].id +``;
              const update_id = await client.query(update_talent_emp);
              console.log("insertd ",update_id.rows[0])
            }
          }
          
        }
      }else{

        // insert new location in table
        now = new Date().toISOString()
        country_id = result_country.rows[0].id
        region_id = null 
        state_id = result_state.rows[0].id
        city_id = result_city.rows[0].id
        zipcode = null
        created_at = now 
        updated_at = now
        try {
          const values = [insertCount++,country_id, region_id, state_id, city_id, zipcode, created_at, updated_at]
          const res = await client.query(insert_location, values)
          console.log(res.rows[0])

          // find and update lcoation in table
          const select_talent_emp_id = `SELECT * FROM talent_employment WHERE 
          talent_id = (SELECT id FROM talent WHERE person_name='`+ row[0].split(",")[0] +`') AND
          company_id = (SELECT id FROM company WHERE company_name='`+ row[2].split(",")[0] +`') AND
          position_id = (SELECT id FROM position WHERE position_name='`+ row[1].split(",")[0] +`') `;
          console.log("insertd ",select_talent_emp_id)
          const found_id = await client.query(select_talent_emp_id);
          console.log("insertd ",found_id.rows[0])
          if(found_id.rows[0] != undefined){
            if(found_id.rows[0].id != undefined){
              const update_talent_emp = `UPDATE talent_employment SET location_id = '`+ res.rows[0].id +`' WHERE id = `+ found_id.rows[0].id +``;
              const update_id = await client.query(update_talent_emp);
              console.log("insertd ",update_id.rows[0])
            }
          }

        } catch (err) {
          console.log(row[0],err.stack)
          
        }

      }
      

    }catch(ex){
      console.log(ex.stack)
    }
    
  }
}

