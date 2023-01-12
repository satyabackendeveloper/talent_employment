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

fs.createReadStream("employment.csv")

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
  const query_employment = 'INSERT INTO talent_employment (id, talent_id,company_id,position_name,position_id,experience_level_id,function_id,seniority_id,location_id,is_current,employment_start_date,employment_end_date,employment_duration_months,employment_type,updated_at,created_at) VALUES ($1, $2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING id'

  const empCountQuery = "SELECT id FROM talent_employment ORDER BY id DESC LIMIT 1";
  const total = await client.query(empCountQuery);
  console.log(total.rows[0].id)
  
  var empCount = total.rows[0].id;
  empCount++;
  var rowCount=0;
  for (let row of data) {
    try{
      console.log("row=> ",rowCount++)

      now = new Date().toISOString()

      const talent_id_query = "SELECT id FROM talent WHERE person_name='"+ row[0].split(",")[0] +"'";
      const found_id = await client.query(talent_id_query);
      console.log(found_id.rows[0].id)
      talent_id = found_id.rows[0].id

      console.log("company=> ",row[3].split(",")[0].trim())
      if(row[3].split(",")[0].trim()!=''){
        const company_id_query = "SELECT id FROM company WHERE company_name='"+ row[3].split(",")[0].trim() +"'";
        const found_company = await client.query(company_id_query);
        if(found_company.rows[0]!=undefined)
          company_id = found_company.rows[0].id
        else
          company_id = null
        
  
        const position_id_query = "SELECT id FROM position WHERE position_name='"+ row[2].split(",")[0].trim() +"'";
        const found_position = await client.query(position_id_query);
        if(found_position.rows[0]!=undefined)
          position_id = found_position.rows[0].id
        else
          position_id = null
  
        position_name = row[1]
        experience_level_id = null
        function_id = null
        seniority_id =null
        location_id = null
        is_current = null
        employment_start_date = null
        employment_end_date = null
        employment_duration_months = null
        employment_type = null
        created_at = now 
        updated_at = now
  
        try {
          const values = [empCount++,talent_id,company_id,position_name,position_id,experience_level_id,function_id,seniority_id,location_id,is_current,employment_start_date,employment_end_date,employment_duration_months,employment_type,created_at,updated_at]
          const res = await client.query(query_employment, values)
          console.log("insertd ",res.rows[0].id)
        } catch (err) {
          console.log(err.stack)
        }
    
      }

      
      
    }catch(ex){
      console.log(ex.stack)
    }
    

    //console.log(insertCount++,person_name,first_name,last_name,photo_url,phone_number,linkedin_url,linkedin_id,facebook_url,facebook_id,twitter_url,twitter_username,github_url,github_username,location_id,summary,is_recruiter,created_at,updated_at)
  }
}

