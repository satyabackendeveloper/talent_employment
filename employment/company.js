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

fs.createReadStream("company.csv")

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
  const text = 'INSERT INTO company (id, company_name, industry_id, sector_id, company_product_id, company_website_url, company_domainl, company_linkedin_url, company_facebook_url, company_twitter_url, company_logo_url, company_size_min, company_size_max, company_found_year, company_total_funding_amount, company_latest_funding_amount, company_latest_funding_type, company_latest_funding_date, company_annual_revenue_amount, company_description, company_keywords, company_size, company_review_rating, created_at, updated_at ) VALUES ($1, $2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)';

  const queryTotalState = "SELECT id FROM company ORDER BY id DESC LIMIT 1";
  const totalState = await client.query(queryTotalState);
  console.log(totalState.rows[0].id)
  
  var insertCount = totalState.rows[0].id;
  insertCount++;
  var rowCount=0;
  for (let row of data) {
    try{

      if(row[0]!=''){

      
      console.log("row ",rowCount++)
      const company_id_query = "SELECT COUNT(*) FROM company WHERE company_name='"+ row[0].split(",")[0] +"'";
      const company_found_id = await client.query(company_id_query);
      console.log(company_found_id.rows[0].count)
      if(company_found_id.rows[0].count==0){
        now = new Date().toISOString()
        const industry_id_query = "SELECT id FROM industry WHERE industry_name='"+ row[1].split(",")[0] +"'";
        const found_id = await client.query(industry_id_query);
        
        if(found_id.rows[0]!=undefined){
          industry_id = found_id.rows[0].id
          console.log(found_id.rows[0].id)
        }
        else
          industry_id = null
        company_name = row[0]
        sector_id = null
        company_product_id = null
        company_website_url = row[2]
        company_domainl = null
        company_linkedin_url = row[8]
        company_facebook_url = null
        company_twitter_url= null
        company_logo_url= row[9]
        try{
          company_size_min = row[3]
        }catch(e){
          company_size_min = null
        }
        try{
          company_size_max = row[4]
        }catch(e){
          company_size_max = null
        }
        
        company_found_year = null
        company_total_funding_amount = null
        company_latest_funding_amount = null
        company_latest_funding_type = null
        company_latest_funding_date = null
        company_annual_revenue_amount = null
        company_description = null
        company_keywords = null
        company_size = null
        company_review_rating = null
        created_at = now 
        updated_at = now
        try {
          const values = [insertCount++,company_name,industry_id,sector_id, company_product_id, company_website_url, company_domainl, company_linkedin_url, company_facebook_url, company_twitter_url, company_logo_url, company_size_min, company_size_max, company_found_year, company_total_funding_amount, company_latest_funding_amount, company_latest_funding_type, company_latest_funding_date, company_annual_revenue_amount, company_description, company_keywords, company_size, company_review_rating, created_at, updated_at]
          const res = await client.query(text, values)
          console.log(res.rows[0])
        } catch (err) {
          console.log(row[0],err.stack)
          
        }
      }

      
    }
      
    }catch(ex){
      console.log(ex.stack)
    }
    

    //console.log(insertCount++,person_name,first_name,last_name,photo_url,phone_number,linkedin_url,linkedin_id,facebook_url,facebook_id,twitter_url,twitter_username,github_url,github_username,location_id,summary,is_recruiter,created_at,updated_at)
  }
}

