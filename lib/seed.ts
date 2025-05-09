import { sql } from '@vercel/postgres';
import fs from 'fs';
import csv from 'csv-parser';
import path from 'path';
import "dotenv/config"

// This function will parse the date from the CSV file
// The date format in the CSV file is DD/MM/YYYY
function parseDate(dateString: string): string {
  // The date format in the CSV file is DD/MM/YYYY
  // We need to convert it to YYYY-MM-DD
  //create a regex to match the date format 
  // and extract the day, month and year
  const parts = dateString.split('/');
  //if parts.length is not 3, then we cannot parse the date
  if (parts.length === 3) {
    //the day is paded with parts[0] with 2 zeros
    const day = parts[0].padStart(2, '0');
    //the month is paded with parts[1] with 2 zeros
    const month = parts[1].padStart(2, '0');
    //the year is parts[2]
    const year = parts[2];
    //  YYYY-MM-DD format
    return `${year}-${month}-${day}`;
  }
  console.warn(`Could not parse date: ${dateString}`); //
  throw Error();
}

// This function will create a table called unicorns if it doesn't exist
export async function seed() {
  const createTable = await sql`
    CREATE TABLE IF NOT EXISTS unicorns (
      id SERIAL PRIMARY KEY,
      company VARCHAR(255) NOT NULL UNIQUE,
      valuation DECIMAL(10, 2) NOT NULL,
      date_joined DATE,
      country VARCHAR(255) NOT NULL,
      city VARCHAR(255) NOT NULL,
      industry VARCHAR(255) NOT NULL,
      select_investors TEXT NOT NULL
    );
  `;

  console.log(`Created "unicorns" table`);

  // Read CSV file and parse it, it should reside in the root directory, which means
  // the same directory as the package.json file
  const results: any[] = [];
  // The path to the CSV file is relative to the current working directory
  // which is the root directory of the project. As such, we can use process.cwd()
  // to get the current working directory and then join it with the file name
  // to get the full path to the file.
  // The CSV file should be in the root directory of the project
  // and should be named unicorns.csv
  const csvFilePath = path.join(process.cwd(), 'unicorns.csv');

  // Check if the file exists, if not, throw an error.
  // resolve to the file path, if it exists.
  await new Promise((resolve, reject) => {
    //createReadStream is a method of the fs module that creates a readable stream
    // here csvFilePath is the path to the csv file
    fs.createReadStream(csvFilePath)
      .pipe(csv()) //pipe is a method of the stream module that pipes the readable stream to the writable stream
      .on('data', (data) => results.push(data)) //on is a method of the stream module that listens for events
      // here data is the event that is emitted when a chunk of data is available to read
      .on('end', resolve) //end is a method of the stream module that listens for the end event
      .on('error', reject);
  });

  // the row of results is an array of objects, where each object is a row in the CSV file
  for (const row of results) {
    // The row is an object with the following properties:
    // company, valuation, date_joined, country, city, industry, select_investors
    const formattedDate = parseDate(row['Date Joined']); //parseDate is a function that parses the date from the CSV file

    //
    await sql`
      INSERT INTO unicorns (company, valuation, date_joined, country, city, industry, select_investors)
      VALUES (
        ${row.Company},
        ${parseFloat(row['Valuation ($B)'].replace('$', '').replace(',', ''))},
        ${formattedDate},
        ${row.Country},
        ${row.City},
        ${row.Industry},
        ${row['Select Investors']}
      )
      ON CONFLICT (company) DO NOTHING;
    `;
  }

  console.log(`Seeded ${results.length} unicorns`);

  return {
    createTable,
    unicorns: results,
  };
}


seed().catch(console.error);