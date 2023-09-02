extern crate csv;
#[macro_use]
extern crate serde_derive;
// use chrono::NaiveDate;

use std::error::Error;
use std::fs::File;
use std::process;

// By default, struct field names are deserialized based on the position of
// a corresponding field in the CSV data's header record.
#[derive(Debug, Deserialize)]
struct Record {
    state_name: String,
    race: String,
    // column_3: String,
    // column_4: u32,
    // column_5: String,
}

fn example() -> Result<(), Box<dyn Error>> {
    let file = File::open("small_data.csv")?;
    let mut count_deaths = 0;

    let mut rdr = csv::Reader::from_reader(file);
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let record: Record = result?;
        // println!("{:?}", record);

        if record.state_name == "Colorado" && record.race == "White" {
            count_deaths += 1;
        }
    }

    println!("#: {}", count_deaths);

    Ok(())
}

fn main() {
    if let Err(err) = example() {
        println!("error running example: {}", err);
        process::exit(1);
    }
}
