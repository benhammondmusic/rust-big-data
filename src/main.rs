use polars_core::prelude::*;
use polars_io::prelude::*;
// use std::fs::File;

fn example() -> PolarsResult<DataFrame> {
    return CsvReader::from_path("small_data.csv")?
        .has_header(true)
        .finish();
}

fn main() {
    let df = example().expect("oops");
    print!("{}", df.to_string());
}
