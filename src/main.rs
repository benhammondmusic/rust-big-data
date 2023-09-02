use polars::{
    lazy::dsl::col,
    prelude::{LazyCsvReader, LazyFileListReader},
};
use polars_core::prelude::*;
use polars_io::{prelude::CsvWriter, SerWriter};

fn load_csv_and_agg() -> Result<DataFrame, PolarsError> {
    LazyCsvReader::new("small_data.csv")
        .has_header(true)
        .finish()?
        .groupby(vec![col("state_name"), col("race")])
        .agg(vec![col("case_count").sum()])
        .sort(
            "state_name",
            SortOptions {
                descending: false,
                nulls_last: true,
                multithreaded: false,
                maintain_order: false,
            },
        )
        .collect()
}

fn main() {
    let mut df = load_csv_and_agg().expect("oops");
    println!("{:?}", df);

    let mut file = std::fs::File::create("results.csv").unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();
}
