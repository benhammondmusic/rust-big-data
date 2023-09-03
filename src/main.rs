use polars::{
    lazy::dsl::{col, lit, when},
    prelude::{LazyCsvReader, LazyFileListReader},
};
use polars_core::prelude::*;
use polars_io::{prelude::CsvWriter, SerWriter};

fn load_csv_and_agg() -> Result<DataFrame, PolarsError> {
    let groupby_cols = vec![
        col("date"),
        col("state_name"),
        col("race"),
        col("age"),
        col("sex"),
    ];

    LazyCsvReader::new("small_data.csv")
        .has_header(true)
        .finish()?
        .with_column(
            when(col("date").is_not_null())
                .then(lit(1))
                .otherwise(lit(0))
                .alias("case_yn"),
        )
        .groupby(groupby_cols)
        .agg(vec![
            col("case_yn").sum(),
            col("hosp_yn").sum(),
            col("death_yn").sum(),
        ])
        .collect()
}

fn main() {
    let mut df = load_csv_and_agg().expect("oops");

    let sort_cols = ["date", "state_name", "race", "age", "sex"];

    df = df.sort(sort_cols, false, false).expect("oops");
    println!("{:?}", df);

    let mut file = std::fs::File::create("results.csv").unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();
}
