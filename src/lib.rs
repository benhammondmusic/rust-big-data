use polars::{
    lazy::dsl::{col, lit, when},
    prelude::{LazyCsvReader, LazyFileListReader, LazyFrame, UnionArgs},
};
use polars_core::prelude::*;
use polars_io::{
    prelude::{CsvWriter, NullValues},
    SerWriter,
};

// const TEST_FILENAME_00: &str = "spark_part-00001-3146a6b9-0113-41df-a776-bfddb9bfce06-c000.csv";

// Same fake data as currently used by cdc_restricted_local
const TEST_FILENAME_PART_1: &str = "COVID_Cases_Restricted_Detailed_04302021_Part_1.csv";
const TEST_FILENAME_PART_2: &str = "COVID_Cases_Restricted_Detailed_04302021_Part_2.csv";

pub fn run() -> Result<(), PolarsError> {
    let lazy_frame = read_csvs_as_lazyframe()?;
    println!("LazyFrame loaded successfully!");

    // ALLS
    let alls_df = process_lazyframe_into_alls_df(lazy_frame.clone())?;
    println!("LazyFrame Processed and Aggregated successfully!");
    println!("alls");
    println!("{:?}", alls_df);
    // let mut file = std::fs::File::create("alls_results.csv").unwrap();
    // CsvWriter::new(&mut file).finish(&mut alls_df)?;

    // SEX
    let mut by_sex_df = process_lazyframe_into_by_sex_df(lazy_frame)?;
    println!("LazyFrame Processed and Aggregated successfully!");
    println!("sex");
    println!("{:?}", by_sex_df);
    // let mut file = std::fs::File::create("sex_results.csv").unwrap();
    // CsvWriter::new(&mut file).finish(&mut by_sex_df)?;

    // vertically add the ALLS rows to the BY SEX rows
    by_sex_df = by_sex_df
        .vstack(&alls_df)
        .expect("Problem vertically combining the ALLS rows with the BY_SEX groups rows");
    let sort_cols = ["state_postal", "sex"];
    by_sex_df = by_sex_df
        .sort(sort_cols, false, false)
        .expect("Problem sorting by_sex_df");
    println!("sex+alls");
    println!("{:?}", by_sex_df);

    // write to csv
    let mut file = std::fs::File::create("RESULTS---cdc_restricted_by_sex_state.csv")
        .expect("Problem writing RESULTS---cdc_restricted_by_sex_state.csv");
    CsvWriter::new(&mut file).finish(&mut by_sex_df)?;

    Ok(())
}

fn read_csvs_as_lazyframe() -> Result<LazyFrame, PolarsError> {
    let test_filepath1: String = format!("tests/fake_source_data/{TEST_FILENAME_PART_1}");
    let test_filepath2: String = format!("tests/fake_source_data/{TEST_FILENAME_PART_2}");

    // List of CSV file paths
    let file_paths = vec![test_filepath1, test_filepath2];

    // Create an empty Vec to store the LazyFrames
    let mut lazy_frames: Vec<LazyFrame> = Vec::new();

    // Read each CSV file into a LazyFrame and store them in the Vec
    for file_path in file_paths.iter() {
        let lf = LazyCsvReader::new(file_path)
            .has_header(true)
            .with_quote_char(None)
            .with_null_values(Some(NullValues::AllColumnsSingle(String::from("NA"))))
            .finish()?;
        lazy_frames.push(lf);
    }

    // Concatenate the LazyFrames into a single LazyFrame
    let combined_lazy_frame = polars::prelude::concat(
        lazy_frames,
        UnionArgs {
            parallel: false,
            rechunk: false,
            to_supertypes: false,
        },
    )?;

    Ok(combined_lazy_frame)
}

fn process_lazyframe_into_by_sex_df(lf: LazyFrame) -> Result<DataFrame, PolarsError> {
    let known_sex_groups = vec!["Male", "Female", "Other"];
    let know_sex_series = Series::new("KNOWN_SEX_GROUP", known_sex_groups);
    let is_known_sex_group = col("sex").is_in(lit(know_sex_series));

    let groupby_cols = vec![col("state_postal"), col("sex"), col("time_period")];

    let df = lf
        // drop rows with missing geography
        .filter(col("res_state").neq(lit("Missing")))
        .rename(["res_state"], ["state_postal"])
        // "time_period" as cdc col with only YYYY-MM
        .with_column((col("cdc_case_earliest_dt").str().str_slice(0, Some(7))).alias("time_period"))
        // count every row as 1 case
        .with_column(col("time_period").is_not_null().alias("cases"))
        .with_column(col("hosp_yn").eq(lit("Yes")).alias("hosp_y"))
        .with_column(col("hosp_yn").eq(lit("No")).alias("hosp_n"))
        .with_column(
            col("hosp_yn")
                .neq(lit("Yes"))
                .and(col("hosp_yn").neq(lit("No")))
                .alias("hosp_unknown"),
        )
        .with_column(col("death_yn").eq(lit("Yes")).alias("death_y"))
        .with_column(col("death_yn").eq(lit("No")).alias("death_n"))
        .with_column(
            col("death_yn")
                .neq(lit("Yes"))
                .and(col("death_yn").neq(lit("No")))
                .alias("death_unknown"),
        )
        // only keep Male/Female/Other/Unknown options for sex
        .with_column(
            when(is_known_sex_group)
                .then(col("sex"))
                .otherwise(lit("Unknown")),
        )
        .groupby(groupby_cols)
        .agg(vec![
            col("cases").sum(),
            col("hosp_y").sum(),
            col("hosp_n").sum(),
            col("hosp_unknown").sum(),
            col("death_y").sum(),
            col("death_n").sum(),
            col("death_unknown").sum(),
        ])
        .collect();

    df
}

fn process_lazyframe_into_alls_df(lf: LazyFrame) -> Result<DataFrame, PolarsError> {
    let groupby_cols = vec![col("state_postal"), col("sex"), col("time_period")];

    let df = lf
        // drop rows with missing geography
        .filter(col("res_state").neq(lit("Missing")))
        .rename(["res_state"], ["state_postal"])
        // "time_period" as cdc col with only YYYY-MM
        .with_column((col("cdc_case_earliest_dt").str().str_slice(0, Some(7))).alias("time_period"))
        // count every row as 1 case
        .with_column(col("time_period").is_not_null().alias("cases"))
        .with_column(col("hosp_yn").eq(lit("Yes")).alias("hosp_y"))
        .with_column(col("hosp_yn").eq(lit("No")).alias("hosp_n"))
        .with_column(
            col("hosp_yn")
                .neq(lit("Yes"))
                .and(col("hosp_yn").neq(lit("No")))
                .alias("hosp_unknown"),
        )
        .with_column(col("death_yn").eq(lit("Yes")).alias("death_y"))
        .with_column(col("death_yn").eq(lit("No")).alias("death_n"))
        .with_column(
            col("death_yn")
                .neq(lit("Yes"))
                .and(col("death_yn").neq(lit("No")))
                .alias("death_unknown"),
        )
        .with_column(lit("All").alias("sex"))
        .groupby(groupby_cols)
        .agg(vec![
            col("cases").sum(),
            col("hosp_y").sum(),
            col("hosp_n").sum(),
            col("hosp_unknown").sum(),
            col("death_y").sum(),
            col("death_n").sum(),
            col("death_unknown").sum(),
        ])
        .collect();

    df
}
