use polars::{
    lazy::dsl::{col, lit, when},
    prelude::{LazyCsvReader, LazyFileListReader, LazyFrame, UnionArgs},
};
use polars_core::prelude::*;
use polars_io::{prelude::CsvWriter, SerWriter};

// const TEST_FILENAME_00: &str = "spark_part-00001-3146a6b9-0113-41df-a776-bfddb9bfce06-c000.csv";

// Same fake data as currently used by cdc_restricted_local
const TEST_FILENAME_PART_1: &str = "COVID_Cases_Restricted_Detailed_04302021_Part_1.csv";
const TEST_FILENAME_PART_2: &str = "COVID_Cases_Restricted_Detailed_04302021_Part_2.csv";

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

fn process_lazyframe_into_aggregated_df(lf: LazyFrame) -> Result<DataFrame, PolarsError> {
    let known_sex_groups = vec!["Male", "Female", "Other"];
    let know_sex_series = Series::new("KNOWN_SEX_GROUP", known_sex_groups);
    let is_known_sex_group = col("sex").is_in(lit(know_sex_series));

    let groupby_cols = vec![
        col("time_period"),
        col("state_postal"),
        // col("race"),
        // col("ethnicity"),
        // col("age_group"),
        col("sex"),
    ];

    let df = lf
        .rename(["res_state"], ["state_postal"])
        // "time_period" as cdc col with only YYYY-MM
        .with_column((col("cdc_case_earliest_dt").str().str_slice(0, Some(7))).alias("time_period"))
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

fn main() {
    match read_csvs_as_lazyframe() {
        Ok(lazy_frame) => {
            println!("LazyFrame loaded successfully!");
            match process_lazyframe_into_aggregated_df(lazy_frame) {
                Ok(mut df) => {
                    println!("LazyFrame Processed and Aggregated successfully!");
                    println!("{:?}", df);

                    let sort_cols = [
                        "time_period",
                        "state_postal",
                        // "race",
                        // "age_group",
                        "sex",
                    ];

                    df = df
                        .sort(sort_cols, false, false)
                        .expect("Problem sorting df");
                    println!("{:?}", df);

                    let mut file = std::fs::File::create("results.csv").unwrap();
                    CsvWriter::new(&mut file).finish(&mut df).unwrap();
                }
                Err(err) => {
                    // Handle the error if there was any
                    eprintln!("Error: {:?}", err);
                }
            }
        }
        Err(err) => {
            // Handle the error if there was any
            eprintln!("Error: {:?}", err);
        }
    }
}

/*
SOURCE DATA COLUMNS

cSpell:disable
abdom_yn,
abxchest_yn,
acuterespdistress_yn,
age_group,
cdc_case_earliest_dt,
cdc_report_dt,
chills_yn,
county_fips_code,
cough_yn,
current_status,
death_yn,
diarrhea_yn,
fever_yn,
sfever_yn,
hc_work_yn,
headache_yn,
hosp_yn,
icu_yn,
mechvent_yn,
medcond_yn,
myalgia_yn,
nauseavomit_yn,
onset_dt,
pna_yn,
pos_spec_dt,
race,
ethnicity,
res_county,
res_state,
runnose_yn,
sex,
sob_yn,
sthroat_yn

cSpell:enable

HET SEX STATE COLUMNS
state_postal,
sex,
time_period,
cases,
hosp_y,
hosp_n,
hosp_unknown,
death_y,
death_n,
death_unknown

*/
