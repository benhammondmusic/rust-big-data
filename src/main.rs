use polars::{
    lazy::dsl::{col, lit, when},
    prelude::{LazyCsvReader, LazyFileListReader},
};
use polars_core::prelude::*;
use polars_io::{prelude::CsvWriter, SerWriter};

// const TEST_FILENAME_00: &str = "spark_part-00001-3146a6b9-0113-41df-a776-bfddb9bfce06-c000.csv";

// Same fake data as currently used by cdc_restricted_local
const TEST_FILENAME_PART_1: &str = "COVID_Cases_Restricted_Detailed_04302021_Part_1.csv";
// const TEST_FILENAME_PART_2: &str = "COVID_Cases_Restricted_Detailed_04302021_Part_2.csv";

fn load_csv_and_agg() -> Result<DataFrame, PolarsError> {
    let test_filepath: String = format!("tests/fake_source_data/{TEST_FILENAME_PART_1}");
    let groupby_cols = vec![
        col("time_period"),
        col("res_state"),
        // col("race"),
        // col("ethnicity"),
        // col("age_group"),
        col("sex"),
    ];

    LazyCsvReader::new(test_filepath)
        .has_header(true)
        .with_quote_char(None)
        .finish()?
        // "time_period" as cdc col with only YYYY-MM
        .with_column((col("cdc_case_earliest_dt").str().str_slice(0, Some(7))).alias("time_period"))
        // "case_yn" counts every row with a valid date as 1 case
        .with_column(
            when(col("time_period").is_not_null())
                .then(lit(1))
                .otherwise(lit(0))
                .alias("cases"),
        )
        // "hosp_y" counts every row with a "Yes"
        .with_column(
            when(col("hosp_yn").eq(lit("Yes")))
                .then(lit(1))
                .otherwise(lit(0))
                .alias("hosp_y"),
        )
        // "hosp_n" counts every row with a "No"
        .with_column(
            when(col("hosp_yn").eq(lit("No")))
                .then(lit(1))
                .otherwise(lit(0))
                .alias("hosp_n"),
        )
        // "hosp_unknown" counts every row without a "Yes" or a "No"
        .with_column(
            col("hosp_yn")
                .neq(lit("Yes"))
                .and(col("hosp_yn").neq(lit("No")))
                .alias("hosp_unknown"),
        )
        // "death_y" counts every row with a "Yes"
        .with_column(
            when(col("death_yn").eq(lit("Yes")))
                .then(lit(1))
                .otherwise(lit(0))
                .alias("death_y"),
        )
        // "death_n" counts every row with a "No"
        .with_column(
            when(col("death_yn").eq(lit("No")))
                .then(lit(1))
                .otherwise(lit(0))
                .alias("death_n"),
        )
        // "death_unknown" counts every row without a "Yes" or a "No"
        .with_column(
            col("death_yn")
                .neq(lit("Yes"))
                .and(col("death_yn").neq(lit("No")))
                .alias("death_unknown"),
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
        .collect()
}

fn main() {
    let mut df = load_csv_and_agg().expect("Problem with load_csv_and_agg()");

    let sort_cols = [
        "time_period",
        "res_state",
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
