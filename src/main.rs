use std::env;

use rust_big_data::run;

fn main() {
    env::set_var("POLARS_FMT_MAX_ROWS", "-1");
    env::set_var("POLARS_FMT_MAX_COLS", "-1");

    run()
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
