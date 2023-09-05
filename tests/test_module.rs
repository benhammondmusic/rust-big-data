#[cfg(test)]
mod tests {
    use polars::prelude::{LazyCsvReader, LazyFileListReader};
    use std::env;

    #[test]
    fn test_compare_new_rust_results_to_old_python_golden_data() {
        env::set_var("POLARS_FMT_MAX_ROWS", "-1");
        env::set_var("POLARS_FMT_MAX_COLS", "-1");
        let rust_results = "RESULTS---cdc_restricted_by_sex_state.csv";
        let golden_data_from_python_by_sex_state =
            "tests/golden_data/cdc_restricted_by_sex_state.csv";

        // Load CSV files into dataframes
        let by_sex_lf = LazyCsvReader::new(rust_results)
            .has_header(true)
            .finish()
            .expect("Failed to read CSV file: RESULTS---cdc_restricted_by_sex_state.csv");

        let by_sex_df = by_sex_lf.collect().unwrap();

        let expected_by_sex_python_lf = LazyCsvReader::new(golden_data_from_python_by_sex_state)
            .has_header(true)
            .finish()
            .expect("Failed to read CSV file: tests/golden_data/cdc_restricted_by_sex_state.csv");

        let expected_by_sex_python_df = expected_by_sex_python_lf.collect().unwrap();

        // for (col1, col2) in by_sex_df.iter().zip(expected_by_sex_python_df.iter()) {
        //     for (row_i, (val1, val2)) in col1.iter().zip(col2.iter()).enumerate() {
        //         assert_eq!(val1, val2, "At row {}, {} != {}", row_i, val1, val2,)
        //     }
        // }

        // Assert the equality of the dataframes
        assert_eq!(by_sex_df, expected_by_sex_python_df);
    }
}
