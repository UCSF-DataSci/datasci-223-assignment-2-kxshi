import polars as pl
import os

def analyze_patient_cohorts(input_file: str) -> pl.DataFrame:
    """
    Analyze patient cohorts based on BMI ranges.
    
    Args:
        input_file: Path to the input CSV file
        
    Returns:
        DataFrame containing cohort analysis results with columns:
        - bmi_range: The BMI range (e.g., "Underweight", "Normal", "Overweight", "Obese")
        - avg_glucose: Mean glucose level by BMI range
        - patient_count: Number of patients by BMI range
        - avg_age: Mean age by BMI range
    """
    # BUG: Should check if the file exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"The file {input_file} does not exist.")

    # Convert CSV to Parquet for efficient processing
    pl.read_csv(input_file).write_parquet("patients_large.parquet")

    lazy_df = pl.scan_parquet("patients_large.parquet")

    # BUG: Doesn't check for presence/structure of input
    # FIX: Check if BMI, Glucose, and Age columns exist and are numeric
    required_cols = ["BMI", "Glucose", "Age"]
    schema = lazy_df.collect_schema()
    
    missing = [col for col in required_cols if col not in schema]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    numeric_types = (pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                     pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                     pl.Float32, pl.Float64)

    non_numeric = [col for col in required_cols if not isinstance(schema[col], numeric_types)]
    if non_numeric:
        raise TypeError(f"Found non-numeric column: {non_numeric}")
    
    # Create a lazy query to analyze cohorts
    cohort_results = lazy_df.pipe(
        lambda df: df.filter((pl.col("BMI") >= 10) & (pl.col("BMI") <= 60))
    ).pipe(
        lambda df: df.select(["BMI", "Glucose", "Age"])
    ).pipe(
        lambda df: df.with_columns(
            pl.col("BMI").cut(
                # BUG: Mismatch between number of breaks and labels, the labels should be 1 more than the number of breaks
                # FIX: Remove the lower and upper breaks (since we are already filtering between 10 and 60 as above)
                breaks=[18.5, 25, 30],
                labels=["Underweight", "Normal", "Overweight", "Obese"],
                left_closed=True
            ).alias("bmi_range")
        )
    ).pipe(
        # BUG: groupby() doesn't exist for LazyFrame objects
        # FIX: use group_by() - homologous function for LazyFrames
        lambda df: df.group_by("bmi_range").agg([
            pl.col("Glucose").mean().alias("avg_glucose"),
            # BUG: pl.count() is apparently deprecated
            # FIX: use pl.len()
            pl.len().alias("patient_count"),
            pl.col("Age").mean().alias("avg_age")
        ])
        # BUG: Apparently streaming is going to be deprecated
        # FIX: Best practice is to remove the streaming argument (polars will use its default functionality)
    ).collect()
    
    return cohort_results

def main():
    # Input file
    input_file = "patients_large.csv"
    
    # Run analysis
    results = analyze_patient_cohorts(input_file)
    
    # Print summary statistics
    print("\nCohort Analysis Summary:")
    print(results)

if __name__ == "__main__":
    main() 
