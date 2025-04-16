# HW2 Analysis

## Explanation of analysis approach

In my cohort analysis, the workflow is the following:
1. Read in the data
  a. Leverage the polars library (more efficient data manipulation operations)
  b. Leverage streaming functionality (saves memory)
2. Check if the relevant data exists (Age, BMI, Glucose)
3. Separate and summarize data by BMI_range (Underweight, Normal, Overweight, etc.)

## Patterns or Insights found
1. I'm familiar with the pipe infrastructure from R, though using lambda functions was new
2. I think the code here would kind of do well in a functional language like R
3. The cut() issue with the LazyFrame was tricky

## Polars' features
1. To be honest I have essentially 0 Python experience outside of this class so I don't know what exactly the efficiency or performance gains are. Reading documentation it seems like the Polars library has more support for parallel operations and is structured/indexed more efficiently