#!/usr/bin/env python3
"""
Patient Data Cleaner

This script standardizes and filters patient records according to specific rules:

Data Cleaning Rules:
1. Names: Capitalize each word (e.g., "john smith" -> "John Smith")
2. Ages: Convert to integers, set invalid ages to 0
3. Filter: Remove patients under 18 years old
4. Remove any duplicate records

Input JSON format:
    [
        {
            "name": "john smith",
            "age": "32",
            "gender": "male",
            "diagnosis": "hypertension"
        },
        ...
    ]

Output:
- Cleaned list of patient dictionaries
- Each patient should have:
  * Properly capitalized name
  * Integer age (≥ 18)
  * Original gender and diagnosis preserved
- No duplicate records
- Prints cleaned records to console

Example:
    Input: {"name": "john smith", "age": "32", "gender": "male", "diagnosis": "flu"}
    Output: {"name": "John Smith", "age": 32, "gender": "male", "diagnosis": "flu"}

Usage:
    python patient_data_cleaner.py
"""

import json
import os
import pandas as pd

def load_patient_data(filepath):
    """
    Load patient data from a JSON file.
    
    Args:
        filepath (str): Path to the JSON file
        
    Returns:
        list: List of patient dictionaries
    """
    # BUG: No error handling for file not found
    # FIX: Use a try... except block to generate an exception - returns None when this happens
    try:
        with open(filepath, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Error: The file '{filepath}' was not found.")
    except json.JSONDecodeError:
        print(f"Error: The file '{filepath}' contains invalid JSON.")
    return None


def clean_patient_data(patients):
    """
    Clean patient data by:
    - Capitalizing names
    - Converting ages to integers
    - Filtering out patients under 18
    - Removing duplicates
    
    Args:
        patients (list): List of patient dictionaries
        
    Returns:
        list: Cleaned list of patient dictionaries
    """
    cleaned_patients = []

    # seems like the original code was using dataframe commands, don't actually need a loop
    df = pd.DataFrame(patients)
    
    # Capitalizing names
    # BUG: Typo in key 'nage' instead of 'name'
    # FIX: changed 'nage' to 'name'
    df['name'] = df['name'].str.title()

    # Converting ages to integers
    df['age'] = pd.to_numeric(df['age'], errors='coerce')

    # Replacing NaN's with 0
    # BUG: Wrong method name (fill_na vs fillna)
    # FIX: changed 'fill_na' to 'fillna'
    df['age'] = df['age'].fillna(0).astype(int)

    # Dropping dupliates by name and age (presumably same person can have different diagnosis)
    # BUG: Wrong method name (drop_duplcates vs drop_duplicates)
    # FIX: changed 'drop_duplcates' to 'drop_duplicates'
    df = df.drop_duplicates(subset=['name', 'age'])

    # Only keep people 18 or older - note this will get rid of people with age 0
    # BUG: Wrong comparison operator (= vs ==)
    # FIX: Use >= instead (comparison rather than assignment operator)
    # BUG: Logic error - keeps patients under 18 instead of filtering them out
    # FIX: Due to logic change below, should only keep people 18 or older, filtering out those < 18 years-old
    df = df[df['age'] >= 18]

    # back to list format
    cleaned_patients = df.to_dict(orient='records')
    
    # BUG: Missing return statement for empty list
    # FIX: Use 'if' statement to check if cleaned_patients is empty - returns None 
    # FIX: Warn user if empty
    if cleaned_patients == []:
        print("Warning: The list of cleaned patient data is empty.")
        return None
    if cleaned_patients == None:
        return None
    
    return cleaned_patients

def main():
    """Main function to run the script."""
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the path to the data file
    data_path = os.path.join(script_dir, 'data', 'raw', 'patients.json')
    
    # BUG: No error handling for load_patient_data failure
    # FIX: Use 'if not' statement to check if patients is empty or None - returns None
    patients = load_patient_data(data_path)
    if not patients:
        print("Error: No patient data found.")
        return None
    
    # Clean the patient data
    cleaned_patients = clean_patient_data(patients)
    
    # BUG: No check if cleaned_patients is None
    # FIX: This checks if cleaned_patients is None or empty -- reasonable to treat both the same
    if cleaned_patients == None:
        print("Error: Cleaned patient data is None/Null.")
        return None

    # Print the cleaned patient data
    print("Cleaned Patient Data:")
    for patient in cleaned_patients:
        # BUG: Using 'name' key but we changed it to 'nage'
        # FIX: Keeping 'name' is okay, we changed it above
        print(
            f"Name: {patient['name']}, Age: {patient['age']}, Diagnosis: {patient['diagnosis']}"
        )
    
    # Return the cleaned data (useful for testing)
    return cleaned_patients

if __name__ == "__main__":
    main()
