#!/usr/bin/env python3
"""
Emergency Room Medication Calculator

This script calculates medication dosages for emergency room patients based on 
standard emergency protocols. It follows weight-based dosing guidelines for common 
emergency medications.

Dosing Formula:
    Base Dosage (mg) = Patient Weight (kg) × Medication Factor (mg/kg)
    Loading Dose (mg) = Base Dosage × 2 (for first dose only)

When to use Loading Doses:
    - Only for first doses of certain medications (e.g., antibiotics, anti-seizure meds)
    - Determined by 'is_first_dose' flag in the input
    - Some medications always use loading doses for first administration

Example:
    Patient: 70kg, Medication: epinephrine, Is First Dose: No
    Base Dosage = 70 kg × 0.01 mg/kg = 0.7 mg
    Final Dosage = 0.7 mg

    Patient: 70kg, Medication: amiodarone, Is First Dose: Yes
    Base Dosage = 70 kg × 5 mg/kg = 350 mg
    Loading Dose = 350 mg × 2 = 700 mg
    Final Dosage = 700 mg

Input Format:
    {
        "name": "John Smith",
        "weight": 70.0,
        "medication": "epinephrine",
        "condition": "anaphylaxis",
        "is_first_dose": false,
        "allergies": ["penicillin"]
    }

Output:
    {
        "name": "John Smith",
        "weight": 70.0,
        "medication": "epinephrine",
        "base_dosage": 0.7,
        "is_first_dose": false,
        "loading_dose_applied": false,
        "final_dosage": 0.7,
        "warnings": ["Monitor for arrhythmias"]
    }

Medication Factors (mg/kg):
    epinephrine:  0.01  (Anaphylaxis)
    amiodarone:   5.00  (Cardiac arrest)
    lorazepam:    0.05  (Seizures)
    fentanyl:     0.001 (Pain)
    ...
"""

import json
import os

# Dosage factors for different medications (mg per kg of body weight)
# These are standard dosing factors based on medical guidelines
DOSAGE_FACTORS = {
    "epinephrine": 0.01,  # Anaphylaxis
    "amiodarone": 5.00,   # Cardiac arrest
    "lorazepam": 0.05,    # Seizures
    "fentanyl": 0.001,    # Pain
    "lisinopril": 0.5,    # ACE inhibitor for blood pressure
    "metformin": 10.0,    # Diabetes medication
    "oseltamivir": 2.5,   # Antiviral for influenza
    "sumatriptan": 1.0,   # Migraine medication
    "albuterol": 0.1,     # Asthma medication
    "ibuprofen": 5.0,     # Pain/inflammation
    "sertraline": 1.5,    # Antidepressant
    "levothyroxine": 0.02 # Thyroid medication
}

# Medications that use loading doses for first administration
# BUG: Missing commas between list items
# FIX: Added commas
# BUG: 'fentynal' spelled wrong
# FIX: Changed 'fentynal' to 'fentanyl'
LOADING_DOSE_MEDICATIONS = [
    "amiodarone",
    "lorazepam",
    "fentanyl"
]

LOADING_FACTOR = 2

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

def calculate_dosage(patient):
    """
    Calculate medication dosage for a patient.
    
    Args:
        patient (dict): Patient dictionary with 'weight', 'medication', and 'is_first_dose' keys
        
    Returns:
        dict: Patient dictionary with added dosage information
    """
    # Create a copy of the patient data to avoid modifying the original
    patient_with_dosage = patient.copy()
    
    # Extract patient information
    # BUG: No check if 'weight' key exists
    # FIX: Use .get() function, which returns None if the key is missing 
    weight = patient.get('weight')
    if weight == None:
        print(f"Warning: No weight key detected for patient {patient.get('name')}")
    # BUG: No check if 'medication' key exists
    # FIX: Check if 'medication' key exists before assigning, else assign to None
    medication = patient.get('medication') # This bug is diabolical
    if medication == None:
        print(f"Warning: No medication key detected for patient {patient.get('name')}")
    
    # Get the medication factor
    # BUG: Adding 's' to medication name, which doesn't match DOSAGE_FACTORS keys
    # FIX: Remove adding 's'
    factor = DOSAGE_FACTORS.get(medication, 0)
    
    # Calculate base dosage
    # BUG: Using addition instead of multiplication
    # FIX: Use multiplication '*'
    base_dosage = weight * factor
    
    # Determine if loading dose should be applied
    # BUG: No check if 'is_first_dose' key exists
    # FIX: Change the .get() function to return None instead of False
    is_first_dose = patient.get('is_first_dose')
    if is_first_dose == None:
        print(f"Warning: No is_first_dose key detected for patient {patient.get('name')}")
    loading_dose_applied = False
    final_dosage = base_dosage
    
    # Apply loading dose if it's the first dose and the medication uses loading doses
    # BUG: Incorrect condition - should check if medication is in LOADING_DOSE_MEDICATIONS
    if is_first_dose and medication in LOADING_DOSE_MEDICATIONS:
        loading_dose_applied = True
        # BUG: Using addition instead of multiplication for loading dose
        # FIX: Use multiplication by LOADING_FACTOR - defined above as 2 per instructions
        final_dosage = base_dosage * LOADING_FACTOR
    
    # Add dosage information to the patient record
    patient_with_dosage['base_dosage'] = base_dosage
    patient_with_dosage['loading_dose_applied'] = loading_dose_applied
    patient_with_dosage['final_dosage'] = final_dosage
    
    # Add warnings based on medication
    warnings = []
    # BUG: Typos in medication names
    # FIX: Added 'e' to 'epinephrin', changed 'fentynal' to 'fentanyl'
    if medication == "epinephrine":
        warnings.append("Monitor for arrhythmias")
    elif medication == "amiodarone":
        warnings.append("Monitor for hypotension")
    elif medication == "fentanyl":
        warnings.append("Monitor for respiratory depression")
    
    patient_with_dosage['warnings'] = warnings
    
    return patient_with_dosage

def calculate_all_dosages(patients):
    """
    Calculate dosages for all patients and sum the total.
    
    Args:
        patients (list): List of patient dictionaries
        
    Returns:
        tuple: (list of patient dicts with dosages, total medication needed)
    """
    total_medication = 0
    patients_with_dosages = []
    
    # Process all patients
    for patient in patients:
        # Calculate dosage for this patient
        patient_with_dosage = calculate_dosage(patient)
        
        # Add to our list
        patients_with_dosages.append(patient_with_dosage)
        
        # Add to total medication
        # BUG: No check if 'final_dosage' key exists
        final_dosage = patient_with_dosage.get('final_dosage')
        if final_dosage == None:
            print(f"Warning: No final_dosage key detected for patient {patient.get('name')}")
        total_medication += final_dosage
    
    return patients_with_dosages, total_medication

def main():
    """Main function to run the script."""
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construct the path to the data file
    # BUG: Missing 'raw' in the file path
    # FIX: Adding 'raw' in the file path
    data_path = os.path.join(script_dir, 'data', 'raw', 'meds.json')
    
    # BUG: No error handling for load_patient_data failure
    # FIX: Use 'if not' statement to check if patients is empty or None - returns None
    patients = load_patient_data(data_path)
    if not patients:
        print("Error: No patient data found.")
        return None
    
    # Calculate dosages for all patients
    patients_with_dosages, total_medication = calculate_all_dosages(patients)
    
    # Print the dosage information
    print("Medication Dosages:")
    for patient in patients_with_dosages:
        # BUG: No check if required keys exist
        # FIX: use if all() statement 
        required_keys = ['name', 'medication', 'base_dosage', 'final_dosage', 'loading_dose_applied', 'warnings']
        if all(key in patient for key in required_keys):
            print(f"Name: {patient['name']}, Medication: {patient['medication']}, "
                f"Base Dosage: {patient['base_dosage']:.2f} mg, "
                f"Final Dosage: {patient['final_dosage']:.2f} mg")
            if patient['loading_dose_applied']:
                print(f"  * Loading dose applied")
            if patient['warnings']:
                print(f"  * Warnings: {', '.join(patient['warnings'])}")
    
    print(f"\nTotal medication needed: {total_medication:.2f} mg")
    
    # Return the results (useful for testing)
    return patients_with_dosages, total_medication

if __name__ == "__main__":
    main()
