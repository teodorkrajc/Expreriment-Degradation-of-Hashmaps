"""
Split experiment_2_results.csv into separate files per variant for LaTeX plotting.
"""
import csv
from pathlib import Path

def split_results():
    results_dir = Path(__file__).parent.parent / "results"
    input_file = results_dir / "experiment_2_results.csv"
    
    # Read all rows
    with open(input_file, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    # Group by variant
    variants = {}
    for row in rows:
        variant = row['variant']
        if variant not in variants:
            variants[variant] = []
        variants[variant].append(row)
    
    # Write separate files
    fieldnames = rows[0].keys()
    for variant, variant_rows in variants.items():
        output_file = results_dir / f"experiment_2_results_{variant.lower()}.csv"
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(variant_rows)
        print(f"Created {output_file.name} with {len(variant_rows)} rows")

if __name__ == "__main__":
    split_results()
