# This program is for converting traces from the lassen dataset to use exclusive alloc

import pandas as pd
import argparse

def adjust_ncpus(csv_path, multiplier, output_path=None):
    """
    Updates NCPUS = int(NNodes * multiplier) for each job in a CSV file.

    Args:
        csv_path (str): Path to the input CSV file.
        multiplier (int or float): The factor to multiply NNodes by.
        output_path (str, optional): Path to save the modified CSV.
                                    Defaults to overwriting input file.
    """
    # Load CSV
    df = pd.read_csv(csv_path)

    # Check required columns exist
    required = {'NNodes', 'NCPUS'}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    # Apply transformation (as int)
    df['NCPUS'] = (df['NNodes'] * multiplier).astype(int)

    # Save result
    output_path = output_path or csv_path
    df.to_csv(output_path, index=False)
    print(f"✅ Updated file saved to: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Adjust NCPUS based on NNodes × multiplier.")
    parser.add_argument("csv_path", help="Path to input CSV file")
    parser.add_argument("multiplier", type=float, help="Value to multiply NNodes by")
    parser.add_argument("-o", "--output", help="Path to save updated CSV (default: overwrite input file)")
    args = parser.parse_args()

    adjust_ncpus(args.csv_path, args.multiplier, args.output)
