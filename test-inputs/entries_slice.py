#!/usr/bin/env python3
import csv
import sys

if len(sys.argv) < 5:
    print(f"Usage: {sys.argv[0]} input.csv output.csv start_row end_row")
    sys.exit(1)

input_file  = sys.argv[1]
output_file = sys.argv[2]
start_row   = int(sys.argv[3]) 
end_row     = int(sys.argv[4])  

with open(input_file, newline="") as infile, open(output_file, "w", newline="") as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    header = next(reader)
    writer.writerow(header)

    row_num = 1
    for row in reader:
        if row_num >= start_row and row_num <= end_row:
            writer.writerow(row)
        if row_num > end_row:
            break
        row_num += 1
