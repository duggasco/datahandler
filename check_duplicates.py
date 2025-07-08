import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# Read EMEA file
df = pd.read_excel('/data/DataDump__EMEA_20250703.xlsx')

# Clean fund codes
df['Fund Code'] = df['Fund Code'].apply(lambda x: x.strip() if isinstance(x, str) else x)

# Find all duplicate fund codes
duplicate_counts = df['Fund Code'].value_counts()
duplicates = duplicate_counts[duplicate_counts > 1]

print('All duplicate fund codes in EMEA:')
for code, count in duplicates.items():
    print(f'  "{code}": {count} occurrences')
    # Show some details for non-MULTIVALUE duplicates
    if str(code) != '#MULTIVALUE':
        print(f'    Rows with this code:')
        dup_rows = df[df['Fund Code'] == code][['Fund Code', 'Fund Name']].head(5)
        for idx, row in dup_rows.iterrows():
            print(f'      Row {idx}: {row["Fund Name"]}')