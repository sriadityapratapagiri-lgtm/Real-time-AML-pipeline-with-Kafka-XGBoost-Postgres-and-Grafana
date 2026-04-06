import pandas as pd
import zipfile

print("Opening the zip file...")

# Open the zip archive
with zipfile.ZipFile('archive.zip', 'r') as z:
    print("Targeting HI-Large_Trans.csv and reading the first 1 million rows...")
    
    # Open ONLY the specific file we need from inside the zip
    with z.open('HI-Large_Trans.csv') as f:
        df = pd.read_csv(f, nrows=1000000)

print("Data loaded into memory. Saving as local_dev_data.csv...")
df.to_csv('local_dev_data.csv', index=False)

print("Success!")
