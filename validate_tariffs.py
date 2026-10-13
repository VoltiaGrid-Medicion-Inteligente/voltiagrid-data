import csv
from datetime import datetime

def validate_criteria():
    ruta_csv = "data/reference/tariffs.csv"
    
    with open(ruta_csv, mode='r', encoding='utf-8') as f:
        reader = list(csv.DictReader(f))
        
    print("=== Starting Tariff Calendar Validation (US-04) ===\n")
    
    # Criterion 1: Less than 100 rows
    num_rows = len(reader)
    print(f"1. Total rows: {num_rows} (Expected: < 100) -> {'✅ OK' if num_rows < 100 else '❌ ERROR'}")
    
    # Criterion 2: At least 2 validity periods
    periods = set((row['valid_from'], row['valid_to']) for row in reader)
    print(f"2. Distinct validity periods: {len(periods)} (Expected: >= 2) -> {'✅ OK' if len(periods) >= 2 else '❌ ERROR'}")
    
    # Criterion 3: Peak, off-peak, and mid bands (punta, valle, intermedia)
    bands = set(row['band'] for row in reader)
    required_bands = {'punta', 'intermedia', 'valle'}
    print(f"3. Found bands: {bands} -> {'✅ OK' if required_bands.issubset(bands) else '❌ ERROR'}")
    
    # Criterion 4: No ambiguity (Exact 24h continuous coverage without gaps/overlaps)
    ambiguity = False
    for period in periods:
        period_rows = [r for r in reader if (r['valid_from'], r['valid_to']) == period]
        period_rows.sort(key=lambda x: x['start_time'])
        
        # Must start exactly at 00:00:00 and end at 23:59:59
        if period_rows[0]['start_time'] != '00:00:00' or period_rows[-1]['end_time'] != '23:59:59':
            ambiguity = True
            
        # Check that the gap between consecutive bands is exactly 1 second
        for i in range(len(period_rows) - 1):
            current_end = datetime.strptime(period_rows[i]['end_time'], '%H:%M:%S')
            next_start = datetime.strptime(period_rows[i+1]['start_time'], '%H:%M:%S')
            
            if (next_start - current_end).total_seconds() != 1.0:
                ambiguity = True
                
    print(f"4. No-ambiguity validation (exact 24h coverage): -> {'❌ ERROR' if ambiguity else '✅ OK'}")
    
    if num_rows < 100 and len(periods) >= 2 and required_bands.issubset(bands) and not ambiguity:
        print("\n Success! The file perfectly complies with all acceptance criteria.")
    else:
        print("\n Validation errors found. Please review the dataset.")

if __name__ == "__main__":
    validate_criteria()