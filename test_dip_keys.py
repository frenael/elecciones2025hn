from db import get_comparison_data

# JRV 9356 has DIPUTADOS files
jrv = '9356' 
print(f"Checking keys for JRV {jrv} (DIPUTADOS)...")
data = get_comparison_data(jrv, 'DIPUTADOS')
candidates = data['all_candidates']

print(f"Total Candidates: {len(candidates)}")
print("First 10 candidates:", candidates[:10])

# Check for duplicates or collapsed keys
unique = set(candidates)
if len(unique) != len(candidates):
    print("WARNING: Duplicates found!")
else:
    print("OK: All keys distinct.")

# Check for " - DIP " presence
dips = [c for c in candidates if " - DIP " in c]
print(f"Count with '- DIP ': {len(dips)}")

if not dips:
    print("WARNING: No DIP keys found. Maybe parsing failed or data missing?")
    # Print raw votes keys if any
    print("TREP Keys:", list(data['trep']['votos'].keys()))
