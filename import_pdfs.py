
import os
import shutil
import re
from pdf2image import convert_from_path

# Poppler Configuration (Added dynamically)
POPPLER_PATH = r"C:\Users\abner\Downloads\poppler-25.12.0\Library\bin"
if os.path.exists(POPPLER_PATH):
    os.environ["PATH"] += os.pathsep + POPPLER_PATH
else:
    print(f"Checking alternative path...")
    # Check if bin is directly under root
    ALT_PATH = r"C:\Users\abner\Downloads\poppler-25.12.0\bin"
    if os.path.exists(ALT_PATH):
        os.environ["PATH"] += os.pathsep + ALT_PATH
        POPPLER_PATH = ALT_PATH

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE_ROOT = os.path.join(BASE_DIR, 'data', 'actas_cna_frenael_v2')
DEST_ROOT = os.path.join(BASE_DIR, 'data', 'ACTAS', 'OFICIAL')

# Map folder names to levels if they differ
LEVELS = ['PRESIDENTE', 'DIPUTADOS', 'ALCALDE']

print(f"Starting Import...")
print(f"Source: {SOURCE_ROOT}")
print(f"Destination: {DEST_ROOT}")

def get_jrv_from_filename(filename):
    # Extracts number from "12345-PRESIDENTE..." or "JRV_12345..."
    match = re.search(r'(\d+)', filename)
    return match.group(1) if match else None

total_processed = 0
total_copied = 0
total_converted = 0
total_skipped = 0
errors = 0

for level in LEVELS:
    level_dir = os.path.join(SOURCE_ROOT, level)
    if not os.path.exists(level_dir):
        print(f"Skipping level {level} (Folder not found)")
        continue

    # Subfolders to check: pdf, no_computadas
    subfolders = ['pdf', 'no_computadas']
    
    for sub in subfolders:
        src_dir = os.path.join(level_dir, sub)
        if not os.path.exists(src_dir): continue
        
        print(f"Scanning {level}/{sub}...")
        
        for fname in os.listdir(src_dir):
            lower_name = fname.lower()
            if not (lower_name.endswith('.pdf') or lower_name.endswith('.jpg') or lower_name.endswith('.jpeg')):
                continue
                
            jrv = get_jrv_from_filename(fname)
            if not jrv:
                print(f"  [SKIP] No JRV found in {fname}")
                continue

            # Define Target Name
            # Standard: JRV-LEVEL.jpg (e.g. 35-PRESIDENTE.jpg)
            target_name = f"{jrv}-{level}.jpg"
            target_path = os.path.join(DEST_ROOT, target_name)
            
            # Check if exists
            if os.path.exists(target_path):
                # print(f"  [EXISTS] {target_name}")
                total_skipped += 1
                continue
                
            src_path = os.path.join(src_dir, fname)
            
            try:
                if lower_name.endswith('.pdf'):
                    # Convert PDF
                    print(f"  [CONVERT] {fname} -> {target_name}")
                    try:
                        # Use poppler_path explicitly if needed, but PATH update should work
                        images = convert_from_path(src_path, dpi=200, first_page=1, last_page=1, poppler_path=POPPLER_PATH)
                        if images:
                            images[0].save(target_path, 'JPEG')
                            total_converted += 1
                        else:
                            print(f"    Error: No images extracted from {fname}")
                            errors += 1
                    except Exception as e:
                        print(f"    Error converting {fname}: {e}")
                        errors += 1
                        
                else:
                    # Copy JPG
                    print(f"  [COPY] {fname} -> {target_name}")
                    shutil.copy2(src_path, target_path)
                    total_copied += 1
                    
            except Exception as e:
                print(f"  [ERROR] Processing {fname}: {e}")
                errors += 1

print("-" * 30)
print(f"Done.")
print(f"Skipped (Existing): {total_skipped}")
print(f"Converted (PDF->JPG): {total_converted}")
print(f"Copied (JPG): {total_copied}")
print(f"Errors: {errors}")
