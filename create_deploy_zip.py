
import os
import shutil
import zipfile

def create_deploy_package():
    # Define source and destination
    source_dir = os.getcwd()
    temp_dir = os.path.join(source_dir, "deploy_temp")
    zip_filename = "deploy_package.zip"

    # Clean up previous temp
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    # Files/Folders to include
    include_files = ["app.py", "db.py", "processor.py", "filtrar_actas.py", "requirements.txt", "auditoria.db"]
    include_dirs = ["templates", "static"] # static might not exist, check
    
    # 1. Copy Files
    for f in include_files:
        if os.path.exists(os.path.join(source_dir, f)):
            shutil.copy2(os.path.join(source_dir, f), os.path.join(temp_dir, f))
            print(f"Copied {f}")
            
    # 2. Copy Key Directories
    for d in include_dirs:
        src = os.path.join(source_dir, d)
        dst = os.path.join(temp_dir, d)
        if os.path.exists(src):
            shutil.copytree(src, dst)
            print(f"Copied directory {d}")

    # 3. Create 'data' folder structure but exclude heavy content (ACTAS)
    data_dir = os.path.join(temp_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    
    # Copy JSON folder? Usually small. ACTAS is huge.
    # Let's copy JSON folder if it exists.
    json_src = os.path.join(source_dir, "data", "JSON")
    if os.path.exists(json_src):
        shutil.copytree(json_src, os.path.join(data_dir, "JSON"))
        print("Copied JSON data")
        
    # Copy diputados_oficial.json
    dip_src = os.path.join(source_dir, "data", "diputados_oficial.json")
    if os.path.exists(dip_src):
        shutil.copy2(dip_src, os.path.join(data_dir, "diputados_oficial.json"))
        print("Copied diputados_oficial.json")

    # Create empty ACTAS folders just so paths exist
    os.makedirs(os.path.join(data_dir, "ACTAS", "FRENAEL"), exist_ok=True)
    os.makedirs(os.path.join(data_dir, "ACTAS", "OFICIAL"), exist_ok=True)

    # 4. Zip it up
    shutil.make_archive("deploy_package", 'zip', temp_dir)
    print(f"Created {zip_filename}")
    
    # Clean up temp
    shutil.rmtree(temp_dir)

if __name__ == "__main__":
    create_deploy_package()
