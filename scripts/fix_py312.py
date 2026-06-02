"""
Quick fix for Python 3.12 pkgutil.ImpImporter error
Run this first: python fix_py312.py
"""
import subprocess
import sys

def run_cmd(cmd):
    print(f">>> {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return False
    print(f"Success: {result.stdout}")
    return True

print("=== Python 3.12 Quick Fix ===")
print(f"Python version: {sys.version}")

# Step 1: Clear pip cache
print("\n1. Clearing pip cache...")
run_cmd(f"{sys.executable} -m pip cache purge")

# Step 2: Upgrade pip aggressively 
print("\n2. Force upgrading pip...")
if not run_cmd(f"{sys.executable} -m pip install --upgrade --force-reinstall pip"):
    print("Failed to upgrade pip!")
    sys.exit(1)

# Step 3: Install modern setuptools
print("\n3. Installing Python 3.12 compatible setuptools...")
if not run_cmd(f"{sys.executable} -m pip install --upgrade --force-reinstall 'setuptools>=68.0.0'"):
    print("Failed to install setuptools!")
    sys.exit(1)

# Step 4: Install wheel
print("\n4. Installing wheel...")
run_cmd(f"{sys.executable} -m pip install --upgrade wheel")

# Step 5: Install build tools
print("\n5. Installing build tools...")
run_cmd(f"{sys.executable} -m pip install build")

print("\n=== Fix completed! ===")
print("Now you can run:")
print("  python setup_py312.py")
print("Or install packages manually:")
print("  pip install confluent-kafka requests python-dotenv")