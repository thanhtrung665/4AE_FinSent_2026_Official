"""
Setup script để cài đặt dependencies một cách an toàn cho Python 3.12
"""
import subprocess
import sys
import os

def run_command(command):
    """Run command và in output"""
    print(f"Running: {command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    if result.stdout:
        print("STDOUT:", result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    return result.returncode == 0

def main():
    """Main setup function"""
    print("=" * 60)
    print("Setting up Python Data Pipeline Producers")
    print("=" * 60)
    
    # Check Python version
    python_version = sys.version_info
    print(f"Python version: {python_version.major}.{python_version.minor}.{python_version.micro}")
    
    if python_version.major != 3 or python_version.minor < 8:
        print("Error: Python 3.8+ required")
        return False
    
    # Special handling for Python 3.12
    if python_version.minor >= 12:
        print("Detected Python 3.12+, applying compatibility fixes...")
        
        # Force upgrade pip to latest version
        print("\n1. Force upgrading pip to latest version...")
        if not run_command(f"{sys.executable} -m pip install --upgrade pip>=23.3"):
            print("Error: Failed to upgrade pip to 3.12-compatible version")
            return False
        
        # Install Python 3.12 compatible setuptools
        print("\n2. Installing Python 3.12 compatible build tools...")
        if not run_command(f"{sys.executable} -m pip install --upgrade 'setuptools>=68.0.0' wheel"):
            print("Error: Failed to install 3.12-compatible setuptools")
            return False
            
        # Install pkg-resources fix if needed
        print("\n3. Installing pkg-resources compatibility...")
        run_command(f"{sys.executable} -m pip install --upgrade 'setuptools-scm>=8.0.0'")
        
    else:
        # Standard upgrade for older Python versions
        print("\n1. Upgrading pip...")
        if not run_command(f"{sys.executable} -m pip install --upgrade pip"):
            print("Warning: Failed to upgrade pip, continuing anyway...")
        
        # Install setuptools and wheel separately
        print("\n2. Installing build tools...")
        if not run_command(f"{sys.executable} -m pip install --upgrade setuptools wheel"):
            print("Warning: Failed to install build tools")
    
    # Install packages one by one for better error handling
    packages = [
        "requests>=2.31.0,<3.0.0",
        "python-dotenv>=1.0.0", 
        "tenacity>=8.2.0",
        "pandas>=1.5.0,<3.0.0",
        "numpy>=1.21.0,<2.0.0",
        "confluent-kafka>=2.3.0",
        "beautifulsoup4>=4.12.0",
        "lxml>=4.9.0",
        "feedparser>=6.0.10",
        "python-dateutil>=2.8.0",
        "urllib3>=1.26.0,<3.0.0"
    ]
    
    print(f"\n3. Installing {len(packages)} packages...")
    
    failed_packages = []
    for i, package in enumerate(packages, 1):
        print(f"\n[{i}/{len(packages)}] Installing {package}...")
        if not run_command(f"{sys.executable} -m pip install '{package}'"):
            print(f"Failed to install {package}")
            failed_packages.append(package)
        else:
            print(f"Successfully installed {package}")
    
    # Try to install vnstock separately (can be problematic)
    print("\n4. Installing vnstock (this might take a while)...")
    vnstock_success = run_command(f"{sys.executable} -m pip install vnstock>=0.2.8.0")
    if not vnstock_success:
        print("Warning: vnstock installation failed. You can try installing it manually later:")
        print("pip install vnstock")
        failed_packages.append("vnstock")
    
    # Summary
    print("\n" + "=" * 60)
    print("INSTALLATION SUMMARY")
    print("=" * 60)
    
    if failed_packages:
        print(f"❌ {len(failed_packages)} packages failed:")
        for pkg in failed_packages:
            print(f"   - {pkg}")
        print("\nTo install failed packages manually:")
        for pkg in failed_packages:
            print(f"   pip install '{pkg}'")
    else:
        print("✅ All packages installed successfully!")
    
    print("\n" + "=" * 60)
    print("NEXT STEPS:")
    print("=" * 60)
    print("1. Copy .env.example to .env")
    print("2. Update AWS_KAFKA_BROKER in .env file")
    print("3. Run: python main.py health")
    print("=" * 60)
    
    return len(failed_packages) == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)