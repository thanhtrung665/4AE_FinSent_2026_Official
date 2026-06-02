"""
Python 3.12 compatibility setup script
Handles pkgutil.ImpImporter and setuptools compatibility issues
"""
import subprocess
import sys
import os
import platform

def run_command(command, ignore_errors=False):
    """Run command với error handling tốt hơn"""
    print(f"Running: {command}")
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.stdout:
            print("STDOUT:", result.stdout)
        if result.stderr and not ignore_errors:
            print("STDERR:", result.stderr)
        
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print("Command timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"Command failed with exception: {e}")
        return False

def clean_pip_cache():
    """Clear pip cache to avoid corrupted builds"""
    print("Clearing pip cache...")
    run_command(f"{sys.executable} -m pip cache purge", ignore_errors=True)

def fix_python312_compatibility():
    """Fix specific Python 3.12 compatibility issues"""
    print("Applying Python 3.12 compatibility fixes...")
    
    # 1. Force upgrade pip to latest
    print("1. Upgrading pip to 3.12-compatible version...")
    if not run_command(f"{sys.executable} -m pip install --upgrade 'pip>=23.3'"):
        print("ERROR: Cannot upgrade pip. This is required for Python 3.12")
        return False
    
    # 2. Install modern setuptools that supports Python 3.12
    print("2. Installing Python 3.12 compatible setuptools...")  
    if not run_command(f"{sys.executable} -m pip install --upgrade 'setuptools>=68.0.0' 'wheel>=0.41.0'"):
        print("ERROR: Cannot install 3.12-compatible setuptools")
        return False
    
    # 3. Install build tools
    print("3. Installing build dependencies...")
    run_command(f"{sys.executable} -m pip install --upgrade build setuptools-scm", ignore_errors=True)
    
    return True

def install_packages_safe():
    """Install packages one by one with safety checks"""
    # Core packages that should work with Python 3.12
    core_packages = [
        "requests>=2.31.0",
        "python-dotenv>=1.0.0", 
        "tenacity>=8.2.0",
        "python-dateutil>=2.8.0",
        "urllib3>=1.26.0,<3.0.0"
    ]
    
    # Data packages (might need special handling)
    data_packages = [
        "pandas>=1.5.0",
        "numpy>=1.21.0"
    ]
    
    # Kafka package
    kafka_packages = [
        "confluent-kafka>=2.3.0"
    ]
    
    # Web scraping packages  
    web_packages = [
        "beautifulsoup4>=4.12.0",
        "lxml>=4.9.0",
        "feedparser>=6.0.10"
    ]
    
    # Problem package
    vnstock_packages = [
        "vnstock>=0.2.8.0"
    ]
    
    all_package_groups = [
        ("Core packages", core_packages),
        ("Data processing packages", data_packages), 
        ("Kafka packages", kafka_packages),
        ("Web scraping packages", web_packages),
        ("VNStock package", vnstock_packages)
    ]
    
    failed_packages = []
    
    for group_name, packages in all_package_groups:
        print(f"\nInstalling {group_name}...")
        for package in packages:
            print(f"  Installing {package}...")
            
            # Special handling for potentially problematic packages
            if "vnstock" in package or "lxml" in package:
                # Try with no-cache and no-build-isolation for problematic packages
                success = run_command(f"{sys.executable} -m pip install --no-cache-dir --no-build-isolation '{package}'")
                if not success:
                    print(f"  Retrying {package} with --only-binary...")
                    success = run_command(f"{sys.executable} -m pip install --only-binary=all '{package}'", ignore_errors=True)
            else:
                success = run_command(f"{sys.executable} -m pip install '{package}'")
            
            if not success:
                print(f"  ❌ Failed to install {package}")
                failed_packages.append(package)
            else:
                print(f"  ✅ Successfully installed {package}")
    
    return failed_packages

def main():
    """Main setup function for Python 3.12"""
    print("=" * 70)
    print("Python 3.12 Compatible Setup for Data Pipeline Producers")
    print("=" * 70)
    
    # System info
    python_version = sys.version_info
    print(f"Python version: {python_version.major}.{python_version.minor}.{python_version.micro}")
    print(f"Platform: {platform.system()} {platform.release()}")
    print(f"Architecture: {platform.architecture()[0]}")
    
    if python_version.major != 3:
        print("ERROR: Python 3.x required")
        return False
    
    if python_version.minor < 8:
        print("ERROR: Python 3.8+ required")
        return False
    
    # Clear any cached builds that might be corrupted
    clean_pip_cache()
    
    # Apply Python 3.12 specific fixes
    if python_version.minor >= 12:
        if not fix_python312_compatibility():
            print("ERROR: Failed to apply Python 3.12 compatibility fixes")
            return False
    else:
        print("Standard setup for Python < 3.12...")
        if not run_command(f"{sys.executable} -m pip install --upgrade pip setuptools wheel"):
            print("Warning: Failed to upgrade build tools")
    
    # Install packages
    print(f"\nInstalling packages for producers...")
    failed_packages = install_packages_safe()
    
    # Results
    print("\n" + "=" * 70)
    print("INSTALLATION RESULTS")
    print("=" * 70)
    
    if failed_packages:
        print(f"❌ {len(failed_packages)} packages failed to install:")
        for pkg in failed_packages:
            print(f"   - {pkg}")
        
        print(f"\nMANUAL INSTALLATION COMMANDS:")
        for pkg in failed_packages:
            print(f"   pip install --no-cache-dir '{pkg}'")
        
        # Special instructions for vnstock
        if any("vnstock" in pkg for pkg in failed_packages):
            print(f"\nFor vnstock specifically, try:")
            print(f"   pip install --no-deps vnstock")
            print(f"   pip install yfinance requests pandas numpy")
    else:
        print("✅ All packages installed successfully!")
    
    # Verification
    print(f"\nVerifying installations...")
    test_imports = [
        "requests",
        "dotenv", 
        "tenacity",
        "confluent_kafka",
        "bs4",
        "feedparser"
    ]
    
    for module in test_imports:
        try:
            __import__(module)
            print(f"✅ {module} import successful")
        except ImportError as e:
            print(f"❌ {module} import failed: {e}")
    
    print("\n" + "=" * 70)
    print("NEXT STEPS:")
    print("=" * 70)
    print("1. Copy .env.example to .env")
    print("2. Update AWS_KAFKA_BROKER in .env")  
    print("3. Test: python quick_start.py")
    print("4. Run: python main.py health")
    print("=" * 70)
    
    return len(failed_packages) == 0

if __name__ == "__main__":
    try:
        success = main()
        print(f"\nSetup {'completed successfully' if success else 'completed with errors'}")
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nSetup cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nSetup failed with unexpected error: {e}")
        sys.exit(1)