"""
Python 3.12 setup script for NLP Engine
Handles compatibility issues with transformers and torch
"""
import subprocess
import sys
import platform

def run_command(command, ignore_errors=False):
    """Run command với timeout và error handling"""
    print(f"Running: {command}")
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True,
            timeout=600  # 10 minute timeout cho torch
        )
        
        if result.stdout:
            print("STDOUT:", result.stdout.strip())
        if result.stderr and not ignore_errors:
            print("STDERR:", result.stderr.strip())
        
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print("Command timed out")
        return False
    except Exception as e:
        print(f"Command failed: {e}")
        return False

def install_torch_first():
    """Install PyTorch first with CPU-only version for compatibility"""
    print("Installing PyTorch (CPU version for better compatibility)...")
    
    # Try CPU-only PyTorch first
    torch_cmd = f"{sys.executable} -m pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu"
    
    if run_command(torch_cmd):
        print("✅ PyTorch installed successfully")
        return True
    else:
        print("Trying standard PyTorch installation...")
        return run_command(f"{sys.executable} -m pip install torch>=2.1.0")

def main():
    """Main setup for NLP Engine"""
    print("=" * 70)
    print("NLP Engine Setup for Python 3.12")
    print("=" * 70)
    
    python_version = sys.version_info
    print(f"Python: {python_version.major}.{python_version.minor}.{python_version.micro}")
    print(f"Platform: {platform.system()}")
    
    # Clear cache
    print("Clearing pip cache...")
    run_command(f"{sys.executable} -m pip cache purge", ignore_errors=True)
    
    # Upgrade build tools
    print("Upgrading build tools...")
    if not run_command(f"{sys.executable} -m pip install --upgrade pip setuptools wheel"):
        print("Warning: Build tools upgrade failed")
    
    # Install in groups
    package_groups = [
        ("Core", [
            "python-dotenv>=1.0.0",
            "requests>=2.31.0", 
            "tenacity>=8.2.3"
        ]),
        
        ("Data Processing", [
            "pandas>=2.0.0",
            "numpy>=1.24.0",
            "scikit-learn>=1.3.2"
        ]),
        
        ("Kafka", [
            "confluent-kafka>=2.3.0"
        ]),
        
        ("Document Processing", [
            "PyPDF2>=3.0.1",
            "python-docx>=1.1.0",
            "pypdf>=3.17.4", 
            "pdfplumber>=0.10.3"
        ]),
        
        ("Utilities", [
            "watchdog>=3.0.0",
            "tqdm>=4.66.0"
        ]),
        
        ("LangChain", [
            "langchain>=0.1.0",
            "langchain-community>=0.0.38"
        ]),
        
        ("NLP Models", [
            "transformers>=4.36.0",
            "tokenizers>=0.15.0",
            "sentence-transformers>=2.2.2"
        ]),
        
        ("Vector DB", [
            "chromadb>=0.4.18",
            "langchain-chroma>=0.1.2"
        ])
    ]
    
    # Install PyTorch first
    if not install_torch_first():
        print("❌ PyTorch installation failed")
        return False
    
    # Install other packages
    failed_packages = []
    
    for group_name, packages in package_groups:
        print(f"\nInstalling {group_name} packages...")
        for package in packages:
            if run_command(f"{sys.executable} -m pip install '{package}'"):
                print(f"✅ {package}")
            else:
                print(f"❌ {package}")
                failed_packages.append(package)
    
    # Summary
    print("\n" + "=" * 70)
    if failed_packages:
        print(f"❌ {len(failed_packages)} packages failed:")
        for pkg in failed_packages:
            print(f"   - {pkg}")
        print("\nTry installing manually:")
        for pkg in failed_packages:
            print(f"   pip install '{pkg}'")
    else:
        print("✅ All packages installed successfully!")
    
    # Test imports
    print("\nTesting key imports...")
    test_modules = [
        ("dotenv", "python-dotenv"),
        ("pandas", "pandas"),
        ("numpy", "numpy"), 
        ("torch", "pytorch"),
        ("transformers", "transformers"),
        ("langchain", "langchain"),
        ("chromadb", "chromadb"),
        ("confluent_kafka", "confluent-kafka")
    ]
    
    for module, package in test_modules:
        try:
            __import__(module)
            print(f"✅ {package} import OK")
        except ImportError:
            print(f"❌ {package} import failed")
    
    print("\n" + "=" * 70)
    print("NEXT STEPS:")
    print("1. Copy .env.example to .env")
    print("2. Configure ChromaDB connection")
    print("3. Test: python test_nlp_system.py")
    print("=" * 70)
    
    return len(failed_packages) == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)