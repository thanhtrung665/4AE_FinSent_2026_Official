"""
Setup script cho NLP Engine modules
"""
import subprocess
import sys
import os
from pathlib import Path

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
    """Main setup function cho NLP Engine"""
    print("=" * 60)
    print("Setting up NLP Engine for FinSent-Agent")
    print("=" * 60)
    
    # Check Python version
    python_version = sys.version_info
    print(f"Python version: {python_version.major}.{python_version.minor}.{python_version.micro}")
    
    if python_version.major != 3 or python_version.minor < 8:
        print("Error: Python 3.8+ required")
        return False
    
    # Upgrade pip first
    print("\n1. Upgrading pip...")
    run_command(f"{sys.executable} -m pip install --upgrade pip setuptools wheel")
    
    # Install PyTorch first (important for transformers)
    print("\n2. Installing PyTorch...")
    if not run_command(f"{sys.executable} -m pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu"):
        print("Warning: Failed to install PyTorch with CPU index, trying default...")
        run_command(f"{sys.executable} -m pip install torch")
    
    # Core packages that are usually stable
    core_packages = [
        "python-dotenv>=1.0.0",
        "confluent-kafka>=2.3.0", 
        "pandas>=1.5.0",
        "numpy>=1.21.0",
        "tqdm>=4.65.0",
        "requests>=2.31.0",
        "tenacity>=8.2.0",
        "watchdog>=3.0.0"
    ]
    
    print(f"\n3. Installing core packages...")
    for package in core_packages:
        print(f"Installing {package}...")
        run_command(f"{sys.executable} -m pip install '{package}'")
    
    # Document processing packages
    doc_packages = [
        "PyPDF2>=3.0.0",
        "python-docx>=1.1.0",
        "pypdf>=3.17.0", 
        "pdfplumber>=0.9.0"
    ]
    
    print(f"\n4. Installing document processing packages...")
    for package in doc_packages:
        print(f"Installing {package}...")
        run_command(f"{sys.executable} -m pip install '{package}'")
    
    # NLP packages (can be more problematic)
    print(f"\n5. Installing transformers and tokenizers...")
    nlp_packages = [
        "transformers>=4.35.0",
        "tokenizers>=0.15.0",
        "sentence-transformers>=2.2.0",
        "scikit-learn>=1.3.0"
    ]
    
    for package in nlp_packages:
        print(f"Installing {package}...")
        if not run_command(f"{sys.executable} -m pip install '{package}'"):
            print(f"Warning: Failed to install {package}")
    
    # LangChain packages (install last as they have many dependencies)
    print(f"\n6. Installing LangChain packages...")
    langchain_packages = [
        "langchain>=0.1.0",
        "langchain-community>=0.0.10"
    ]
    
    for package in langchain_packages:
        print(f"Installing {package}...")
        run_command(f"{sys.executable} -m pip install '{package}'")
    
    # ChromaDB (can be tricky)
    print(f"\n7. Installing ChromaDB...")
    if not run_command(f"{sys.executable} -m pip install chromadb>=0.4.0"):
        print("Warning: ChromaDB installation failed, trying alternative...")
        run_command(f"{sys.executable} -m pip install chromadb --no-deps")
        run_command(f"{sys.executable} -m pip install chroma-hnswlib pydantic typing-extensions")
    
    # LangChain ChromaDB integration
    print(f"\n8. Installing LangChain ChromaDB integration...")
    run_command(f"{sys.executable} -m pip install langchain-chroma")
    
    # Create directories
    print(f"\n9. Creating directories...")
    Path("nhnn_docs").mkdir(exist_ok=True)
    Path("nhnn_docs/pdf").mkdir(exist_ok=True)
    Path("nhnn_docs/docx").mkdir(exist_ok=True)
    Path("nhnn_docs/txt").mkdir(exist_ok=True)
    
    print("\n" + "=" * 60)
    print("NLP Engine Setup Complete!")
    print("=" * 60)
    print("Next steps:")
    print("1. Copy .env.example to .env")
    print("2. Update AWS_CHROMA_HOST and AWS_KAFKA_BROKER in .env")
    print("3. Add documents to nhnn_docs/ directory")
    print("4. Run: python nhnn_ingester.py --mode scan")
    print("5. Run: python sentiment_worker.py --mode health")
    print("=" * 60)

if __name__ == "__main__":
    main()