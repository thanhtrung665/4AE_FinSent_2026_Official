# Python 3.12 Compatibility Fix Guide

## The Problem
You're encountering `AttributeError: module 'pkgutil' has no attribute 'ImpImporter'` because:
- Python 3.12 removed `pkgutil.ImpImporter` (deprecated since 3.3)
- Older versions of setuptools/pkg_resources still reference it
- Some packages haven't updated their build systems

## Quick Fix (Run These Commands)

### Step 1: Run the Quick Fix Script
```bash
cd producers
python fix_py312.py
```

This will:
- Clear pip cache
- Force upgrade pip to latest version
- Install Python 3.12 compatible setuptools
- Install modern build tools

### Step 2: Install Producers Dependencies
```bash
# Option A: Use the Python 3.12 setup script (recommended)
python setup_py312.py

# Option B: Use manual installation
pip install -r requirements-py312.txt

# Option C: Install core packages only
pip install confluent-kafka requests python-dotenv beautifulsoup4 feedparser pandas numpy
```

### Step 3: Install NLP Engine Dependencies  
```bash
cd ../nlp_engine
python setup_py312.py
```

## Alternative Manual Fix

If the scripts fail, try these manual steps:

### 1. Upgrade Build Tools
```bash
# Clear everything first
pip cache purge

# Force upgrade pip
python -m pip install --upgrade --force-reinstall pip

# Install 3.12-compatible setuptools
python -m pip install --upgrade --force-reinstall "setuptools>=68.0.0"

# Install wheel and build
python -m pip install wheel build
```

### 2. Install Packages One by One
```bash
# Core packages (should work)
pip install requests python-dotenv tenacity

# Data packages
pip install "pandas>=2.0.0" "numpy>=1.24.0"

# Kafka
pip install confluent-kafka

# Web scraping  
pip install beautifulsoup4 lxml feedparser

# Market data (might fail - skip if needed)
pip install vnstock
```

### 3. Handle Problematic Packages

For `vnstock` (if it fails):
```bash
# Try without build isolation
pip install --no-build-isolation vnstock

# Or install dependencies separately
pip install yfinance requests pandas numpy
pip install --no-deps vnstock
```

For `lxml` (if it fails):
```bash
# Try binary wheel only
pip install --only-binary=lxml lxml
```

## Verification

Test your installation:
```python
# Test script
import sys
print(f"Python version: {sys.version}")

modules = ['requests', 'dotenv', 'confluent_kafka', 'pandas', 'numpy']
for module in modules:
    try:
        __import__(module)
        print(f"✅ {module} OK")
    except ImportError as e:
        print(f"❌ {module} failed: {e}")
```

## Files Created for Python 3.12 Support

1. **`producers/fix_py312.py`** - Quick fix script
2. **`producers/setup_py312.py`** - Full setup for producers
3. **`producers/requirements-py312.txt`** - 3.12 compatible requirements
4. **`nlp_engine/setup_py312.py`** - NLP engine setup
5. **Updated `producers/setup.py`** - Enhanced with 3.12 detection
6. **Updated `nlp_engine/requirements.txt`** - 3.12 compatible versions

## Common Issues and Solutions

### Issue: `pkgutil.ImpImporter` error
**Solution**: Run `fix_py312.py` first to upgrade setuptools

### Issue: `vnstock` installation fails
**Solution**: 
- Skip vnstock for now: comment it out in requirements
- Or use `pip install --no-deps vnstock` after installing its dependencies

### Issue: `torch` installation takes too long
**Solution**: Use CPU-only version:
```bash
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
```

### Issue: `lxml` compilation errors
**Solution**: Use pre-compiled binary:
```bash
pip install --only-binary=lxml lxml
```

## Testing Your Setup

### Test Producers
```bash
cd producers
python quick_start.py
```

### Test NLP Engine  
```bash
cd nlp_engine
python test_nlp_system.py
```

## Next Steps After Fix

1. Copy `.env.example` to `.env` in both directories
2. Configure your AWS Kafka broker and ChromaDB settings
3. Run the health checks
4. Start with `python main.py health` in producers directory

## If All Else Fails

Create a fresh virtual environment:
```bash
# Create new venv
python -m venv venv312
venv312\Scripts\activate  # Windows
# source venv312/bin/activate  # Linux/Mac

# Run the fix
python fix_py312.py
python setup_py312.py
```