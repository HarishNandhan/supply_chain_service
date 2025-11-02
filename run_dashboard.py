#!/usr/bin/env python3
"""
Simple script to run the Streamlit dashboard locally
"""

import subprocess
import sys
import os

def main():
    print("🚀 Starting Supply Chain Analytics Dashboard...")
    
    # Check if streamlit is installed
    try:
        import streamlit
        print("✅ Streamlit found")
    except ImportError:
        print("❌ Streamlit not found. Installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "streamlit"])
    
    # Check if required packages are installed
    required_packages = ["plotly", "google-cloud-bigquery", "euriai"]
    for package in required_packages:
        try:
            __import__(package.replace("-", "_"))
            print(f"✅ {package} found")
        except ImportError:
            print(f"❌ {package} not found. Please install: pip install {package}")
    
    # Set environment variables if needed
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        print("⚠️  GOOGLE_APPLICATION_CREDENTIALS not set. Make sure you have BigQuery access configured.")
    
    # Run streamlit
    print("🌐 Starting dashboard at http://localhost:8501")
    subprocess.run([
        sys.executable, "-m", "streamlit", "run", "app.py",
        "--server.port=8501",
        "--server.address=0.0.0.0"
    ])

if __name__ == "__main__":
    main()