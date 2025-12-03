#!/usr/bin/env python3
"""
Data Setup Script

Complete data pipeline: cleans RFC documents and builds vectorstore.
Use this script to set up the data infrastructure for ChatBGP.
"""

import sys
from pathlib import Path
from typing import Tuple

# Add scripts to path
sys.path.append(str(Path(__file__).parent))

from clean_rfc_documents import clean_rfc_documents
from build_vectorstore import build_vectorstore

def setup_chatbgp_data(base_data_dir: str = None) -> Tuple[bool, bool]:
    """
    Complete data setup pipeline
    
    Args:
        base_data_dir: Base directory for data (optional)
        
    Returns:
        Tuple of (cleaning_success, vectorstore_success)
    """
    if base_data_dir is None:
        base_data_dir = Path(__file__).parent.parent / "data"
    else:
        base_data_dir = Path(base_data_dir)
    
    # Define paths
    raw_docs_path = base_data_dir / "rfc_documents" / "raw"
    clean_docs_path = base_data_dir / "rfc_documents" / "clean"
    vectorstore_path = base_data_dir / "vectorstore"
    
    # Step 1: Clean RFC documents
    try:
        cleaned_count = clean_rfc_documents(str(raw_docs_path), str(clean_docs_path))
        cleaning_success = cleaned_count > 0
    except Exception:
        cleaning_success = False
    
    # Step 2: Build vectorstore (only if cleaning succeeded)
    if cleaning_success:
        try:
            vectorstore_success = build_vectorstore(str(clean_docs_path), str(vectorstore_path))
        except Exception:
            vectorstore_success = False
    else:
        vectorstore_success = False
    
    return cleaning_success, vectorstore_success

def main():
    """Run complete data setup"""
    cleaning_success, vectorstore_success = setup_chatbgp_data()
    
    if cleaning_success and vectorstore_success:
        return True
    else:
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 