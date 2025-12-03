#!/usr/bin/env python3
"""
RFC Document Cleaner

Cleans raw RFC text files by removing headers, footers, TOC, and references.
Prepares documents for vectorstore ingestion.
"""

import os
import re
from pathlib import Path
from typing import Optional

class RFCCleaner:
    """Clean RFC documents for better text processing"""
    
    def __init__(self, source_dir: str, output_dir: str):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def clean_rfc_text(self, text: str, skip_lines: int = 250) -> str:
        """Clean RFC text content"""
        lines = text.splitlines()

        # Remove everything before the "Abstract"
        abstract_start = next((i for i, line in enumerate(lines) if "abstract" in line.lower()), 0)
        lines = lines[abstract_start:]

        # Remove the Table of Contents (if present)
        toc_start = next((i for i, line in enumerate(lines) if "table of contents" in line.lower()), None)
        toc_end = next((i for i, line in enumerate(lines) if re.match(r"^\s*1\.\s+introduction", line.lower())), None)

        if toc_start is not None and toc_end is not None and toc_start < toc_end:
            lines = lines[:toc_start] + lines[toc_end:]

        # Prepare for tail cleanup
        head = lines[:skip_lines]
        tail = lines[skip_lines:]
        tail_text = "\n".join(tail).lower()

        match = re.search(
            r"(normative references|informative references|full copyright statement|editors' addresses|intellectual property)",
            tail_text,
            re.IGNORECASE
        )

        if match:
            cutoff_line_index = tail_text[:match.start()].count("\n") + skip_lines
            lines = lines[:cutoff_line_index]

        # Remove excess blank lines & trailing spaces
        cleaned = "\n".join(line.strip() for line in lines if line.strip())
        
        return cleaned

    def clean_file(self, filename: str) -> Optional[str]:
        """Clean a single RFC file"""
        in_path = self.source_dir / filename
        out_filename = filename.replace(".txt", "_clean.txt")
        out_path = self.output_dir / out_filename

        try:
            with open(in_path, "r", encoding="utf-8") as infile:
                raw_text = infile.read()

            cleaned_text = self.clean_rfc_text(raw_text)

            with open(out_path, "w", encoding="utf-8") as outfile:
                outfile.write(cleaned_text)

            return out_filename
        except Exception as e:
            return None

    def clean_all_files(self) -> int:
        """Clean all RFC files in source directory"""
        cleaned_count = 0
        
        for file in self.source_dir.glob("*.txt"):
            if self.clean_file(file.name):
                cleaned_count += 1
        
        return cleaned_count

def clean_rfc_documents(source_dir: str, output_dir: str) -> int:
    """
    Clean RFC documents for vectorstore processing
    
    Args:
        source_dir: Directory containing raw RFC .txt files
        output_dir: Directory to save cleaned files
        
    Returns:
        Number of files successfully cleaned
    """
    cleaner = RFCCleaner(source_dir, output_dir)
    return cleaner.clean_all_files()

def main():
    """Clean RFC documents"""
    # Default paths relative to package
    base_dir = Path(__file__).parent.parent
    source_dir = base_dir / "data" / "rfc_documents" / "raw"
    output_dir = base_dir / "data" / "rfc_documents" / "clean"
    
    cleaned_count = clean_rfc_documents(str(source_dir), str(output_dir))
    return cleaned_count

if __name__ == "__main__":
    main() 