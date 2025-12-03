#!/usr/bin/env python3
"""
Vectorstore Builder

Builds vector embeddings from cleaned RFC documents using HuggingFace embeddings.
Creates a Chroma vectorstore for document retrieval.
"""

import os
import sys
from pathlib import Path
from typing import List, Optional

from langchain_chroma import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

# Add chatbgp package to path
sys.path.append(str(Path(__file__).parent.parent))

class VectorstoreBuilder:
    """Build vectorstore from RFC documents"""
    
    def __init__(self, 
                 embedding_model: str = "Alibaba-NLP/gte-multilingual-base",
                 chunk_size: int = 800,
                 chunk_overlap: int = 100):
        self.embedding_model_name = embedding_model
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.embedding_model = None
        
    def _initialize_embedding_model(self):
        """Initialize the embedding model"""
        if self.embedding_model is None:
            self.embedding_model = HuggingFaceEmbeddings(
                model_name=self.embedding_model_name,
                model_kwargs={"trust_remote_code": True}
            )
    
    def load_rfc_documents(self, folder_path: str) -> List[Document]:
        """Load all cleaned RFC files"""
        docs = []
        folder = Path(folder_path)
        
        for file in folder.glob("*_clean.txt"):
            try:
                with open(file, "r", encoding="utf-8") as f:
                    text = f.read()
                    metadata = {"source": file.name}
                    docs.append(Document(page_content=text, metadata=metadata))
            except Exception:
                continue  # Skip problematic files
                
        return docs
    
    def split_documents(self, documents: List[Document]) -> List[Document]:
        """Split documents into chunks"""
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap
        )
        return text_splitter.split_documents(documents)
    
    def build_vectorstore(self, 
                         documents_path: str, 
                         output_path: str) -> Optional[Chroma]:
        """Build vectorstore from documents"""
        try:
            # Load documents
            raw_documents = self.load_rfc_documents(documents_path)
            if not raw_documents:
                return None
            
            # Split documents
            split_documents = self.split_documents(raw_documents)
            if not split_documents:
                return None
            
            # Initialize embedding model
            self._initialize_embedding_model()
            
            # Create vectorstore
            vectorstore = Chroma.from_documents(
                documents=split_documents,
                embedding=self.embedding_model,
                persist_directory=output_path
            )
            
            return vectorstore
            
        except Exception:
            return None

def build_vectorstore(documents_path: str, 
                     output_path: str,
                     embedding_model: str = "Alibaba-NLP/gte-multilingual-base",
                     chunk_size: int = 800,
                     chunk_overlap: int = 100) -> bool:
    """
    Build vectorstore from RFC documents
    
    Args:
        documents_path: Path to cleaned RFC documents
        output_path: Path to save vectorstore
        embedding_model: HuggingFace embedding model name
        chunk_size: Text chunk size for splitting
        chunk_overlap: Overlap between chunks
        
    Returns:
        True if successful, False otherwise
    """
    builder = VectorstoreBuilder(embedding_model, chunk_size, chunk_overlap)
    vectorstore = builder.build_vectorstore(documents_path, output_path)
    return vectorstore is not None

def main():
    """Build vectorstore with default paths"""
    # Default paths relative to package
    base_dir = Path(__file__).parent.parent
    documents_path = base_dir / "data" / "rfc_documents" / "clean"
    output_path = base_dir / "data" / "vectorstore"
    
    success = build_vectorstore(str(documents_path), str(output_path))
    return success

if __name__ == "__main__":
    main() 