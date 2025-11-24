from pathlib import Path
from typing import List
from langchain_chroma import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.documents import Document

class RetrieverConfig:
    # Paths
    BASE_DIR = Path(__file__).parent.parent
    VECTOR_DB_DIR = BASE_DIR / "Data" / "rfc_vectorstore"
    
    # Embedding settings
    EMBEDDING_MODEL = "Alibaba-NLP/gte-multilingual-base"
    TOP_K_DOCS = 3

class BGPRetriever:
    """Handles document retrieval from the vectorstore."""
    
    def __init__(self, config: RetrieverConfig = RetrieverConfig()):
        self.config = config
        self._init_retriever()

    def _init_retriever(self):
        """Initialize the embedding model and vectorstore."""
        if not self.config.VECTOR_DB_DIR.exists():
            raise ValueError(f"Vector store not found at: {self.config.VECTOR_DB_DIR}")

        # Initialize embedding model
        self.embedding_model = HuggingFaceEmbeddings(
            model_name=self.config.EMBEDDING_MODEL,
            model_kwargs={"trust_remote_code": True}
        )
        
        # Initialize vector store
        vectorstore = Chroma(
            persist_directory=str(self.config.VECTOR_DB_DIR),
            embedding_function=self.embedding_model
        )
        self.retriever = vectorstore.as_retriever(
            search_kwargs={"k": self.config.TOP_K_DOCS}
        )

    def get_relevant_documents(self, query: str) -> List[Document]:
        """Retrieve relevant documents for a query."""
        return self.retriever.invoke(query)

    def get_retriever(self):
        """Get the underlying retriever object."""
        return self.retriever
