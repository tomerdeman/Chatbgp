"""
RAG Framework for BGP Documentation
---------------------------------
A modular framework for retrieving and answering questions about BGP using RFC documents.
"""

# Version of the rag_framework package
__version__ = "0.1.0"

from rag_framework.router import BGPRouter, RouterConfig
from rag_framework.retriever import BGPRetriever, RetrieverConfig
from rag_framework.llm_chain import BGPChain, LLMChainConfig

__all__ = [
    'BGPRouter',
    'RouterConfig',
    'BGPRetriever',
    'RetrieverConfig',
    'BGPChain',
    'LLMChainConfig'
] 