# ChatBGP - Intelligent BGP Analysis System

ChatBGP is a comprehensive system for analyzing BGP routing data using LLM-powered natural language queries and heuristic analysis.

## Features

- 🧠 **Natural Language Queries**: Ask questions about BGP in plain English
- 📊 **Heuristic Analysis**: Automated detection of routing anomalies and potential hijacks
- 📚 **RFC Knowledge Base**: Access to cleaned BGP RFC documents 
- 🔍 **Live BGP Data**: Query current routing state using radix trees
- 📈 **Historical Analysis**: Track routing changes over time
- 🛡️ **RPKI & IRR Validation**: Cross-reference routing data with authoritative sources
- 🌐 **Web Interface**: FastAPI-based web chat interface

## Quick Start

### Installation

```bash
# Install web interface dependencies
pip install -r web_interface/requirements.txt

# Install core dependencies
pip install fastapi uvicorn jinja2 python-multipart
pip install langchain langchain-chroma langchain-huggingface
pip install duckdb py-radix
pip install openai requests  # or your LLM provider
```

### Running the Web Interface

```bash
python web_interface/main.py
```

Then open `http://localhost:8000` in your browser.

## Architecture

The system uses **CleanBGPRouter** which orchestrates:
- Entity extraction (LLM or regex)
- RFC document retrieval (semantic search)
- Live BGP state queries (radix trees)
- Historical BGP data (DuckDB)
- RPKI/IRR validation
- Heuristic analysis (hijack detection, route flaps)

## Required Data Files

For full functionality, you'll need:
1. **Radix trees** (`radix_v4_obj.pkl.gz`, `radix_v6_obj.pkl.gz`) - for live BGP lookups
2. **DuckDB database** (`bgp_rib_snapshot.duckdb`) - for historical queries
3. **Vectorstore** (`RAG/Data/rfc_vectorstore/`) - for RFC document search

These large files are not included in the repository. See setup scripts to generate them.

## Project Structure

```
├── web_interface/          # FastAPI web application
│   ├── main.py            # Main application entry point
│   ├── templates/         # HTML templates
│   └── requirements.txt   # Web dependencies
├── RAG/
│   ├── rag_framework/     # Core framework
│   │   ├── clean_router.py  # Main router (CleanBGPRouter)
│   │   ├── retriever.py     # RFC document retrieval
│   │   ├── llm_chain.py     # LLM response generation
│   │   └── ...
│   └── Scripts/
│       └── live_data_tools/  # RPKI/IRR data fetchers
```

## Configuration

Set environment variables for:
- LLM API keys (OpenAI, etc.)
- Vectorstore paths
- Database paths

## License

[Your License Here]

