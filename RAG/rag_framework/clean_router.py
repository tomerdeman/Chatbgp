#!/usr/bin/env python3
"""
clean_router.py - A focused BGP router that orchestrates existing components
"""

import os
import sys
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
import duckdb
import gzip
import pickle

# Add parent directory for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rag_framework.retriever import BGPRetriever, RetrieverConfig
from rag_framework.llm_chain import BGPChain, LLMChainConfig
from rag_framework.llm_entity_extractor import LLMEntityExtractor
from rag_framework.entity_extractor import RegexEntityExtractor
from rag_framework.heuristic_analyzer import analyze_bgp_discrepancies
from Scripts.live_data_tools.rpki_fetcher import fetch_rpki_validation
from Scripts.live_data_tools.whois_fetcher import fetch_whois_data


class QueryType:
    """Query type constants"""
    STATIC_DOCS = "static"           # RFC/documentation only
    LIVE_BGP = "live"               # Current BGP state
    RPKI_VALIDATION = "rpki"        # RPKI validation
    HISTORICAL = "historical"       # Historical BGP data
    HYBRID = "hybrid"               # Multiple sources


class CleanBGPRouter:
    """
    A focused BGP router that orchestrates specialized components.
    Delegates entity extraction, analysis, and response generation to existing modules.
    """
    
    def __init__(self, 
                 retriever_config: Optional[RetrieverConfig] = None,
                 chain_config: Optional[LLMChainConfig] = None,
                 entity_extractor: str = "llm"):  # "llm" or "regex"
        
        self.retriever = BGPRetriever(retriever_config or RetrieverConfig())
        self.chain = BGPChain(chain_config or LLMChainConfig())
        
        # Initialize entity extractor
        if entity_extractor == "llm":
            try:
                self.entity_extractor = LLMEntityExtractor()
            except Exception as e:
                print(f"Warning: LLM extractor failed, using regex: {e}")
                self.entity_extractor = RegexEntityExtractor()
        else:
            self.entity_extractor = RegexEntityExtractor()
        
        # Load data sources
        self._load_radix_trees()
        self._connect_database()
    
    def _load_radix_trees(self):
        """Load radix trees for BGP routing data"""
        self.rtree_v4 = None
        self.rtree_v6 = None
        
        # Try to find radix tree files
        possible_paths = [
            "radix_v4_obj.pkl.gz",
            "RAG/rag_framework/radix_v4_obj.pkl.gz"
        ]
        
        for v4_path in possible_paths:
            v6_path = v4_path.replace("v4", "v6")
            if os.path.exists(v4_path) and os.path.exists(v6_path):
                try:
                    with gzip.open(v4_path, "rb") as f:
                        self.rtree_v4 = pickle.load(f)
                    with gzip.open(v6_path, "rb") as f:
                        self.rtree_v6 = pickle.load(f)
                    print(f"Loaded radix trees from {v4_path}")
                    break
                except Exception as e:
                    print(f"Failed to load radix trees from {v4_path}: {e}")
        
        if not self.rtree_v4:
            print("Warning: No radix trees loaded. BGP lookups will not work.")
    
    def _connect_database(self):
        """Connect to DuckDB for historical data"""
        self.db_con = None
        
        db_paths = [
            "bgp_rib_snapshot.duckdb",
            "RAG/rag_framework/bgp_rib_snapshot.duckdb"
        ]
        
        for db_path in db_paths:
            if os.path.exists(db_path):
                try:
                    self.db_con = duckdb.connect(db_path)
                    self.db_con.execute('INSTALL inet; LOAD inet;')
                    print(f"Connected to database: {db_path}")
                    break
                except Exception as e:
                    print(f"Failed to connect to {db_path}: {e}")
        
        if not self.db_con:
            print("Warning: No database connection. Historical queries will not work.")
    
    def determine_query_type(self, query: str, entities: Dict[str, Any]) -> List[str]:
        """Determine what types of data sources are needed"""
        query_lower = query.lower()
        types = []
        
        # Static docs for general BGP concepts
        static_keywords = [
            "what is", "explain", "definition", "how does", "describe",
            "route flapping", "flapping", "bgp", "border gateway protocol",
            "routing protocol", "autonomous system", "convergence", "rfc"
        ]
        
        if any(keyword in query_lower for keyword in static_keywords):
            types.append(QueryType.STATIC_DOCS)
        
        # Live BGP data for prefixes/IPs/ASNs
        if entities.get("prefixes") or entities.get("ip_addresses") or entities.get("asns"):
            types.append(QueryType.LIVE_BGP)
        
        # RPKI validation
        if "rpki" in query_lower or "roa" in query_lower or "valid" in query_lower:
            types.append(QueryType.RPKI_VALIDATION)
        
        # Historical data
        if "history" in query_lower or "historical" in query_lower or entities.get("time_references"):
            types.append(QueryType.HISTORICAL)
        
        # Default to static docs for concept questions
        if not types:
            types.append(QueryType.STATIC_DOCS)
        
        return types
    
    def get_static_docs(self, query: str, max_docs: int = 3) -> List[Dict[str, str]]:
        """Get relevant RFC documentation with source information"""
        try:
            docs = self.retriever.get_relevant_documents(query)
            result = []
            for doc in docs[:max_docs]:
                # Extract RFC number from filename (e.g., "rfc4271_clean.txt" -> "RFC 4271")
                source = doc.metadata.get("source", "Unknown")
                rfc_number = "Unknown RFC"
                if source.startswith("rfc") and source.endswith("_clean.txt"):
                    rfc_num = source.replace("rfc", "").replace("_clean.txt", "")
                    rfc_number = f"RFC {rfc_num}"
                
                result.append({
                    "content": doc.page_content,
                    "source": rfc_number,
                    "filename": source
                })
            return result
        except Exception as e:
            print(f"Error retrieving static docs: {e}")
            return []
    
    def get_live_bgp_state(self, entities: Dict[str, Any]) -> Dict[str, Any]:
        """Get current BGP state from radix trees"""
        if not self.rtree_v4 and not self.rtree_v6:
            return {"status": "no_data", "message": "No radix trees available"}
        
        result = {"status": "success", "routes": []}
        
        # Handle prefixes
        for prefix in entities.get("prefixes", []):
            tree = self.rtree_v6 if ":" in prefix else self.rtree_v4
            if tree:
                node = tree.search_exact(prefix)
                if node:
                    result["routes"].append({
                        "type": "exact_match",
                        "prefix": prefix,
                        "data": node.data
                    })
        
        # Handle IP addresses
        for ip in entities.get("ip_addresses", []):
            tree = self.rtree_v6 if ":" in ip else self.rtree_v4
            if tree:
                node = tree.search_best(ip)
                if node:
                    result["routes"].append({
                        "type": "longest_prefix_match", 
                        "ip": ip,
                        "matching_prefix": node.prefix,
                        "data": node.data
                    })
        
        # Handle AS numbers
        for asn in entities.get("asns", []):
            # Find prefixes originated by this AS
            prefixes = []
            for tree in [self.rtree_v4, self.rtree_v6]:
                if tree:
                    for node in tree:
                        if node.data.get("origin_as") == asn:
                            prefixes.append(node.prefix)
            
            if prefixes:
                result["routes"].append({
                    "type": "as_originated",
                    "asn": asn,
                    "prefixes": prefixes[:10]  # Limit to first 10
                })
        
        return result
    
    def get_historical_data(self, entities: Dict[str, Any], prefix: str = None) -> List[Dict[str, Any]]:
        """Get historical BGP updates from database"""
        if not self.db_con:
            return []
        
        try:
            # Use provided prefix or extract from entities
            target_prefix = prefix or (entities.get("prefixes", [None])[0])
            if not target_prefix:
                return []
            
            query = """
            SELECT timestamp, update_type, as_path, origin_as
            FROM rrc03_updates 
            WHERE prefix = ?
            ORDER BY timestamp DESC
            LIMIT 100
            """
            
            results = self.db_con.execute(query, [target_prefix]).fetchall()
            
            return [{
                "timestamp": row[0].isoformat(),
                "type": row[1],
                "as_path": row[2],
                "origin_as": row[3]
            } for row in results]
            
        except Exception as e:
            print(f"Error querying historical data: {e}")
            return []
    
    def get_validation_data(self, prefix: str, origin_as: str) -> Dict[str, Any]:
        """Get RPKI and IRR validation data"""
        result = {"rpki": None, "irr": None, "analysis": None}
        
        try:
            # Fetch RPKI validation
            result["rpki"] = fetch_rpki_validation(prefix, f"AS{origin_as}")
        except Exception as e:
            print(f"RPKI fetch error: {e}")
        
        try:
            # Fetch IRR data
            result["irr"] = fetch_whois_data(prefix)
        except Exception as e:
            print(f"IRR fetch error: {e}")
        
        return result
    
    def query(self, query: str) -> Dict[str, Any]:
        """Process a BGP query and return structured results"""
        start_time = time.time()
        
        try:
            # Step 1: Extract entities
            entities = self.entity_extractor.extract(query)
            print(f"Entities extracted: {entities}")
            
            # Step 2: Determine query types needed
            query_types = self.determine_query_type(query, entities)
            print(f"Query types: {query_types}")
            
            # Step 3: Gather data from appropriate sources
            context_parts = []
            bgp_data = {}
            
            # Static documentation
            if QueryType.STATIC_DOCS in query_types:
                static_docs = self.get_static_docs(query)
                if static_docs:
                    context_parts.append("=== RFC DOCUMENTATION ===")
                    for i, doc in enumerate(static_docs):
                        context_parts.append(f"\n--- {doc['source']} (Document {i+1}) ---")
                        context_parts.append(doc['content'])
                        context_parts.append(f"[Source: {doc['source']}]")
            
            # Live BGP state
            if QueryType.LIVE_BGP in query_types:
                bgp_state = self.get_live_bgp_state(entities)
                bgp_data["live_state"] = bgp_state
                
                if bgp_state.get("routes"):
                    context_parts.append("\n=== CURRENT BGP STATE ===")
                    for route in bgp_state["routes"]:
                        if route["type"] == "exact_match":
                            context_parts.append(f"Prefix: {route['prefix']}")
                            context_parts.append(f"Origin AS: {route['data'].get('origin_as')}")
                            context_parts.append(f"AS Path: {route['data'].get('as_path')}")
                        elif route["type"] == "longest_prefix_match":
                            context_parts.append(f"IP: {route['ip']} matches prefix: {route['matching_prefix']}")
                            context_parts.append(f"Origin AS: {route['data'].get('origin_as')}")
                        elif route["type"] == "as_originated":
                            context_parts.append(f"AS{route['asn']} originates {len(route['prefixes'])} prefixes")
            
            # Always fetch IRR data and historical data for ANY prefix query (security critical)
            # Also handle RPKI if explicitly requested
            if entities.get("prefixes"):
                # Get the first prefix for validation
                prefix = entities.get("prefixes", [None])[0]
                origin_as = None
                
                # First, try to get BGP state to find origin AS
                if not bgp_data.get("live_state"):
                    bgp_state = self.get_live_bgp_state(entities)
                    bgp_data["live_state"] = bgp_state
                
                if bgp_data.get("live_state", {}).get("routes"):
                    for route in bgp_data["live_state"]["routes"]:
                        if route.get("data", {}).get("origin_as"):
                            origin_as = route["data"]["origin_as"]
                            if not prefix and route["type"] == "exact_match":
                                prefix = route["prefix"]
                            break
                
                if prefix:
                    # Always get historical data - important for flap analysis
                    historical_updates = self.get_historical_data(entities, prefix)
                    bgp_data["historical_updates"] = historical_updates
                    
                    # Always fetch IRR data - critical security information
                    validation_data = {"rpki": None, "irr": None}
                    try:
                        irr_data = fetch_whois_data(prefix)
                        validation_data["irr"] = irr_data
                        print(f"IRR data fetched: {irr_data}")
                    except Exception as e:
                        print(f"IRR fetch error: {e}")
                    
                    # Only fetch RPKI for explicit RPKI queries to avoid unnecessary API calls
                    if QueryType.RPKI_VALIDATION in query_types and origin_as:
                        try:
                            rpki_data = fetch_rpki_validation(prefix, f"AS{origin_as}")
                            validation_data["rpki"] = rpki_data
                            print(f"RPKI data fetched: {rpki_data}")
                        except Exception as e:
                            print(f"RPKI fetch error: {e}")
                    
                    bgp_data["validation"] = validation_data
                    
                    # Always run heuristic analysis with flap detection for prefix queries
                    if origin_as:
                        live_state = bgp_data.get("live_state", {}).get("routes", [{}])[0].get("data", {})
                        analysis = analyze_bgp_discrepancies(
                            live_state, 
                            validation_data["rpki"], 
                            validation_data["irr"],
                            historical_updates
                        )
                        bgp_data["analysis"] = analysis
                        
                        # Add analysis to context
                        if analysis:
                            context_parts.append("\n=== SECURITY ANALYSIS ===")
                            if analysis.get("flags"):
                                context_parts.append(f"Flags: {', '.join(analysis['flags'])}")
                            if analysis.get("flap_analysis"):
                                flap = analysis["flap_analysis"]
                                context_parts.append(f"Route Flapping: {flap['pattern_analysis']}")
                    
                    # Always add validation data to context if available
                    if validation_data.get("rpki") or validation_data.get("irr"):
                        context_parts.append("\n=== VALIDATION DATA ===")
                        if validation_data.get("rpki"):
                            rpki = validation_data["rpki"]
                            context_parts.append(f"RPKI Status: {rpki.get('rpki_status')}")
                            context_parts.append(f"ROA Details: {rpki.get('roa_details')}")
                            context_parts.append(f"Is Hijack: {rpki.get('is_hijack')}")
                        if validation_data.get("irr"):
                            irr = validation_data["irr"]
                            context_parts.append(f"IRR Origins: {irr.get('irr_origins')}")
                            context_parts.append(f"IRR Authorities: {irr.get('authorities')}")
                            context_parts.append(f"IRR Status: {irr.get('status')}")
                    
                    print(f"Final context parts: {len(context_parts)} sections")
            
            # Step 4: Generate response using LLM chain
            context = "\n".join(context_parts) if context_parts else "No specific data available."
            answer = self.chain.generate_response(context=context, query=query, entities=entities)
            
            # Step 5: Return structured result
            return {
                "query": query,
                "entities": entities,
                "query_types": query_types,
                "bgp_data": bgp_data,
                "answer": answer,
                "processing_time": time.time() - start_time
            }
            
        except Exception as e:
            print(f"Error processing query: {e}")
            import traceback
            traceback.print_exc()
            return {
                "query": query,
                "entities": {},
                "error": str(e),
                "answer": f"Sorry, I encountered an error: {str(e)}",
                "processing_time": time.time() - start_time
            }


if __name__ == "__main__":
    # Simple test
    router = CleanBGPRouter()
    result = router.query("What is BGP?")
    print(result["answer"]) 