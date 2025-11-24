#!/usr/bin/env python3
"""
entity_extractor.py - Simple entity extraction for BGP queries
"""

import re
from typing import Dict, List


class RegexEntityExtractor:
    """Simple regex-based entity extractor for BGP queries"""
    
    def __init__(self):
        # Simple patterns for common entities
        self.ip_pattern = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')
        self.prefix_pattern = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}/\d{1,2}\b')
        self.asn_pattern = re.compile(r'\bAS\s*(\d+)\b', re.IGNORECASE)
        
        # BGP keywords
        self.keywords = {
            "route", "prefix", "path", "bgp", "origin", "as", "rpki", "roa", 
            "routing", "announce", "valid", "invalid", "peer", "hijack"
        }
        
        # Time words
        self.time_words = {
            "yesterday", "today", "now", "current", "hour", "minute", "week", 
            "month", "day", "ago", "since", "last", "this", "recent"
        }
    
    def extract(self, query: str) -> Dict[str, List[str]]:
        """Extract entities from query"""
        # Find IPs (excluding those that are part of prefixes)
        all_ips = self.ip_pattern.findall(query)
        prefixes = self.prefix_pattern.findall(query)
        prefix_ips = [p.split('/')[0] for p in prefixes]
        ip_addresses = [ip for ip in all_ips if ip not in prefix_ips]
        
        # Find AS numbers
        asns = self.asn_pattern.findall(query)
        
        # Find keywords and time references
        query_lower = query.lower()
        keywords = [kw for kw in self.keywords if kw in query_lower]
        time_references = [tw for tw in self.time_words if tw in query_lower]
        
        return {
            "ip_addresses": list(set(ip_addresses)),
            "prefixes": list(set(prefixes)),
            "asns": list(set(asns)),
            "keywords": list(set(keywords)),
            "time_references": list(set(time_references))
        }
    
    def extract_dict(self, query: str) -> Dict[str, List[str]]:
        """Alias for backward compatibility"""
        return self.extract(query) 