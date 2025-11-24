#!/usr/bin/env python3
"""
llm_entity_extractor.py - Simple LLM-based entity extraction for BGP queries
"""

import os
import json
import openai
from typing import Dict, List, Optional

from .entity_extractor import RegexEntityExtractor


class LLMEntityExtractor:
    """Simple LLM-based entity extractor using OpenAI's API"""
    
    def __init__(self, model: str = "gpt-4.1-mini", temperature: float = 0.1, fallback_to_regex: bool = True):
        self.model = model
        self.temperature = temperature
        self.fallback_to_regex = fallback_to_regex
        
        # Initialize regex fallback
        if fallback_to_regex:
            self.regex_extractor = RegexEntityExtractor()
        
        # Check for API key
        if not os.environ.get('OPENAI_API_KEY'):
            raise ValueError("OpenAI API key not found in environment variables")
    
    def extract(self, query: str) -> Dict[str, List[str]]:
        """Extract entities using LLM with regex fallback"""
        try:
            system_prompt = """You are an expert BGP network engineer. Extract specific entities from BGP routing queries.

Extract these entity types from the query:

1. **ip_addresses**: Individual IPv4 or IPv6 addresses WITHOUT prefix length
   - NEVER extract IP addresses that have /XX after them - those are prefixes
   - Host routes like "8.8.8.8/32" or "2001:db8::1/128" are prefixes, NOT ip_addresses
   - Only extract standalone IPs without any slash notation
   - Convert word forms: "ate.ate.ate.ate" → "8.8.8.8"
   - Only extract valid IP addresses (IPv4: 0-255 per octet)

2. **prefixes**: IP prefixes WITH CIDR notation (IPv4 or IPv6)
   - Must have /XX to be a prefix (including host routes /32, /128)
   - Only extract VALID prefixes with valid IP and prefix length
   - DO NOT extract malformed prefixes like "256.256.256.256/33"
   - Examples: "8.8.8.8/32", "192.168.1.0/24", "2001:db8::/32"

3. **asns**: Autonomous System Numbers (extract ONLY the numeric part)
   - Extract from formats: AS15169, ASN13335, AS-64512
   - Convert word forms: "fifteen one six nine" → "15169"
   - NEVER extract community values (anything with colon like "65000:100")
   - NEVER extract empty strings - skip if no valid ASN found
   - Only extract clear numeric AS identifiers

4. **keywords**: BGP and networking related terms
   - Include technical terms, actions, network concepts
   - Include misspelled versions exactly as written
   - Exclude common words and pronouns

5. **time_references**: Time-related expressions (extract components separately)
   - Include temporal prepositions: "since", "before", "during", "at"
   - Preserve misspellings exactly: "yestrday" stays "yestrday"
   - Include all temporal components, not just the first one
    - Keep ISO dates and times intact: "2025-05-04", "08:00:00"
    - Relative phrases may be split: "last week" → ["last", "week"]

CRITICAL RULES - FOLLOW EXACTLY:
- Any IP with /XX is a PREFIX, never extract the IP part separately
- Host routes (/32, /128) are PREFIXES not IP addresses
- Community values (X:Y format) are NOT ASNs
- Never return empty strings in any list
- Extract ALL temporal words, not just the first one
- Preserve all misspellings exactly as written

Return ONLY a JSON object with all 5 keys:
{"ip_addresses": [], "prefixes": [], "asns": [], "keywords": [], "time_references": []}"""

            response = openai.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Extract BGP entities from this query: {query}"}
                ],
                temperature=self.temperature
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # Ensure all keys exist
            for key in ['ip_addresses', 'prefixes', 'asns', 'keywords', 'time_references']:
                if key not in result:
                    result[key] = []
            
            return result
            
        except Exception as e:
            print(f"LLM extraction failed: {e}")
            if self.fallback_to_regex:
                return self.regex_extractor.extract(query)
            return {'ip_addresses': [], 'prefixes': [], 'asns': [], 'keywords': [], 'time_references': []}
    
    def extract_dict(self, query: str) -> Dict[str, List[str]]:
        """Alias for backward compatibility"""
        return self.extract(query)


class HybridEntityExtractor:
    """Simple hybrid extractor that combines LLM and regex"""
    
    def __init__(self, strategy: str = "llm_primary"):
        self.strategy = strategy
        self.regex_extractor = RegexEntityExtractor()
        self.llm_extractor = LLMEntityExtractor()
    
    def extract(self, query: str) -> Dict[str, List[str]]:
        """Extract using hybrid strategy"""
        if self.strategy == "llm_primary":
            return self.llm_extractor.extract(query)
        elif self.strategy == "merge_results":
            regex_result = self.regex_extractor.extract(query)
            llm_result = self.llm_extractor.extract(query)
            
            merged = {}
            for key in regex_result.keys():
                merged[key] = list(set(regex_result[key] + llm_result.get(key, [])))
            return merged
        else:  # regex_validation
            return self.regex_extractor.extract(query)
    
    def extract_dict(self, query: str) -> Dict[str, List[str]]:
        """Alias for backward compatibility"""
        return self.extract(query) 