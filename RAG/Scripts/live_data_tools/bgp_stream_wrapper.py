from typing import Dict, List, Optional, Union
import pybgpstream
from datetime import datetime, timedelta
import pandas as pd
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class BGPUpdate:
    timestamp: datetime
    prefix: str
    as_path: str
    update_type: str  # 'A' for announce, 'W' for withdraw
    origin_as: Optional[str]
    collector: str
    peer_address: Optional[str] = None
    peer_asn: Optional[int] = None
    next_hop: Optional[str] = None
    communities: Optional[str] = None
    med: Optional[int] = None
    local_pref: Optional[int] = None
    atomic_aggregate: Optional[bool] = None
    aggregator: Optional[str] = None

class CollectorConfig:
    # Route Views collectors
    ROUTE_VIEWS = {
        "route-views2": "Main Route Views collector",
        "route-views.amsix": "Amsterdam Internet Exchange collector",
        "route-views.linx": "London Internet Exchange collector",
        "route-views.sg": "Singapore collector"
    }
    
    # RIPE RIS collectors
    RIPE_RIS = {
        "rrc00": "Amsterdam, Netherlands",
        "rrc03": "London, United Kingdom",
        "rrc04": "Geneva, Switzerland",
        "rrc05": "Vienna, Austria"
    }

class BGPStreamWrapper:
    """Wrapper for pybgpstream with built-in limitations and smart data handling."""
    
    # Constants for data limitations
    MAX_BGP_ENTRIES = None  # No limit on entries
    MAX_LOOKBACK_MINUTES = 60  # Maximum time to look back
    DEFAULT_COLLECTORS = ["route-views2", "rrc00"]  # Default collectors to use
    
    # Known valid collector names
    VALID_COLLECTORS = {
        # Route Views
        "route-views2": "route-views2",
        "route-views.amsix": "route-views.amsix", 
        "route-views.linx": "route-views.linx",
        "route-views.sg": "route-views.sg",
        # RIPE RIS
        "rrc00": "rrc00",
        "rrc03": "rrc03",
        "rrc04": "rrc04",
        "rrc05": "rrc05",
        # Aliases (maps to canonical names)
        "amsix": "route-views.amsix",
        "ams-ix": "route-views.amsix",
        "linx": "route-views.linx",
        "london": "route-views.linx"
    }
    
    def __init__(self, collectors: Optional[List[str]] = None):
        """Initialize the BGP stream wrapper."""
        self.stream = None
        self._cache = defaultdict(list)  # Simple cache for recent queries
        self.collectors = self._normalize_collectors(collectors) if collectors else self.DEFAULT_COLLECTORS
    
    def _normalize_collectors(self, collectors: List[str]) -> List[str]:
        """Normalize collector names to ensure they're valid for BGPStream."""
        if not collectors:
            return self.DEFAULT_COLLECTORS
            
        normalized = []
        for collector in collectors:
            # Skip None or empty strings
            if not collector:
                continue
                
            # If it's a known collector or alias, use the canonical name
            if collector in self.VALID_COLLECTORS:
                normalized.append(self.VALID_COLLECTORS[collector])
            else:
                print(f"WARNING: Unknown collector '{collector}', skipping")
        
        # If we ended up with no valid collectors, use the default
        if not normalized:
            print(f"WARNING: No valid collectors found, using default: {self.DEFAULT_COLLECTORS}")
            return self.DEFAULT_COLLECTORS
            
        # Remove duplicates while preserving order
        normalized_unique = []
        for c in normalized:
            if c not in normalized_unique:
                normalized_unique.append(c)
                
        return normalized_unique
    
    def _create_stream(self, 
                      start_time: datetime,
                      end_time: datetime,
                      collectors: Optional[List[str]] = None) -> pybgpstream.BGPStream:
        """Create a new BGP stream with the specified parameters."""
        # Normalize collector names
        normalized_collectors = self._normalize_collectors(collectors) if collectors else self.collectors
        
        try:
            stream = pybgpstream.BGPStream(
                from_time=start_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                until_time=end_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                collectors=normalized_collectors,
                record_type="updates"
            )
            return stream
        except Exception as e:
            print(f"ERROR creating BGPStream: {str(e)}")
            # Create a fallback stream with minimal settings
            return pybgpstream.BGPStream(
                from_time=start_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                until_time=end_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                collectors=self.DEFAULT_COLLECTORS,
                record_type="updates"
            )
    
    def get_prefix_updates(self, 
                          prefix: Optional[str] = None,
                          asn: Optional[str] = None,
                          minutes: int = 5,
                          collectors: Optional[List[str]] = None) -> List[BGPUpdate]:
        """
        Get BGP updates for a specific prefix and/or ASN.
        
        Args:
            prefix: Optional prefix to filter for
            asn: Optional ASN to filter for
            minutes: Number of minutes to look back (max 60)
            collectors: Optional list of collectors to filter for
            
        Returns:
            List of BGPUpdate objects
        """
        try:
            # Enforce time limitation
            minutes = min(max(1, minutes), self.MAX_LOOKBACK_MINUTES)
            
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=minutes)
            
            # Use provided collectors or instance default, ensuring they're normalized
            collectors_to_use = self._normalize_collectors(collectors) if collectors else self.collectors
            
            # Print debug info
            print(f"DEBUG: Querying BGP updates from {start_time} to {end_time} UTC")
            print(f"DEBUG: Prefix filter: {prefix}")
            print(f"DEBUG: ASN filter: {asn}")
            print(f"DEBUG: Collectors: {collectors_to_use}")
            
            # Create stream with explicit collectors parameter
            self.stream = self._create_stream(start_time, end_time, collectors_to_use)
            updates = []
            
            for elem in self.stream:
                try:
                    # Extract basic information
                    current_prefix = elem.fields.get("prefix", "")
                    as_path = elem.fields.get("as-path", "")
                    origin = as_path.split()[-1] if as_path else None
                    
                    # Apply filters
                    if prefix and current_prefix != prefix:
                        continue
                    if asn and (not as_path or asn not in as_path.split()):
                        continue
                    
                    # Create update object
                    update = BGPUpdate(
                        timestamp=datetime.utcfromtimestamp(elem.time),  # Fixed to use UTC time
                        prefix=current_prefix,
                        as_path=as_path,
                        update_type=elem.type,
                        origin_as=origin,
                        collector=elem.collector,
                        peer_address=str(elem.peer_address) if elem.peer_address else None,
                        peer_asn=elem.peer_asn,
                        next_hop=elem.fields.get("next-hop"),
                        communities=str(elem.fields.get("communities")) if elem.fields.get("communities") else None,
                        med=elem.fields.get("med"),
                        local_pref=elem.fields.get("local-pref"),
                        atomic_aggregate='atomic-aggregate' in elem.fields,
                        aggregator=elem.fields.get("aggregator")
                    )
                    updates.append(update)
                    
                    # Check entry limit
                    if len(updates) >= self.MAX_BGP_ENTRIES:
                        break
                except Exception as e:
                    print(f"Error processing BGP element: {e}")
                    continue
                    
        except Exception as e:
            print(f"Error fetching BGP updates: {e}")
            return []
            
        # Print how many updates were found
        print(f"DEBUG: Found {len(updates)} updates")
        
        return updates
    
    def get_prefix_updates_in_range(self, 
                          prefix: Optional[str] = None,
                          asn: Optional[str] = None,
                          start_time: datetime = None,
                          end_time: datetime = None,
                          collectors: Optional[List[str]] = None) -> List[BGPUpdate]:
        """
        Get BGP updates for a specific prefix and/or ASN within a specific time range.
        
        Args:
            prefix: Optional prefix to filter for
            asn: Optional ASN to filter for
            start_time: Start of the time range
            end_time: End of the time range
            collectors: Optional list of collectors to filter for
            
        Returns:
            List of BGPUpdate objects
        """
        try:
            if not start_time or not end_time:
                print("ERROR: Invalid time range provided")
                return []
                
            # Enforce a maximum window size (to prevent extremely large queries)
            time_diff = end_time - start_time
            max_diff = timedelta(hours=1)  # Maximum 1 hour window
            if time_diff > max_diff:
                print(f"WARNING: Time range too large ({time_diff}), limiting to {max_diff}")
                end_time = start_time + max_diff
            
            collectors_to_use = self._normalize_collectors(collectors) if collectors else self.collectors
            
            print(f"DEBUG: Querying BGP updates from {start_time} to {end_time} UTC")
            print(f"DEBUG: Prefix filter: {prefix}")
            print(f"DEBUG: ASN filter: {asn}")
            print(f"DEBUG: Collectors: {collectors_to_use}")
            
            self.stream = self._create_stream(start_time, end_time, collectors_to_use)
            updates = []
            
            for elem in self.stream:
                try:
                    current_prefix = elem.fields.get("prefix", "")
                    as_path = elem.fields.get("as-path", "")
                    origin = as_path.split()[-1] if as_path else None
                    
                    if prefix and current_prefix != prefix:
                        continue
                    if asn and (not as_path or asn not in as_path.split()):
                        continue
                    
                    timestamp = datetime.utcfromtimestamp(elem.time)
                    if timestamp < start_time or timestamp > end_time:
                        continue
                    
                    update = BGPUpdate(
                        timestamp=timestamp,
                        prefix=current_prefix,
                        as_path=as_path,
                        update_type=elem.type,
                        origin_as=origin,
                        collector=elem.collector,
                        peer_address=str(elem.peer_address) if elem.peer_address else None,
                        peer_asn=elem.peer_asn,
                        next_hop=elem.fields.get("next-hop"),
                        communities=str(elem.fields.get("communities")) if elem.fields.get("communities") else None,
                        med=elem.fields.get("med"),
                        local_pref=elem.fields.get("local-pref"),
                        atomic_aggregate='atomic-aggregate' in elem.fields,
                        aggregator=elem.fields.get("aggregator")
                    )
                    updates.append(update)
                    
                    if len(updates) % 10000 == 0:  # Print progress every 10k updates
                        print(f"Processed {len(updates)} updates...")
                        
                except Exception as e:
                    print(f"Error processing BGP element: {e}")
                    continue
                    
        except Exception as e:
            print(f"Error fetching BGP updates: {e}")
            return []
            
        print(f"DEBUG: Found {len(updates)} updates")
        return updates
    
    def summarize_updates(self, updates: List[BGPUpdate]) -> Dict:
        """
        Create a summary of BGP updates.
        
        Returns a dictionary with:
        - total_updates
        - announcements
        - withdrawals
        - unique_as_paths
        - time_range
        """
        if not updates:
            return {
                "total_updates": 0,
                "status": "No updates found"
            }
            
        # Sort updates by timestamp
        updates.sort(key=lambda x: x.timestamp)
        
        # Collect statistics
        announcements = sum(1 for u in updates if u.update_type == 'A')
        withdrawals = sum(1 for u in updates if u.update_type == 'W')
        unique_as_paths = len(set(u.as_path for u in updates if u.as_path))
        
        return {
            "total_updates": len(updates),
            "announcements": announcements,
            "withdrawals": withdrawals,
            "unique_as_paths": unique_as_paths,
            "time_range": {
                "start": updates[0].timestamp.isoformat(),
                "end": updates[-1].timestamp.isoformat()
            },
            "most_recent_state": "withdrawn" if updates[-1].update_type == 'W' else "announced"
        }
    
    # def detect_flapping(self, updates: List[BGPUpdate], min_transitions: int = 3) -> bool:
    #     """
    #     Detect if a prefix is flapping based on the number of state transitions.
    #     """
    #     if len(updates) < min_transitions:
    #         return False
            
    #     transitions = 0
    #     prev_type = updates[0].update_type
        
    #     for update in updates[1:]:
    #         if update.update_type != prev_type:
    #             transitions += 1
    #             prev_type = update.update_type
                
    #         if transitions >= min_transitions:
    #             return True
                
    #     return False
