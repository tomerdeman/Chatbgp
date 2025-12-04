#!/usr/bin/env python3
"""
rib_to_duckdb.py - Parses a BGP RIB dump (MRT format) and loads it into DuckDB.
"""

from _pybgpstream import BGPStream # Or just `import bgpstream` if installed that way
import duckdb
import time
import os
from datetime import datetime

DUCKDB_FILE = "bgp_rib_snapshot.duckdb"
RIB_TABLE_NAME = "rib_entries"

def create_rib_table(con):
    """Creates the RIB table in DuckDB if it doesn't exist."""
    try:
        # Ensure INET extension is loaded
        con.execute("INSTALL inet;")
        con.execute("LOAD inet;")
    except duckdb.IOException as e:
        print(f"Info: Could not (re)install or load INET extension (may already be loaded or static): {e}")
    except Exception as e:
        print(f"Warning: Error with INET extension: {e}")


    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {RIB_TABLE_NAME} (
            dump_time TIMESTAMP,          -- Timestamp of this particular dump processing
            record_time TIMESTAMP,        -- Timestamp from the BGP record itself
            collector VARCHAR,
            peer_address INET,
            peer_asn BIGINT,
            prefix INET,
            as_path VARCHAR,              -- Store as space-separated string
            origin_as BIGINT,
            next_hop INET,
            communities VARCHAR,          -- Store as space-separated string of "ASN:VAL"
            med BIGINT,
            local_pref BIGINT,
            atomic_aggregate BOOLEAN,
            aggregator_as BIGINT,
            aggregator_address INET
        );
    """)
    print(f"Table '{RIB_TABLE_NAME}' ensured in {DUCKDB_FILE}")

def create_live_updates_table(con):
    """Creates a table specifically for live BGP updates if it doesn't exist."""
    try:
        # Ensure INET extension is loaded (if not already)
        try:
            con.execute("INSTALL inet;")
            con.execute("LOAD inet;")
        except:
            pass  # May already be loaded

        con.execute("""
            CREATE TABLE IF NOT EXISTS rrc03_updates (
                timestamp TIMESTAMP,           -- When the update was received
                collector VARCHAR,            -- Which collector received it
                peer_address INET,            -- The peer that sent the update
                peer_asn BIGINT,             -- The peer's ASN
                prefix INET,                  -- The prefix being updated
                update_type CHAR(1),          -- 'A' for announcement, 'W' for withdrawal
                as_path VARCHAR,              -- AS path (NULL for withdrawals)
                origin_as BIGINT,             -- Origin AS (NULL for withdrawals)
                next_hop INET,                -- Next hop (NULL for withdrawals)
                communities VARCHAR,          -- Communities (NULL for withdrawals)
                med BIGINT,                   -- MED (NULL for withdrawals)
                local_pref BIGINT,           -- Local pref (NULL for withdrawals)
                atomic_aggregate BOOLEAN,     -- Atomic aggregate flag (NULL for withdrawals)
                aggregator VARCHAR            -- Aggregator info (NULL for withdrawals)
            );
        """)
        
        # Add an index on timestamp for efficient historical queries
        con.execute("CREATE INDEX IF NOT EXISTS idx_updates_timestamp ON rrc03_updates(timestamp);")
        # Add an index on prefix for prefix-specific queries
        con.execute("CREATE INDEX IF NOT EXISTS idx_updates_prefix ON rrc03_updates(prefix);")
        
        print("Live updates table 'rrc03_updates' ensured with indexes")
        
    except Exception as e:
        print(f"Error creating live updates table: {e}")

def store_live_update(update, con):
    """
    Store a single BGP update in the live updates table.
    Handles both announcements and withdrawals appropriately.
    """
    try:
        # Check for valid update type first
        if update.update_type not in ['A', 'W']:
            # Silently skip state messages and other types
            return True

        # Check for empty prefix
        if not update.prefix or update.prefix.strip() == "":
            print(f"Error: Empty prefix in {update.update_type} update from {update.collector}")
            return False

        if update.update_type == 'W':
            # For withdrawals, only store basic info, rest will be NULL
            con.execute("""
                INSERT INTO rrc03_updates (
                    timestamp, collector, peer_address, peer_asn, 
                    prefix, update_type
                ) VALUES (?, ?, ?, ?, ?, ?);
            """, [
                update.timestamp,
                update.collector,
                update.peer_address,
                update.peer_asn,
                update.prefix,
                'W'
            ])
        else:
            # For announcements, store all available information
            con.execute("""
                INSERT INTO rrc03_updates (
                    timestamp, collector, peer_address, peer_asn,
                    prefix, update_type, as_path, origin_as, next_hop,
                    communities, med, local_pref, atomic_aggregate, aggregator
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, [
                update.timestamp,
                update.collector,
                update.peer_address,
                update.peer_asn,
                update.prefix,
                'A',
                update.as_path,
                update.origin_as,
                update.next_hop,
                update.communities,
                update.med,
                update.local_pref,
                update.atomic_aggregate,
                update.aggregator
            ])
        return True
    except Exception as e:
        print(f"Error storing {update.update_type} update for prefix '{update.prefix}': {str(e)}")
        return False

def parse_as_path_to_data(as_path_str):
    """
    Parses an AS path string into a list of integers and extracts the origin AS.
    Returns (list_of_as_numbers, origin_as_int) or (None, None) if invalid.
    Simplistic AS_SET handling (takes first element).
    """
    if not as_path_str:
        return None, None
    
    as_numbers = []
    raw_asns = as_path_str.split()
    for asn_token in raw_asns:
        try:
            if '{' in asn_token: # Handle simple AS_SETs like {123}, {123,456}
                clean_token = asn_token.strip('{}')
                parts = clean_token.split(',')
                if parts and parts[0].isdigit(): # Take the first AS in the set
                    as_numbers.append(int(parts[0]))
            elif '(' in asn_token: # Handle simple AS_CONFED_SEQUENCE (123) - skip for path, not for origin
                # More complex parsing needed if you want to fully represent confederations
                pass # For now, we just skip adding confederation sequences to the path list
            elif asn_token.isdigit():
                as_numbers.append(int(asn_token))
        except ValueError:
            # print(f"Warning: Could not parse ASN token '{asn_token}' in path '{as_path_str}'")
            continue # Skip non-integer tokens that are not sets/confeds

    if not as_numbers:
        # Try to find origin even if path parsing is tricky (e.g. only confed members)
        # A more robust parser would extract the "originating member AS" from a confederation.
        # For simplicity, if as_numbers is empty but raw_asns was not, this indicates complex structure.
        # We'll return None for origin in this simplified version if as_numbers is empty.
        return " ".join(raw_asns) if raw_asns else None, None # Return raw path if parsing to list fails

    origin_as = as_numbers[-1] if as_numbers else None
    return " ".join(map(str, as_numbers)), origin_as


def parse_communities_to_string(communities_list):
    """
    Converts a list of communities to a space-separated string.
    Handles both dictionary format and string format communities.
    """
    if not communities_list:
        return None
        
    # If we already have a string, return it
    if isinstance(communities_list, str):
        return communities_list
        
    result = []
    for c in communities_list:
        if isinstance(c, dict):
            # Handle dictionary format
            result.append(f"{c.get('asn',0)}:{c.get('value',0)}")
        elif isinstance(c, str):
            # Handle string format
            result.append(c)
        else:
            # Skip invalid formats
            continue
            
    return " ".join(result) if result else None


def load_rib_to_duckdb(rib_file_path, db_file=DUCKDB_FILE, table_name=RIB_TABLE_NAME):
    """
    Parses a BGP RIB dump (MRT) file and loads its entries into a DuckDB table.
    """
    if not os.path.exists(rib_file_path):
        print(f"Error: RIB file not found at {rib_file_path}")
        return

    con = duckdb.connect(database=db_file, read_only=False)
    create_rib_table(con)

    # Optional: Clear the table if you want to reload fresh each time
    # print(f"Clearing existing data from {table_name}...")
    # con.execute(f"DELETE FROM {table_name};")

    stream = BGPStream()
    stream.set_data_interface("singlefile")
    stream.set_data_interface_option("singlefile", "rib-file", rib_file_path)
    stream.start()

    print(f"Processing RIB file: {rib_file_path}...")
    dump_processing_time = datetime.now() # Timestamp for this batch of entries
    inserted_count = 0
    processed_count = 0
    batch_data = []
    BATCH_SIZE = 10000 # Insert in batches for better performance

    while True:
        rec = stream.get_next_record()
        if rec is None:
            break
        if rec.status != "valid":
            continue
        
        processed_count += 1
        if processed_count % 100000 == 0:
            print(f"  Processed {processed_count} BGP records...")

        elem = rec.get_next_elem()
        while elem:
            if elem.type == "R":  # RIB Entry
                as_path_str, origin_as = parse_as_path_to_data(elem.fields.get("as-path"))
                communities_str = parse_communities_to_string(elem.fields.get("communities"))
                
                # Prepare data for insertion, handle missing fields gracefully
                entry_data = (
                    dump_processing_time,
                    datetime.fromtimestamp(rec.time) if rec.time else None,
                    rec.collector,
                    str(elem.peer_address) if elem.peer_address else None,
                    elem.peer_asn,
                    elem.fields.get("prefix"),
                    as_path_str,
                    origin_as,
                    elem.fields.get("next-hop"),
                    communities_str,
                    elem.fields.get("med"),
                    elem.fields.get("local-pref"),
                    'atomic-aggregate' in elem.fields, # Boolean flag
                    elem.fields.get("aggregator", "::").split(":",1)[0] if "aggregator" in elem.fields and elem.fields.get("aggregator").count(":") >=1 else None, # AGGREGATOR AS
                    elem.fields.get("aggregator", "::").split(":",1)[-1] if "aggregator" in elem.fields and elem.fields.get("aggregator").count(":") >=1 else None # AGGREGATOR Address
                )
                batch_data.append(entry_data)

                if len(batch_data) >= BATCH_SIZE:
                    con.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", batch_data)
                    inserted_count += len(batch_data)
                    print(f"  Inserted {inserted_count} RIB entries into DuckDB...")
                    batch_data = []
            
            elem = rec.get_next_elem()

    # Insert any remaining data in the last batch
    if batch_data:
        con.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", batch_data)
        inserted_count += len(batch_data)
        print(f"  Inserted final {len(batch_data)} RIB entries. Total inserted: {inserted_count}")

    con.close()
    print(f"Finished loading RIB data from {rib_file_path} into {db_file}")

if __name__ == "__main__":
    # --- IMPORTANT: Replace with the actual path to YOUR bview RIB file ---
    # Example: rib_file = "/path/to/your/mrt_rib_dumps/bview.20240101.0000.gz"
    # For testing, you might use the one from your previous script if it's a RIB dump:
    rib_file = "RAG/Data/other_stuff/bview.20250504.0800" # Make sure this is an MRT RIB file

    if not os.path.exists(rib_file) or rib_file == "path/to/your/bview.rib.file":
         print(f"ERROR: Please update the 'rib_file' variable in the __main__ block with a valid path to an MRT RIB dump file.")
    else:
        start_total_time = time.time()
        load_rib_to_duckdb(rib_file)
        end_total_time = time.time()
        print(f"Total script execution time: {end_total_time - start_total_time:.2f} seconds")

        # Example query to verify
        con_verify = duckdb.connect(database=DUCKDB_FILE, read_only=True)
        try:
            con_verify.execute("INSTALL inet; LOAD inet;") # Ensure INET for querying
        except: pass # May already be loaded
        
        print("\nVerifying data (first 5 entries):")
        results = con_verify.execute(f"SELECT prefix, origin_as, as_path FROM {RIB_TABLE_NAME} LIMIT 5").fetchall()
        for row in results:
            print(row)
        
        print(f"\nTotal entries in {RIB_TABLE_NAME}:")
        count = con_verify.execute(f"SELECT COUNT(*) FROM {RIB_TABLE_NAME}").fetchone()
        print(count[0] if count else 0)
        con_verify.close()