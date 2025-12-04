#!/usr/bin/env python3
"""
bgp_radix.py - Parse RIB dump into radix trees and maintain live updates
"""

from _pybgpstream import BGPStream # Assuming this is how you import it
import radix # py-radix
import gzip
import pickle
import time
import os
from Scripts.live_data_tools.bgp_stream_wrapper import BGPStreamWrapper
from datetime import datetime, timedelta
import duckdb
from bgp_to_duckdb import create_rib_table, create_live_updates_table, store_live_update

# Constants
DUCKDB_FILE = "bgp_rib_snapshot.duckdb"

def init_duckdb_connection():
    """Initialize DuckDB connection and ensure tables exist."""
    con = duckdb.connect(DUCKDB_FILE)
    create_rib_table(con)
    create_live_updates_table(con)
    return con

# You'll need your RIB parsing logic here if you want to create from scratch
# For now, I'll assume the create_from_rib function would be separate or
# you primarily want to fix the load/save of existing pickles.

def load_or_create_trees_OPTIMIZED(): # Renamed for clarity
    """Load existing Radix tree objects if they exist, otherwise indicate failure to load."""
    rtree_v4_path = "radix_v4_obj.pkl.gz" # Use a new filename to avoid conflict
    rtree_v6_path = "radix_v6_obj.pkl.gz"

    if os.path.exists(rtree_v4_path) and os.path.exists(rtree_v6_path):
        print("Loading existing optimized Radix tree objects...")
        try:
            start_time = time.time()
            with gzip.open(rtree_v4_path, "rb") as f4:
                print("Loading IPv4 tree object...")
                rtree_v4 = pickle.load(f4)
                print(f"Loaded IPv4 tree with {len(rtree_v4.nodes())} nodes.") # py-radix uses .nodes()

            with gzip.open(rtree_v6_path, "rb") as f6:
                print("Loading IPv6 tree object...")
                rtree_v6 = pickle.load(f6)
                print(f"Loaded IPv6 tree with {len(rtree_v6.nodes())} nodes.")

            end_time = time.time()
            print(f"Trees loaded directly in {end_time - start_time:.2f} seconds!")
            return rtree_v4, rtree_v6
        except Exception as e:
            print(f"Error loading pickled Radix tree objects: {e}")
            print("You might need to recreate the pickle files using save_trees_OPTIMIZED if the format changed.")
            return None, None # Indicate failure
    else:
        print(f"Optimized pickle files not found ({rtree_v4_path}, {rtree_v6_path}).")
        print("Please create them first using a RIB dump and save_trees_OPTIMIZED.")
        return None, None # Indicate new trees need to be created from RIB and then saved

def save_trees_OPTIMIZED(rtree_v4, rtree_v6): # Renamed for clarity
    """Save Radix tree objects directly to disk using pickle."""
    rtree_v4_path = "radix_v4_obj.pkl.gz"
    rtree_v6_path = "radix_v6_obj.pkl.gz"
    print("\nSaving Radix tree objects to disk...")
    try:
        start_time = time.time()
        with gzip.open(rtree_v4_path, "wb") as f4:
            pickle.dump(rtree_v4, f4, protocol=pickle.HIGHEST_PROTOCOL)

        with gzip.open(rtree_v6_path, "wb") as f6:
            pickle.dump(rtree_v6, f6, protocol=pickle.HIGHEST_PROTOCOL)
        end_time = time.time()
        print(f"Trees saved directly in {end_time - start_time:.2f} seconds!")
    except Exception as e:
        print(f"Error saving Radix tree objects: {e}")

# Your RIB parsing function would look something like this (simplified):
def create_trees_from_rib(rib_file_path):
    print(f"Creating new trees from RIB: {rib_file_path}...")
    rtree_v4 = radix.Radix()
    rtree_v6 = radix.Radix()

    # Create and configure stream for initial RIB
    stream = BGPStream()
    stream.set_data_interface("singlefile")
    stream.set_data_interface_option("singlefile", "rib-file", "RAG/Data/other_stuff/bview.20250504.0800")

    # Print RIB filename timestamp
    rib_filename_time = datetime.strptime("20250504.0800", "%Y%m%d.%H%M")
    print(f"\nRIB filename timestamp: {rib_filename_time} UTC")

    # stream.add_filter(...) # Add any filters if necessary
    stream.start()

    processed_count = 0
    first_record_time = None
    last_record_time = None

    while True:
        rec = stream.get_next_record()
        if rec is None:
            break
        if rec.status != "valid":
            continue

        # Track record timestamps
        record_time = datetime.utcfromtimestamp(rec.time)
        if first_record_time is None:
            first_record_time = record_time
            print(f"First record timestamp: {first_record_time} UTC")
        last_record_time = record_time

        elem = rec.get_next_elem()
        while elem:
            if elem.type == "R":  # RIB entries
                prefix_str = elem.fields.get("prefix")
                as_path_str = elem.fields.get("as-path")

                if prefix_str and as_path_str:
                    try:
                        # Basic AS path parsing
                        as_numbers = []
                        raw_asns = as_path_str.split()
                        for asn_token in raw_asns:
                            if '{' in asn_token:
                                clean_token = asn_token.strip('{}')
                                first_asn_in_set = clean_token.split(',')[0]
                                if first_asn_in_set.isdigit():
                                    as_numbers.append(int(first_asn_in_set))
                            elif asn_token.isdigit():
                                as_numbers.append(int(asn_token))
                        
                        if not as_numbers:
                            elem = rec.get_next_elem()
                            continue
                        
                        origin_as = as_numbers[-1]
                        
                        rnode = None
                        if ":" in prefix_str:
                            rnode = rtree_v6.add(prefix_str)
                        else:
                            rnode = rtree_v4.add(prefix_str)
                        
                        rnode.data["origin_as"] = origin_as # Store as origin_as for consistency
                        rnode.data["as_path"] = as_numbers
                        # rnode.data["timestamp"] = rec.time # Timestamp of RIB entry if available
                        processed_count +=1
                        if processed_count % 100000 == 0:
                            print(f"Processed {processed_count} RIB entries...")

                    except (ValueError, IndexError, KeyError) as e:
                        # print(f"Skipping entry due to parsing error: {prefix_str}, {as_path_str}, Error: {e}")
                        pass # Be careful with silent passes in production
            elem = rec.get_next_elem()

    print(f"\nTimestamp Summary:")
    print(f"RIB filename time: {rib_filename_time} UTC")
    print(f"First record time: {first_record_time} UTC")
    print(f"Last record time: {last_record_time} UTC")
    print(f"Time span: {last_record_time - first_record_time}")
    print(f"\nFinished RIB processing. IPv4 nodes: {len(rtree_v4.nodes())}, IPv6 nodes: {len(rtree_v6.nodes())}")
    return rtree_v4, rtree_v6

def handle_live_updates(rtree_v4, rtree_v6):
    """Handle live BGP updates from rrc03 starting from RIB snapshot time."""
    print("\nStarting live BGP update processing...")
    
    # Create BGPStreamWrapper instance specifically for rrc03
    stream_wrapper = BGPStreamWrapper(collectors=["rrc03"])
    
    # Initialize DuckDB connection and create live updates table
    db_con = init_duckdb_connection()
    
    # Parse the RIB timestamp from filename (bview.20250504.0800)
    rib_time = datetime.strptime("20250504.0800", "%Y%m%d.%H%M")
    current_time = rib_time + timedelta(minutes=10)  # Only process 10 minutes
    
    print(f"\nFetching updates from RIB snapshot time: {rib_time} UTC")
    print(f"Until: {current_time} UTC (10 minute window)")
    
    update_count = 0
    last_save_count = 0
    SAVE_INTERVAL = 10000  # Save every 10,000 updates
    
    try:
        # Process historical updates in 1-minute chunks
        chunk_start = rib_time
        while chunk_start < current_time:
            chunk_end = min(chunk_start + timedelta(minutes=1), current_time)
            print(f"\nProcessing updates from {chunk_start} to {chunk_end}")
            
            historical_updates = stream_wrapper.get_prefix_updates_in_range(
                start_time=chunk_start,
                end_time=chunk_end
            )
            
            if historical_updates:
                # Store updates in DuckDB's live updates table
                stored_count = 0
                error_count = 0
                for update in historical_updates:
                    if store_live_update(update, db_con):
                        stored_count += 1
                    else:
                        error_count += 1
                print(f"Stored {stored_count} historical updates in DuckDB (Errors: {error_count})")
                
                # Get summary of this chunk
                summary = stream_wrapper.summarize_updates(historical_updates)
                print("\nUpdate Summary for this chunk:")
                print(f"Total Updates: {summary['total_updates']}")
                print(f"Announcements: {summary['announcements']}")
                print(f"Withdrawals: {summary['withdrawals']}")
                print(f"Unique AS Paths: {summary['unique_as_paths']}")
                
                # Update radix trees (minimal info for fast lookups)
                for update in historical_updates:
                    prefix_str = update.prefix
                    if not prefix_str:
                        continue

                    # Determine which tree to use
                    target_tree = rtree_v6 if ":" in prefix_str else rtree_v4
                    
                    if update.update_type == 'W':  # Withdrawal
                        if prefix_str in target_tree:
                            target_tree.delete(prefix_str)
                    
                    elif update.update_type == 'A':  # Announcement
                        if not update.as_path:
                            continue

                        try:
                            # Parse AS path (already cleaned by wrapper)
                            as_numbers = [int(asn) for asn in update.as_path.split()]
                            
                            if not as_numbers:
                                continue
                            
                            # Add or update the node (minimal info)
                            rnode = target_tree.add(prefix_str)
                            rnode.data["origin_as"] = int(update.origin_as) if update.origin_as else as_numbers[-1]
                            rnode.data["as_path"] = as_numbers
                            
                            update_count += 1
                            
                            # Save trees periodically
                            if update_count - last_save_count >= SAVE_INTERVAL:
                                print(f"\nSaving trees after {update_count} updates...")
                                save_trees_OPTIMIZED(rtree_v4, rtree_v6)
                                last_save_count = update_count
                                print("Trees saved successfully!")

                        except (ValueError, IndexError, KeyError) as e:
                            print(f"Error processing update: {e}")
                            continue
                
                print(f"\nCurrent tree sizes after chunk:")
                print(f"IPv4 nodes: {len(rtree_v4.nodes())}")
                print(f"IPv6 nodes: {len(rtree_v6.nodes())}")
            
            # Move to next chunk
            chunk_start = chunk_end
        
        print("\nFinished processing historical updates. Starting live update processing...")
        
        # Now continue with live updates
        while True:
            # Get updates from the last minute
            updates = stream_wrapper.get_prefix_updates(minutes=1)
            
            if updates:
                # Store in DuckDB's live updates table
                stored_count = 0
                error_count = 0
                for update in updates:
                    if store_live_update(update, db_con):
                        stored_count += 1
                    else:
                        error_count += 1
                print(f"Stored {stored_count} live updates in DuckDB (Errors: {error_count})")
                
                # Get summary of live updates
                summary = stream_wrapper.summarize_updates(updates)
                print("\nLive Update Summary:")
                print(f"Total Updates: {summary['total_updates']}")
                print(f"Announcements: {summary['announcements']}")
                print(f"Withdrawals: {summary['withdrawals']}")
                print(f"Unique AS Paths: {summary['unique_as_paths']}")
                
                # Update radix trees (minimal info for fast lookups)
                for update in updates:
                    prefix_str = update.prefix
                    if not prefix_str:
                        continue

                    # Determine which tree to use
                    target_tree = rtree_v6 if ":" in prefix_str else rtree_v4
                    
                    if update.update_type == 'W':  # Withdrawal
                        if prefix_str in target_tree:
                            target_tree.delete(prefix_str)
                    
                    elif update.update_type == 'A':  # Announcement
                        if not update.as_path:
                            continue

                        try:
                            # Parse AS path (already cleaned by wrapper)
                            as_numbers = [int(asn) for asn in update.as_path.split()]
                            
                            if not as_numbers:
                                continue
                            
                            # Add or update the node (minimal info)
                            rnode = target_tree.add(prefix_str)
                            rnode.data["origin_as"] = int(update.origin_as) if update.origin_as else as_numbers[-1]
                            rnode.data["as_path"] = as_numbers
                            
                            update_count += 1
                            
                            # Save trees periodically
                            if update_count - last_save_count >= SAVE_INTERVAL:
                                print(f"\nSaving trees after {update_count} updates...")
                                save_trees_OPTIMIZED(rtree_v4, rtree_v6)
                                last_save_count = update_count
                                print("Trees saved successfully!")

                        except (ValueError, IndexError, KeyError) as e:
                            print(f"Error processing update: {e}")
                            continue
                
                print(f"\nCurrent tree sizes:")
                print(f"IPv4 nodes: {len(rtree_v4.nodes())}")
                print(f"IPv6 nodes: {len(rtree_v6.nodes())}")
            else:
                print("No updates in the last minute, waiting...")
                time.sleep(30)  # Wait 30 seconds before checking again

    except KeyboardInterrupt:
        print("\nStopping live update processing...")
        # Save one final time before exiting
        save_trees_OPTIMIZED(rtree_v4, rtree_v6)
        db_con.close()
        print("Final save completed. Exiting.")

if __name__ == "__main__":
    print("Starting script...")

    # Attempt to load optimized trees
    rtree_v4, rtree_v6 = load_or_create_trees_OPTIMIZED()

    if rtree_v4 is None or rtree_v6 is None:
        print("Failed to load optimized trees. Attempting to create from RIB and save...")
        # Define your RIB file path
        RIB_FILE = "RAG/Data/other_stuff/bview.20250504.0800"  # Fixed path
        if not os.path.exists(RIB_FILE):
            print(f"RIB file {RIB_FILE} not found. Cannot create trees. Exiting.")
            exit(1)
            
        rtree_v4, rtree_v6 = create_trees_from_rib(RIB_FILE)
        if rtree_v4 and rtree_v6:
            save_trees_OPTIMIZED(rtree_v4, rtree_v6)
        else:
            print("Failed to create trees from RIB. Exiting.")
            exit(1)

    # Example searches
    print("\nSearching trees:")
    test_ip = "8.8.8.8"  # Changed to Google DNS for a better example
    print(f"\nSearching for {test_ip}:")
    best = rtree_v4.search_best(test_ip)
    if best:
        print(f"Best match: {best.prefix} -> AS Path: {best.data.get('as_path', 'N/A')}")
    else:
        print("No best match found")

    worst = rtree_v4.search_worst(test_ip)
    if worst:
        print(f"Worst match: {worst.prefix} -> AS Path: {worst.data.get('as_path', 'N/A')}")
    else:
        print("No worst match found")

    # Example IPv6 search
    test_ip6 = "2606:4700:4700::1111"  # Changed to Cloudflare DNS for a better example
    print(f"\nSearching for {test_ip6}:")
    best6 = rtree_v6.search_best(test_ip6)
    if best6:
        print(f"Best match: {best6.prefix} -> AS Path: {best6.data.get('as_path', 'N/A')}")
    else:
        print("No IPv6 match found")

    # Start handling live updates
    print("\nStarting live update processing...")
    handle_live_updates(rtree_v4, rtree_v6)