from datetime import datetime, timedelta

def analyze_route_flaps(historical_updates, time_window_minutes=5, min_transitions_for_flap=4, rapid_flap_interval_seconds=60):
    """
    Analyze BGP updates for route flapping patterns with improved detection logic.
    
    Args:
        historical_updates: List of update dictionaries with timestamp, type (A/W), as_path
        time_window_minutes: Time window to analyze for flaps (default: 5 minutes)
        min_transitions_for_flap: Minimum state transitions to consider as flapping (default: 4)
        rapid_flap_interval_seconds: Time threshold for rapid flaps (default: 60 seconds)
    
    Returns:
        Dictionary containing flap analysis results
    """
    if not historical_updates or len(historical_updates) < 3:
        return {
            "flap_detected": False,
            "transition_count": 0,
            "details": [],
            "rapid_transitions": 0,
            "pattern_analysis": "Insufficient updates to analyze for route flaps.",
            "severity": "normal",
            "time_window_minutes": time_window_minutes
        }
    
    # Sort updates by timestamp if not already sorted
    updates = sorted(historical_updates, key=lambda x: x["timestamp"])
    
    # Filter updates to the relevant time window from the latest update
    latest_timestamp = datetime.fromisoformat(updates[-1]["timestamp"])
    window_start = latest_timestamp - timedelta(minutes=time_window_minutes)
    
    relevant_updates = [
        update for update in updates 
        if datetime.fromisoformat(update["timestamp"]) >= window_start
    ]
    
    if len(relevant_updates) < 3:
        return {
            "flap_detected": False,
            "transition_count": 0,
            "details": [],
            "rapid_transitions": 0,
            "pattern_analysis": "Insufficient updates in the analyzed time window.",
            "severity": "normal",
            "time_window_minutes": time_window_minutes
        }
    
    # Count state transitions and track details
    transition_count = 0
    flap_details = []
    last_state = relevant_updates[0]["type"]
    announcement_count = 0
    withdrawal_count = 0
    
    # Track state changes
    for update in relevant_updates:
        if update["type"] == "A":
            announcement_count += 1
        else:
            withdrawal_count += 1
            
        if update["type"] != last_state:
            transition_count += 1
            flap_details.append({
                "timestamp": update["timestamp"],
                "transition": f"{last_state}->{update['type']}",
                "as_path": update.get("as_path", "Unknown")
            })
        last_state = update["type"]
    
    # Check for rapid flaps (transitions happening close together)
    rapid_flap_segments = 0
    for i in range(len(flap_details) - 1):
        time1 = datetime.fromisoformat(flap_details[i]["timestamp"])
        time2 = datetime.fromisoformat(flap_details[i + 1]["timestamp"])
        time_diff = (time2 - time1).total_seconds()
        
        if time_diff <= rapid_flap_interval_seconds:
            rapid_flap_segments += 1
    
    # Determine if flapping is occurring and its severity
    flap_detected = transition_count >= min_transitions_for_flap
    
    # Calculate severity based on transition count and rapid changes
    severity = "normal"
    if flap_detected:
        if transition_count > (min_transitions_for_flap + 4) or rapid_flap_segments > (min_transitions_for_flap // 2):
            severity = "high"
        elif transition_count > (min_transitions_for_flap + 2) or rapid_flap_segments > 0:
            severity = "medium"
        else:
            severity = "low"
    
    # Generate detailed pattern analysis
    pattern_analysis = []
    if not flap_detected:
        pattern_analysis.append("No significant route flapping detected within the analyzed window.")
    else:
        pattern_analysis.append(f"Route instability detected: {transition_count} state transitions observed.")
        
        if rapid_flap_segments > 0:
            pattern_analysis.append(f"{rapid_flap_segments} segments involved rapid changes (within {rapid_flap_interval_seconds}s).")
        
        # Analyze the nature of changes
        total_changes = announcement_count + withdrawal_count
        if total_changes > 0:
            announcement_pct = (announcement_count / total_changes) * 100
            withdrawal_pct = (withdrawal_count / total_changes) * 100
            
            if abs(announcement_pct - withdrawal_pct) < 20:  # Within 20% of each other
                pattern_analysis.append("Balanced mix of announcements and withdrawals.")
            elif announcement_pct > withdrawal_pct:
                pattern_analysis.append(f"Predominantly announcements ({announcement_pct:.1f}% of changes).")
            else:
                pattern_analysis.append(f"Predominantly withdrawals ({withdrawal_pct:.1f}% of changes).")
    
    # Calculate actual time window analyzed
    if len(relevant_updates) >= 2:
        start_time = datetime.fromisoformat(relevant_updates[0]["timestamp"])
        end_time = datetime.fromisoformat(relevant_updates[-1]["timestamp"])
        actual_window_minutes = (end_time - start_time).total_seconds() / 60.0
    else:
        actual_window_minutes = 0
    
    return {
        "flap_detected": flap_detected,
        "transition_count": transition_count,
        "details": flap_details,
        "rapid_transitions": rapid_flap_segments,
        "pattern_analysis": " ".join(pattern_analysis),
        "severity": severity,
        "time_window_minutes": actual_window_minutes,
        "stats": {
            "announcements": announcement_count,
            "withdrawals": withdrawal_count,
            "total_updates": len(relevant_updates)
        }
    }

def analyze_bgp_discrepancies(live_bgp_data: dict, rpki_data: dict, irr_data: dict, historical_updates: list = None) -> dict:
    """
    Analyze discrepancies between live BGP, RPKI, and IRR data.
    
    Args:
        live_bgp_data: Current BGP state data
        rpki_data: RPKI validation results
        irr_data: IRR/whois data
        historical_updates: Optional list of historical updates for flap detection
        
    Returns:
        dict: {
            "flags": list,           # List of issue flags
            "severity": str,         # 'normal', 'low', 'medium', 'high', 'critical'
            "recommendations": list  # List of recommended actions
            "flap_analysis": dict    # Route flap analysis results
        }
    """
    flags = []
    recommendations = []
    severity = "normal"
    flap_analysis = None
    
    # Extract and normalize current origin AS from BGP data (strip AS prefix, keep only digits)
    current_origin = None
    if live_bgp_data and live_bgp_data.get("origin_as"):
        origin_raw = str(live_bgp_data["origin_as"]).replace("AS", "").strip()
        if origin_raw.isdigit():
            current_origin = origin_raw
    
    # Check for route flaps if historical data is provided
    if historical_updates:
        flap_analysis = analyze_route_flaps(historical_updates)
        if flap_analysis["flap_detected"]:
            # Add specific flags based on severity
            if flap_analysis["severity"] == "critical":
                flags.append("SEVERE_ROUTE_FLAPPING")
            elif flap_analysis["severity"] == "high":
                flags.append("FREQUENT_ROUTE_FLAPPING")
            else:
                flags.append("ROUTE_FLAPPING")
            
            # Add detailed recommendations
            recommendations.append(
                f"Route instability detected: {flap_analysis['transition_count']} state transitions\n" +
                f"Pattern: {flap_analysis['pattern_analysis']}"
            )
            
            if flap_analysis["rapid_transitions"] > 0:
                flags.append("RAPID_STATE_CHANGES")
                recommendations.append(
                    f"Found {flap_analysis['rapid_transitions']} rapid transitions " +
                    f"(state transitions within {flap_analysis['time_window_minutes']} minutes)"
                )
            
            # Update severity based on flap analysis
            if flap_analysis["severity"] in ["critical", "high"] or severity == "normal":
                severity = flap_analysis["severity"]
    
    # First check for potential hijack (most critical)
    is_rpki_invalid = rpki_data and rpki_data.get("rpki_status", "").startswith("invalid")
    is_irr_mismatch = irr_data and irr_data.get("irr_origins") and current_origin and current_origin not in irr_data["irr_origins"]
    
    if is_rpki_invalid and is_irr_mismatch:
        flags.append("POTENTIAL_HIJACK")
        recommendations.append("URGENT: Investigate potential route hijacking")
        severity = "critical"
    else:
        # Only check other conditions if not a potential hijack
        if is_rpki_invalid:
            flags.append("RPKI_INVALID")
            recommendations.append(f"Verify route authorization with ROA - RPKI status: {rpki_data.get('rpki_status')}")
            severity = max(severity, "high")
        
        if is_irr_mismatch:
            flags.append("IRR_MISMATCH")
            recommendations.append("Check IRR registration for current origin AS")
            severity = max(severity, "medium")
        
        # Check for multiple IRR origins
        if irr_data and irr_data.get("irr_origins") and len(irr_data["irr_origins"]) > 1:
            flags.append("MULTIPLE_IRR_ORIGINS")
            recommendations.append("Review multiple origin registrations")
            if severity == "normal":
                severity = "low"
        
        # Check RPKI valid but IRR mismatch (conflict)
        if (rpki_data and rpki_data.get("rpki_status") == "valid" and is_irr_mismatch):
            flags.append("RPKI_IRR_CONFLICT")
            recommendations.append("RPKI shows valid but IRR has different origin")
            severity = max(severity, "medium")
    
    result = {
        "flags": flags,
        "severity": severity,
        "recommendations": recommendations,
        "flap_analysis": flap_analysis  # Always include flap analysis if available
    }
    
    return result 