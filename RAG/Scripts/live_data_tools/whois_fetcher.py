import requests

def fetch_whois_data(prefix: str) -> dict:
    """
    Query the RIPEstat API to get whois/IRR data for a given prefix.

    Args:
        prefix (str): IP prefix to check (e.g., "192.0.2.0/24")

    Returns:
        dict: {
            "prefix": str,
            "irr_origins": list,       # List of origin ASNs from IRR route objects (numbers only)
            "authorities": list,       # List of IRR authorities (RIPE, ARIN, etc.)
            "status": str             # 'success', 'not-found', or 'error'
        }
    """
    url = f"https://stat.ripe.net/data/whois/data.json?resource={prefix}"

    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        
        irr_origins = []
        authorities = []
        
        # Parse IRR route objects first (primary source)
        if "data" in data and "irr_records" in data["data"]:
            for record in data["data"]["irr_records"]:
                for item in record:
                    if item.get("key") == "origin":
                        # Normalize ASN - strip AS prefix and keep only digits
                        origin_as = item.get("value", "").replace("AS", "").strip()
                        if origin_as.isdigit() and origin_as not in irr_origins:
                            irr_origins.append(origin_as)
                    elif item.get("key") == "source":
                        source = item.get("value", "").strip()
                        if source and source not in authorities:
                            authorities.append(source)
        
        # Fallback: parse RIR records if no IRR route objects found
        if not irr_origins and "data" in data and "records" in data["data"]:
            for record in data["data"]["records"]:
                for item in record:
                    if item.get("key") in ["OriginAS", "origin"]:
                        # Normalize ASN - strip AS prefix and keep only digits
                        origin_as = item.get("value", "").replace("AS", "").strip()
                        if origin_as.isdigit() and origin_as not in irr_origins:
                            irr_origins.append(origin_as)
                    elif item.get("key") == "source":
                        source = item.get("value", "").strip()
                        if source and source not in authorities:
                            authorities.append(source)

        return {
            "prefix": prefix,
            "irr_origins": irr_origins,
            "authorities": authorities,
            "status": "success" if irr_origins else "not-found"
        }

    except Exception as e:
        return {
            "prefix": prefix,
            "irr_origins": [],
            "authorities": [],
            "status": "error",
            "error": str(e)
        } 