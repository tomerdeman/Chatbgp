import requests

def fetch_rpki_validation(prefix: str, origin_asn: str) -> dict:
    """
    Query the RIPEstat API to check RPKI validity for a given prefix and origin ASN.

    Args:
        prefix (str): IP prefix to check (e.g., "192.0.2.0/24")
        origin_asn (str): Origin ASN claiming the prefix (e.g., "AS65000")

    Returns:
        dict: {
            "prefix": str,
            "origin_asn": str,
            "rpki_status": str,        # 'valid', 'invalid', 'not-found', or 'error'
            "is_hijack": bool,
            "roa_details": str         # e.g., 'AS64496:valid, AS65000:invalid'
        }
    """
    url = f"https://stat.ripe.net/data/rpki-validation/data.json?resource={origin_asn}&prefix={prefix}"

    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        rpki_data = data.get("data", {})
        status = rpki_data.get("status", "unknown").lower()
        roas = rpki_data.get("validating_roas", [])

        roa_details = []
        is_hijack = False

        for roa in roas:
            asn = roa.get("origin", "unknown")
            validity = roa.get("validity", "unknown")
            roa_details.append(f"{asn}:{validity}")

            if asn == origin_asn and validity != "valid":
                is_hijack = True
            elif status != "valid":
                is_hijack = True

        return {
            "prefix": prefix,
            "origin_asn": origin_asn,
            "rpki_status": status,
            "is_hijack": is_hijack,
            "roa_details": ", ".join(roa_details) if roa_details else "No ROAs found"
        }

    except Exception as e:
        return {
            "prefix": prefix,
            "origin_asn": origin_asn,
            "rpki_status": "error",
            "is_hijack": False,
            "roa_details": f"API error: {str(e)}"
        }
