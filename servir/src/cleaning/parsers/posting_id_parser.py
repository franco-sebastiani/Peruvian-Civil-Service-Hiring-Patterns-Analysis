def parse_posting_id(raw_value: str) -> int:
    """
    Extract posting unique ID from raw text and convert to integer.
    
    Args:
        raw_value: Raw posting ID (e.g., "738119", "738213B")
    
    Returns:
        Integer posting ID
    
    Raises:
        ValueError: If value cannot be converted to integer
    """
    if raw_value is None or raw_value.strip() == "":
        raise ValueError("Posting ID cannot be None or empty")
    
    # Strip whitespace
    cleaned = raw_value.strip()
    
    # Remove any non-digit characters (in case there are trailing letters like "B")
    numeric_only = ''.join(c for c in cleaned if c.isdigit())
    
    if not numeric_only:
        raise ValueError(f"No digits found in posting ID: {raw_value}")
    
    return int(numeric_only)