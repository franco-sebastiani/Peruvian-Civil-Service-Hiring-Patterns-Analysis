"""
Contract type mappings for processing phase.

Maps raw contract type strings from SERVIR into standardized categories.
Uses regex patterns to handle institution-specific suffixes.
"""

# Mapping of regex patterns to standardized contract types
# Each tuple: (regex_pattern, standardized_name)
# Patterns are matched in order, first match wins
CONTRACT_TYPE_PATTERNS = [
    # D.LEG 1057 contracts
    (r"^D\.LEG 1057 - DETERMINADO \(NECESIDAD TRANSITORIA\).*", "D.LEG 1057 DETERMINADO NECESIDAD TRANSITORIA"),
    (r"^D\.LEG 1057 - DETERMINADO \(SUPLENCIA\).*", "D.LEG 1057 DETERMINADO SUPLENCIA"),
    (r"^D\.LEG 1057 - INDETERMINADO.*", "D.LEG 1057 INDETERMINADO"),
    
    # D.LEG codes (728, 276, etc.)
    (r"^728.*", "D.LEG 728"),
    (r"^276.*", "D.LEG 276"),
    
    # LEY 30220 (University teachers)
    (r"^DOCENTES UNIVERSITARIOS.*", "DOCENTES UNIVERSITARIOS LEY 30220"),
    (r"^LEY 30220.*", "DOCENTES UNIVERSITARIOS LEY 30220"),
    
    # LEY 30057
    (r"^LEY 30057.*", "LEY 30057"),
]