"""
Contract classification mappings for transforming phase.

Maps cleaned contract types to their regime and temporal nature classifications.
"""

# Mapping of cleaned contract types to classification components
# Each entry maps: cleaned_contract_type -> {regime, temporal_nature}
#
# Temporal nature categories:
# - TEMPORARY: Fixed-term contracts for transitory needs
# - REPLACEMENT: Fixed-term contracts for substitute/replacement positions
# - INDETERMINATE: Indefinite-term contracts under D.LEG 1057
# - PERMANENT: Permanent career positions (D.LEG 276, 728, LEY 30057, LEY 30220)

CONTRACT_CLASSIFICATIONS = {
    "D.LEG 1057 DETERMINADO NECESIDAD TRANSITORIA": {
        "regime": "D.LEG 1057",
        "temporal_nature": "TEMPORARY"
    },
    "D.LEG 1057 DETERMINADO SUPLENCIA": {
        "regime": "D.LEG 1057",
        "temporal_nature": "REPLACEMENT"
    },
    "D.LEG 1057 INDETERMINADO": {
        "regime": "D.LEG 1057",
        "temporal_nature": "INDETERMINATE"
    },
    "D.LEG 728": {
        "regime": "D.LEG 728",
        "temporal_nature": "PERMANENT"
    },
    "D.LEG 276": {
        "regime": "D.LEG 276",
        "temporal_nature": "PERMANENT"
    },
    "DOCENTES UNIVERSITARIOS LEY 30220": {
        "regime": "LEY 30220",
        "temporal_nature": "PERMANENT"
    },
    "LEY 30057": {
        "regime": "LEY 30057",
        "temporal_nature": "PERMANENT"
    }
}