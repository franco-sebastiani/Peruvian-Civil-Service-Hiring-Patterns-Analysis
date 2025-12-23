"""
Text cleaning configuration for processing phase.

Defines rules and patterns for cleaning text fields.
"""

# Text cleaning rules
TEXT_CLEANING_RULES = {
    'trim': True,                    # Remove leading/trailing whitespace
    'remove_extra_spaces': True,     # Replace multiple spaces with single space
    'fix_encoding': True,            # Handle encoding issues
    'target_case': None              # None = keep original case
}