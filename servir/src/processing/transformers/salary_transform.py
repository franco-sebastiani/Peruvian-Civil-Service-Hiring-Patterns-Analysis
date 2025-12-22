"""
Salary transformer for processing phase.

Converts raw salary strings from SERVIR into clean numeric values.
"""


def transform_salary(salary_str):
    """
    Transform raw salary string into clean numeric value.
    
    Handles SERVIR format: "S/. 6,000.00" or "S/. 6000.00"
    
    Args:
        salary_str: Raw salary string from SERVIR collection database
    
    Returns:
        dict: {
            'salary_amount': float or None,
            'error': str or None
        }
    """
    
    # Handle None or empty input
    if not salary_str or not isinstance(salary_str, str):
        return {
            'salary_amount': None,
            'error': 'Salary is empty or invalid type'
        }
    
    try:
        # Remove "S/. " prefix and trim whitespace
        cleaned = salary_str.replace("S/. ", "").strip()
        
        # Remove comma separators (thousands)
        cleaned = cleaned.replace(",", "")
        
        # Convert to float
        salary_amount = float(cleaned)
        
        return {
            'salary_amount': salary_amount,
            'error': None
        }
    
    except (ValueError, AttributeError) as e:
        return {
            'salary_amount': None,
            'error': f'Failed to parse salary string: {str(e)}'
        }