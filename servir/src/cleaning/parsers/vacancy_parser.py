"""
Vacancy parser for cleaning phase.

Converts raw vacancy count strings into integer values.
"""


def clean_vacancy(vacancy_str):
    """
    Clean raw vacancy string into clean integer value.
    
    Args:
        vacancy_str: Raw vacancy count string from extracting database
                    e.g., "1", "5", "10"
    
    Returns:
        dict: {
            'vacancy_count': int or None,
            'error': str or None
        }
    """
    
    # Handle None or empty input
    if not vacancy_str or not isinstance(vacancy_str, str):
        return {
            'vacancy_count': None,
            'error': 'Vacancy count is empty or invalid type'
        }
    
    try:
        # Clean whitespace
        cleaned = vacancy_str.strip()
        
        # Convert to integer
        vacancy_count = int(cleaned)
        
        return {
            'vacancy_count': vacancy_count,
            'error': None
        }
    
    except (ValueError, AttributeError) as e:
        return {
            'vacancy_count': None,
            'error': f'Failed to parse vacancy count: {str(e)}'
        }