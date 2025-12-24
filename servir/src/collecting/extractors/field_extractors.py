"""
HTML parsing functions for extracting individual fields from SERVIR job postings.

Uses Playwright for reliable extraction and better page state management.
Includes proper Unicode normalization for Spanish/special characters.
"""


async def _extract_field_by_class(page, field_name, class_name):
    """
    Helper function to extract a field by searching for a specific class.
    
    Searches for a span with the given class name that contains the field name,
    then returns the text content of the next 'detalle-sp' sibling.
    
    Normalizes Unicode characters to prevent encoding issues.
    
    Args:
        page: Playwright page object
        field_name (str): The field to extract (e.g., "EXPERIENCIA", "REMUNERACIÃ“N")
        class_name (str): The CSS class to search in (e.g., "sub-titulo", "sub-titulo-2")
    
    Returns:
        str or None: The extracted value, or None if not found
    """
    try:
        value = await page.evaluate(f"""
            () => {{
                // Find all spans with the specified class
                let labels = document.querySelectorAll('span.{class_name}');
                
                for (let label of labels) {{
                    // Normalize whitespace in label text (handles line breaks)
                    let normalizedLabel = label.textContent.replace(/\s+/g, ' ').trim();
                    
                    // Check if this label contains our target field
                    if (normalizedLabel.includes('{field_name}')) {{
                        // Found the label, get the next detalle-sp sibling
                        let next = label.nextElementSibling;
                        while (next) {{
                            if (next.classList && next.classList.contains('detalle-sp')) {{
                                // Extract text and normalize Unicode characters
                                let text = next.textContent.trim();
                                // Normalize to NFC form (canonical composed form)
                                return text.normalize('NFC');
                            }}
                            next = next.nextElementSibling;
                        }}
                    }}
                }}
                return null;
            }}
        """)
        
        return value
        
    except Exception as e:
        return None


async def extract_simple_field(page, field_name):
    """
    Extract a simple field from the job details section.
    
    Simple fields (salary, dates, vacancies, contract type) use the 'sub-titulo' class.
    
    Args:
        page: Playwright page object
        field_name (str): The field to extract (e.g., "REMUNERACIÃ“N", "CANTIDAD DE VACANTES")
    
    Returns:
        str or None: The extracted value, or None if not found
    """
    return await _extract_field_by_class(page, field_name, "sub-titulo")


async def extract_requirement_field(page, field_name):
    """
    Extract a requirement field from the REQUERIMIENTO section.
    
    Requirement fields (experience, education, specialization, etc.) use the 'sub-titulo-2' class.
    
    Args:
        page: Playwright page object
        field_name (str): The requirement field to extract (e.g., "EXPERIENCIA", "CONOCIMIENTO")
    
    Returns:
        str or None: The extracted value, or None if not found
    """
    return await _extract_field_by_class(page, field_name, "sub-titulo-2")


async def extract_job_title(page):
    """
    Extract the job title from the detail page.
    
    Normalizes Unicode characters to prevent encoding issues.
    
    Args:
        page: Playwright page object
    
    Returns:
        str or None: The job title, or None if not found
    """
    try:
        job_title_elem = page.locator('span.sp-aviso0').first
        job_title = await job_title_elem.text_content()
        if job_title:
            # Normalize Unicode characters and trim whitespace
            return job_title.strip().normalize('NFC')
        return None
    except Exception:
        return None


async def extract_institution(page):
    """
    Extract the institution name from the detail page.
    
    Normalizes Unicode characters to prevent encoding issues.
    
    Args:
        page: Playwright page object
    
    Returns:
        str or None: The institution name, or None if not found
    """
    try:
        institution_elem = page.locator('span.sp-aviso').first
        institution = await institution_elem.text_content()
        if institution:
            # Normalize Unicode characters and trim whitespace
            return institution.strip().normalize('NFC')
        return None
    except Exception:
        return None


async def extract_posting_unique_id(page):
    """
    Extract the posting unique ID (NÂº) from the detail page.
    
    Normalizes Unicode characters to prevent encoding issues.
    
    Args:
        page: Playwright page object
    
    Returns:
        str or None: The posting ID (numeric), or None if not found
    """
    try:
        posting_id = await page.evaluate("""
            () => {
                let elements = document.querySelectorAll('.sub-titulo-2');
                for (let el of elements) {
                    // Normalize text before checking
                    let normalizedText = el.textContent.normalize('NFC');
                    if (normalizedText.includes('NÂº')) {
                        let match = normalizedText.match(/\\d+/);
                        return match ? match[0] : null;
                    }
                }
                return null;
            }
        """)
        return posting_id
    except Exception:
        return None