"""
HTML parsing functions for extracting individual fields from SERVIR job postings.

Uses Playwright for reliable extraction and better page state management.
Properly handles UTF-8 encoding to prevent double-encoding issues with Spanish characters.
"""


async def _extract_field_by_class(page, field_name, class_name):
    """
    Helper function to extract a field by searching for a specific class.
    
    Searches for a span with the given class name that contains the field name,
    then returns the text content of the next 'detalle-sp' sibling.
    
    Properly handles UTF-8 encoding for Spanish/special characters.
    
    Args:
        page: Playwright page object
        field_name (str): The field to extract (e.g., "EXPERIENCIA", "REMUNERACIÓN")
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
                                // Extract text and ensure proper UTF-8 handling
                                let text = next.textContent.trim();
                                // Return as-is; encoding already handled by Playwright
                                return text;
                            }}
                            next = next.nextElementSibling;
                        }}
                    }}
                }}
                return null;
            }}
        """)
        
        # Ensure result is properly encoded as UTF-8 string
        if value:
            return str(value).encode('utf-8', errors='replace').decode('utf-8')
        return value
        
    except Exception as e:
        return None


async def extract_simple_field(page, field_name):
    """
    Extract a simple field from the job details section.
    
    Simple fields (salary, dates, vacancies, contract type) use the 'sub-titulo' class.
    
    Args:
        page: Playwright page object
        field_name (str): The field to extract (e.g., "REMUNERACIÓN", "CANTIDAD DE VACANTES")
    
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
    
    Properly handles UTF-8 encoding for Spanish characters (á, é, í, ó, ú, ñ, etc.).
    
    Args:
        page: Playwright page object
    
    Returns:
        str or None: The job title, or None if not found
    """
    try:
        job_title_elem = page.locator('span.sp-aviso0').first
        job_title = await job_title_elem.text_content()
        if job_title:
            # Clean whitespace and ensure UTF-8 encoding
            text = job_title.strip()
            return text.encode('utf-8', errors='replace').decode('utf-8')
        return None
    except Exception:
        return None


async def extract_institution(page):
    """
    Extract the institution name from the detail page.
    
    Properly handles UTF-8 encoding for Spanish characters (á, é, í, ó, ú, ñ, etc.).
    
    Args:
        page: Playwright page object
    
    Returns:
        str or None: The institution name, or None if not found
    """
    try:
        institution_elem = page.locator('span.sp-aviso').first
        institution = await institution_elem.text_content()
        if institution:
            # Clean whitespace and ensure UTF-8 encoding
            text = institution.strip()
            return text.encode('utf-8', errors='replace').decode('utf-8')
        return None
    except Exception:
        return None


async def extract_posting_unique_id(page):
    """
    Extract the posting unique ID (Nº) from the detail page.
    
    Properly handles UTF-8 encoding.
    
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
                    // Check for 'Nº' pattern (UTF-8 safe)
                    let text = el.textContent;
                    if (text.includes('Nº')) {
                        let match = text.match(/\\d+/);
                        return match ? match[0] : null;
                    }
                }
                return null;
            }
        """)
        return posting_id
    except Exception:
        return None