"""
HTML parsing functions for extracting individual fields from SERVIR job postings.
"""

async def extract_simple_field(page, label_text):
    """
    Extract fields that use the 'sub-titulo' label pattern.
    
    This pattern is used for basic job posting information like salary,
    number of vacancies, publication dates, etc. The function finds a label
    containing the specified text, then locates the corresponding value in
    a 'detalle-sp' element nearby.
    
    Args:
        page: Playwright page object
        label_text (str): The label text to search for (e.g., "Remuneración mensual")
    
    Returns:
        str or None: The extracted value, or None if not found
    """
    try:
        value = await page.evaluate(f"""
            () => {{
                let el = document.evaluate(
                    "//*[contains(text(), '{label_text}')]",
                    document,
                    null,
                    XPathResult.FIRST_ORDERED_NODE_TYPE,
                    null
                ).singleNodeValue;
                
                if (!el) return null;
                
                // Look for detalle-sp in the same row or nearby
                let container = el.closest('.row');
                let detalle = container?.querySelector('.detalle-sp');
                
                if (detalle) return detalle.textContent.trim();
                
                // Try parent's next sibling
                let nextRow = container?.nextElementSibling;
                detalle = nextRow?.querySelector('.detalle-sp');
                if (detalle) return detalle.textContent.trim();
                
                return null;
            }}
        """)
        return value
        
    except Exception as e:
        return None


async def extract_requirement_field(page, label_text):
    """
    Extract fields that use the 'sub-titulo-2' label pattern.
    
    This pattern is used for requirement fields like experience, education,
    and specialization. The function finds a 'sub-titulo-2' element containing
    the label text, then gets the next 'detalle-sp' sibling element.
    
    Args:
        page: Playwright page object
        label_text (str): The label text to search for (e.g., "Experiencia")
    
    Returns:
        str or None: The extracted value, or None if not found
    """
    try:
        value = await page.evaluate(f"""
            () => {{
                // Find all elements with the label text
                let elements = document.querySelectorAll('.sub-titulo-2');
                
                for (let el of elements) {{
                    if (el.textContent.includes('{label_text}')) {{
                        // Look for next sibling that is detalle-sp
                        let next = el.nextElementSibling;
                        while (next) {{
                            if (next.classList.contains('detalle-sp')) {{
                                return next.textContent.trim();
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


async def extract_special_fields(page):
    """
    Extract fields that require custom logic and don't follow standard patterns.
    
    These are fields that have unique HTML structures:
    - Job title: uses 'sp-aviso0' class
    - Institution: uses 'sp-aviso' class
    - Posting unique ID: extracted from text containing 'N°'
    
    Args:
        page: Playwright page object
    
    Returns:
        dict: Dictionary with keys 'job_title', 'institution', 'posting_unique_id'
              Values are strings or None if not found
    """
    special_data = {}
    
    # Extract job title
    try:
        job_title = await page.locator('span.sp-aviso0').text_content()
        special_data["job_title"] = job_title.strip() if job_title else None
    except Exception:
        special_data["job_title"] = None
    
    # Extract institution name
    try:
        institution = await page.locator('span.sp-aviso').text_content()
        special_data["institution"] = institution.strip() if institution else None
    except Exception:
        special_data["institution"] = None
    
    # Extract posting unique ID (the N° field)
    try:
        posting_unique_id = await page.evaluate("""
            () => {
                let elements = document.querySelectorAll('.sub-titulo-2');
                for (let el of elements) {
                    if (el.textContent.includes('N°')) {
                        let match = el.textContent.match(/\\d+/);
                        return match ? match[0] : null;
                    }
                }
                return null;
            }
        """)
        special_data["posting_unique_id"] = posting_unique_id
    except Exception:
        special_data["posting_unique_id"] = None
    
    return special_data