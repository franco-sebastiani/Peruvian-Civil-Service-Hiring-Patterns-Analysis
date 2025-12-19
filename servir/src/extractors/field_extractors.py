"""
HTML parsing functions for extracting individual fields from SERVIR job postings.

Uses Playwright locators for reliable extraction and better page state management.
"""


async def extract_simple_field(page, label_text):
    """
    Extract fields that use the 'sub-titulo' label pattern.
    
    This pattern is used for basic job posting information like salary,
    vacancies, dates, etc. The function finds a span with class 'sub-titulo'
    containing the label text, then gets the next 'detalle-sp' span.
    
    Structure in HTML:
    <span class="sub-titulo">REMUNERACIÓN: </span>
    <span class="detalle-sp">S/. 6,000.00</span>
    
    Args:
        page: Playwright page object
        label_text (str): The label text to search for (e.g., "REMUNERACIÓN")
    
    Returns:
        str or None: The extracted value, or None if not found
    """
    try:
        # Find the label by searching all sub-titulo spans for the text
        value = await page.evaluate(f"""
            () => {{
                let labels = document.querySelectorAll('span.sub-titulo');
                for (let label of labels) {{
                    if (label.textContent.includes('{label_text}')) {{
                        // Found the label, now get next detalle-sp sibling
                        let next = label.nextElementSibling;
                        while (next) {{
                            if (next.classList && next.classList.contains('detalle-sp')) {{
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


async def extract_requirement_field(page, label_text):
    """
    Extract fields that use the 'sub-titulo-2' label pattern.
    
    This pattern is used for requirement fields like experience, education,
    and specialization. The function finds a 'sub-titulo-2' element containing
    the label text, then gets the next 'detalle-sp' sibling element.
    
    Args:
        page: Playwright page object
        label_text (str): The label text to search for (e.g., "EXPERIENCIA")
    
    Returns:
        str or None: The extracted value, or None if not found
    """
    try:
        # Find all sub-titulo-2 elements
        labels = page.locator('.sub-titulo-2')
        count = await labels.count()
        
        for i in range(count):
            label = labels.nth(i)
            text = await label.text_content()
            
            # Check if this label contains our target text
            if label_text in text:
                # Get the next element that is detalle-sp
                next_elem = await label.evaluate("""
                    el => {
                        let next = el.nextElementSibling;
                        while (next) {
                            if (next.classList.contains('detalle-sp')) {
                                return next.textContent.trim();
                            }
                            next = next.nextElementSibling;
                        }
                        return null;
                    }
                """)
                
                if next_elem:
                    return next_elem
        
        return None
        
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
        job_title_elem = page.locator('span.sp-aviso0').first
        job_title = await job_title_elem.text_content()
        special_data["job_title"] = job_title.strip() if job_title else None
    except Exception:
        special_data["job_title"] = None
    
    # Extract institution name
    try:
        institution_elem = page.locator('span.sp-aviso').first
        institution = await institution_elem.text_content()
        special_data["institution"] = institution.strip() if institution else None
    except Exception:
        special_data["institution"] = None
    
    # Extract posting unique ID (the N° field)
    try:
        posting_id = await page.evaluate("""
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
        special_data["posting_unique_id"] = posting_id
    except Exception:
        special_data["posting_unique_id"] = None
    
    return special_data