"""
HTML parsing functions for extracting individual fields from SERVIR job postings.

Uses Playwright locators instead of page.evaluate() for more reliable extraction
and better page state management.
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
        # Find the label element containing the text
        label = page.locator(f"text='{label_text}'").first
        
        # Get the closest .row container
        container = await label.evaluate('el => el.closest(".row")')
        
        if not container:
            return None
        
        # Look for detalle-sp within this container
        detalle = page.locator('.detalle-sp').first
        value = await detalle.text_content()
        
        return value.strip() if value else None
        
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
        # Find all sub-titulo-2 elements
        labels = page.locator('.sub-titulo-2')
        count = await labels.count()
        
        for i in range(count):
            label = labels.nth(i)
            text = await label.text_content()
            
            # Check if this label contains our target text
            if label_text in text:
                # Get the next element that is detalle-sp
                # Navigate to parent, find next detalle-sp sibling
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