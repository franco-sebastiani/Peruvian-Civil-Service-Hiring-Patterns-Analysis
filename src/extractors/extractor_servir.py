import asyncio
import sys
from pathlib import Path

# Add project root to Python path so imports work
sys.path.insert(0, str(Path(__file__).parent.parent))

from playwright.async_api import async_playwright
from config.servir_fields import SIMPLE_FIELDS, REQUIREMENT_FIELDS, SPECIAL_FIELDS, FIELD_ORDER

async def extract_simple_field(page, label_text):
    """
    Find span with sub-titulo label, then get the detalle-sp value.
    Used for basic fields like salary, vacancies, dates, etc.
    
    Returns:
        str or None: Extracted value, or None if not found
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
    Find sub-titulo-2 label, then get the next detalle-sp sibling.
    Used for requirement fields (experience, education, specialization, etc.)
    
    Returns:
        str or None: Extracted value, or None if not found
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

async def extract_job_posting(page):
    """
    Extract all job posting data from the detail page.
    Returns a dictionary with all field values (None if not found).
    """
    data = {}
    
    # Extract simple fields (sub-titulo labels)
    for field_name, label_text in SIMPLE_FIELDS.items():
        value = await extract_simple_field(page, label_text)
        data[field_name] = value
    
    # Extract requirement fields (sub-titulo-2 labels)
    for field_name, label_text in REQUIREMENT_FIELDS.items():
        value = await extract_requirement_field(page, label_text)
        data[field_name] = value
    
    # Extract special fields (custom logic)
    job_title = await page.locator('span.sp-aviso0').text_content()
    data["job_title"] = job_title.strip() if job_title else None
    
    institution = await page.locator('span.sp-aviso').text_content()
    data["institution"] = institution.strip() if institution else None
    
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
    data["posting_unique_id"] = posting_unique_id
    
    return data

async def print_job_data(data):
    """Pretty print the extracted job data in the specified order."""
    print("\n" + "=" * 60)
    print("EXTRACTED JOB POSTING DATA")
    print("=" * 60)
    
    for field_name in FIELD_ORDER:
        value = data.get(field_name)
        
        if value is None:
            display_value = "[NOT FOUND]"
        elif isinstance(value, str) and len(value) > 100:
            display_value = value[:100] + "..."
        else:
            display_value = value
        
        print(f"{field_name:.<35} {display_value}")
    
    print("=" * 60 + "\n")

async def debug(job_offer_index=0):
    """
    Debug function to test extraction on different job offers.
    
    Args:
        job_offer_index: Which job offer to extract (0 = 1st, 1 = 2nd, 2 = 3rd, etc.)
    """
    async with async_playwright() as p:
        browser = await p.firefox.launch()
        page = await browser.new_page()
        
        try:
            servir_url = "https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/ofertas_laborales.xhtml"
            await page.goto(servir_url, wait_until="networkidle")
            await page.wait_for_timeout(3000)
            print(f"✓ List page loaded")
            
            # Find all "Ver más" buttons
            ver_mas_buttons = await page.locator('button:has-text("Ver más")').all()
            print(f"✓ Found {len(ver_mas_buttons)} job offers available\n")
            
            # Check that the requested index exists
            if job_offer_index >= len(ver_mas_buttons):
                print(f"✗ Error: Only {len(ver_mas_buttons)} job offers found. Index {job_offer_index} is out of range.")
                await browser.close()
                return
            
            print(f"→ Clicking job offer #{job_offer_index + 1}...")
            await ver_mas_buttons[job_offer_index].click()
            await page.wait_for_timeout(3000)
            print(f"✓ Detail page loaded")
            
            # Extract all data
            job_data = await extract_job_posting(page)
            
            # Display results
            await print_job_data(job_data)
            
            print("✓ Extraction completed successfully!")
            
        except Exception as e:
            print(f"\n✗ Error: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            await browser.close()

if __name__ == "__main__":
    # Test on different job offers by changing the index
    asyncio.run(debug(job_offer_index=0))
    
    # Uncomment to test other job offers:
    # asyncio.run(debug(job_offer_index=1))
    # asyncio.run(debug(job_offer_index=2))