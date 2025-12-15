import asyncio
from playwright.async_api import async_playwright

async def debug():
    async with async_playwright() as p:
        browser = await p.firefox.launch()
        page = await browser.new_page()
        
        try:
            servir_url = "https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/ofertas_laborales.xhtml"
            await page.goto(servir_url, wait_until="networkidle")
            await page.wait_for_timeout(3000)
            print("✓ List page loaded\n")
            
            # Find first "Ver más" button
            ver_mas_buttons = await page.locator('button:has-text("Ver más")').all()
            print(f"Found {len(ver_mas_buttons)} 'Ver más' buttons")
            
            if ver_mas_buttons:
                print("Clicking first 'Ver más' button...")
                await ver_mas_buttons[0].click()
                await page.wait_for_timeout(3000)
                print("✓ Detail page loaded\n")
                
                # Extract data using proper selectors
                print("=== Extracting job posting data ===\n")
                
                # Job title
                job_title = await page.locator('span.sp-aviso0').text_content()
                job_title = job_title.strip() if job_title else None
                print(f"job_title: {job_title}")
                
                # Institution
                institution = await page.locator('span.sp-aviso').text_content()
                institution = institution.strip() if institution else None
                print(f"institution: {institution}")
                
                # Helper function to extract simple fields (with sub-titulo labels)
                async def extract_simple_field(label_text):
                    """Find span with sub-titulo label, then get the detalle-sp value"""
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
                
                # Helper function to extract requirement fields (inside <li> tags)
                async def extract_requirement_field(label_text):
                    """Find sub-titulo-2 label, then get the next detalle-sp sibling"""
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
                
                # Extract other fields using the sub-titulo pattern
                number_of_vacancies = await extract_simple_field("CANTIDAD DE VACANTES:")
                print(f"number_of_vacancies: {number_of_vacancies}")
                
                salary = await extract_simple_field("REMUNERACIÓN:")
                print(f"salary: {salary}")
                
                posting_start_date = await extract_simple_field("FECHA INICIO DE")
                print(f"posting_start_date: {posting_start_date}")
                
                posting_end_date = await extract_simple_field("FECHA FIN DE PUBLICACIÓN:")
                print(f"posting_end_date: {posting_end_date}")
                
                contract_type_raw = await extract_simple_field("NÚMERO DE CONVOCATORIA:")
                print(f"contract_type_raw: {contract_type_raw}")
                
                # Extract requirement fields (from <li> tags)
                experience_requirements = await extract_requirement_field("EXPERIENCIA:")
                print(f"experience_requirements: {experience_requirements[:100] if experience_requirements else None}...")
                
                academic_profile = await extract_requirement_field("FORMACIÓN")
                print(f"academic_profile: {academic_profile[:100] if academic_profile else None}...")
                
                specialization = await extract_requirement_field("ESPECIALIZACIÓN:")
                print(f"specialization: {specialization[:100] if specialization else None}...")
                
                knowledge = await extract_requirement_field("CONOCIMIENTO:")
                print(f"knowledge: {knowledge[:100] if knowledge else None}...")
                
                competencies = await extract_requirement_field("COMPETENCIAS:")
                print(f"competencies: {competencies[:100] if competencies else None}...")
                
                # Extract posting unique ID (from the right-side summary section)
                posting_unique_id = await page.evaluate("""
                    () => {
                        // Look for the span in the cuadro-seccion-lat (right sidebar section)
                        let sidebar = document.querySelector('.cuadro-seccion-lat');
                        if (sidebar) {
                            let span = sidebar.querySelector('span');
                            if (span) return span.textContent.trim();
                        }
                        return null;
                    }
                """)
                print(f"posting_unique_id: {posting_unique_id.replace(chr(10), '').replace(chr(13), '').strip() if posting_unique_id else None}")
                
                print("\n✓ All fields extracted successfully!")
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug())