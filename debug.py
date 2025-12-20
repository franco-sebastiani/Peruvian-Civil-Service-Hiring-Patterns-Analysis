import asyncio
from playwright.async_api import async_playwright
from servir.src.extractors.job_assembler import assemble_job_offer


async def test_job_assembler():
    """Test the complete job assembler on a live SERVIR job"""
    
    async with async_playwright() as p:
        browser = await p.firefox.launch()
        page = await browser.new_page()
        
        # Navigate to job listing
        url = "https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/ofertas_laborales.xhtml"
        await page.goto(url, wait_until="networkidle")
        await page.wait_for_timeout(3000)
        
        # Click first job to open detail page
        buttons = await page.locator('button:has-text("Ver más")').all()
        await buttons[0].click()
        await page.wait_for_timeout(2000)
        
        # Assemble the complete job offer
        print("\n" + "=" * 70)
        print("ASSEMBLED JOB OFFER")
        print("=" * 70)
        
        job_data = await assemble_job_offer(page)
        
        # Display all fields
        for key, value in job_data.items():
            if value is not None:
                # Truncate long values for readability
                display_value = str(value)[:60] + "..." if len(str(value)) > 60 else value
                print(f"{key}: {display_value}")
            else:
                print(f"{key}: None")
        
        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        
        total_fields = len(job_data)
        filled_fields = sum(1 for v in job_data.values() if v is not None)
        
        print(f"Total fields: {total_fields}")
        print(f"Filled fields: {filled_fields}")
        print(f"Empty fields: {total_fields - filled_fields}")
        
        print("\n" + "=" * 70)
        
        await browser.close()


if __name__ == "__main__":
    asyncio.run(test_job_assembler())