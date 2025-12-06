import asyncio
from playwright.async_api import async_playwright

async def test_scrape():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        try:
            servir_url = "https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/ofertas_laborales.xhtml"
            await page.goto(servir_url, wait_until="networkidle", timeout=60000)
            print("Page loaded successfully")
            
            # Try to find posting links
            links = await page.locator('a[href*="/DifusionOfertasExterno/faces/consultas/detalle"]').all()
            print(f"Found {len(links)} posting links")
            
            if len(links) > 0:
                first_url = await links[0].get_attribute('href')
                print(f"First posting URL: {first_url}")
        
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_scrape())