"""
SERVIR Web Scraper
Collects job postings from SERVIR Peru public sector hiring portal
and stores them in SQLite database.
"""

import sqlite3
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright

# Database connection
DB_PATH = "data/raw/servir_postings.db"

def init_database():
    """Create SQLite database and table if they don't exist"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS postings (
            posting_id TEXT PRIMARY KEY,
            institution TEXT NOT NULL,
            job_title TEXT NOT NULL,
            number_of_vacancies INTEGER,
            experience_requirements TEXT,
            academic_profile TEXT,
            specialization TEXT,
            knowledge TEXT,
            competencies TEXT,
            salary TEXT,
            posting_start_date TEXT,
            posting_end_date TEXT,
            contract_type_raw TEXT,
            scrape_date TEXT NOT NULL
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Database initialized successfully")

async def scrape_page(page, page_number):
    """
    Scrape all posting links from a single list page
    
    Args:
        page: Playwright page object
        page_number: Current page number (for logging)
    
    Returns:
        List of posting URLs
    """
    print(f"Scraping page {page_number}...")
    
    # Get all posting links on the page
    posting_links = await page.locator('a[href*="/DifusionOfertasExterno/faces/consultas/detalle"]').all()
    
    urls = []
    for link in posting_links:
        url = await link.get_attribute('href')
        if url:
            urls.append(url)
    
    print(f"Found {len(urls)} postings on page {page_number}")
    return urls

async def scrape_posting_detail(page, posting_url):
    """
    Scrape all details from a single posting detail page
    
    Args:
        page: Playwright page object
        posting_url: URL of the posting detail page
    
    Returns:
        Dictionary with posting data
    """
    try:
        await page.goto(posting_url, wait_until="networkidle")
        
        # Extract posting ID from URL
        posting_id = posting_url.split('/')[-1] if posting_url else None
        
        # Extract institution (from "Resumen del aviso" section)
        institution = await page.locator('text=INSTITUCIÓN:').locator('..').text_content()
        institution = institution.replace('INSTITUCIÓN:', '').strip() if institution else None
        
        # Extract job title (from "Resumen del aviso" section)
        job_title = await page.locator('text=/^[A-Z0-9-]+ [A-Z]/).first.text_content()')
        
        # Extract salary
        salary = await page.locator('text=REMUNERACIÓN:').locator('..').text_content()
        salary = salary.replace('REMUNERACIÓN:', '').strip() if salary else None
        
        # Extract dates
        posting_start_date = await page.locator('text=FECHA INICIO DE PUBLICACIÓN:').locator('..').text_content()
        posting_start_date = posting_start_date.replace('FECHA INICIO DE PUBLICACIÓN:', '').strip() if posting_start_date else None
        
        posting_end_date = await page.locator('text=FECHA FIN DE PUBLICACIÓN:').locator('..').text_content()
        posting_end_date = posting_end_date.replace('FECHA FIN DE PUBLICACIÓN:', '').strip() if posting_end_date else None
        
        # Extract number of vacancies
        number_of_vacancies = await page.locator('text=CANTIDAD DE VACANTES:').locator('..').text_content()
        number_of_vacancies = number_of_vacancies.replace('CANTIDAD DE VACANTES:', '').strip() if number_of_vacancies else None
        
        # Extract contract type raw
        contract_type_raw = await page.locator('text=NÚMERO DE CONVOCATORIA:').locator('..').text_content()
        contract_type_raw = contract_type_raw.replace('NÚMERO DE CONVOCATORIA:', '').strip() if contract_type_raw else None
        
        # Extract requirements (from "Sobre el aviso" section)
        experience_requirements = await page.locator('text=EXPERIENCIA:').locator('..').text_content()
        experience_requirements = experience_requirements.replace('EXPERIENCIA:', '').strip() if experience_requirements else None
        
        academic_profile = await page.locator('text=FORMACIÓN ACADÉMICA').locator('..').text_content()
        academic_profile = academic_profile.replace('FORMACIÓN ACADÉMICA - PERFIL:', '').strip() if academic_profile else None
        
        specialization = await page.locator('text=ESPECIALIZACIÓN:').locator('..').text_content()
        specialization = specialization.replace('ESPECIALIZACIÓN:', '').strip() if specialization else None
        
        knowledge = await page.locator('text=CONOCIMIENTO:').locator('..').text_content()
        knowledge = knowledge.replace('CONOCIMIENTO:', '').strip() if knowledge else None
        
        competencies = await page.locator('text=COMPETENCIAS:').locator('..').text_content()
        competencies = competencies.replace('COMPETENCIAS:', '').strip() if competencies else None
        
        # Get current scrape timestamp
        scrape_date = datetime.now().isoformat()
        
        # Return data as dictionary
        posting_data = {
            'posting_id': posting_id,
            'institution': institution,
            'job_title': job_title,
            'number_of_vacancies': number_of_vacancies,
            'experience_requirements': experience_requirements,
            'academic_profile': academic_profile,
            'specialization': specialization,
            'knowledge': knowledge,
            'competencies': competencies,
            'salary': salary,
            'posting_start_date': posting_start_date,
            'posting_end_date': posting_end_date,
            'contract_type_raw': contract_type_raw,
            'scrape_date': scrape_date
        }
        
        return posting_data
        
    except Exception as e:
        print(f"Error scraping {posting_url}: {e}")
        return None

def save_posting_to_db(posting_data):
    """
    Save posting data to SQLite database
    
    Args:
        posting_data: Dictionary with posting information
    
    Returns:
        True if successful, False otherwise
    """
    if not posting_data or not posting_data.get('posting_id'):
        return False
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR IGNORE INTO postings 
            (posting_id, institution, job_title, number_of_vacancies, 
             experience_requirements, academic_profile, specialization, 
             knowledge, competencies, salary, posting_start_date, 
             posting_end_date, contract_type_raw, scrape_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            posting_data['posting_id'],
            posting_data['institution'],
            posting_data['job_title'],
            posting_data['number_of_vacancies'],
            posting_data['experience_requirements'],
            posting_data['academic_profile'],
            posting_data['specialization'],
            posting_data['knowledge'],
            posting_data['competencies'],
            posting_data['salary'],
            posting_data['posting_start_date'],
            posting_data['posting_end_date'],
            posting_data['contract_type_raw'],
            posting_data['scrape_date']
        ))
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error saving posting {posting_data.get('posting_id')}: {e}")
        return False

async def main():
    """Main scraper function"""
    print("Starting SERVIR scraper...")
    init_database()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        
        try:
            # Navigate to SERVIR portal
            servir_url = "https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/ofertas_laborales.xhtml"
            await page.goto(servir_url, wait_until="networkidle")
            print("Connected to SERVIR portal")
            
            # Scrape only the first page for testing
            posting_urls = await scrape_page(page, 1)
            
            # Scrape details for each posting found
            for i, url in enumerate(posting_urls, 1):
                print(f"Scraping posting {i}/{len(posting_urls)}...")
                posting_data = await scrape_posting_detail(page, url)
                
                if posting_data:
                    if save_posting_to_db(posting_data):
                        print(f"✓ Saved posting {posting_data['posting_id']}")
                    else:
                        print(f"✗ Failed to save posting {posting_data['posting_id']}")
                
                # Go back to list
                await page.goto(servir_url, wait_until="networkidle")
            
            print("Scraping complete!")
            
        finally:
            await browser.close()
    
if __name__ == "__main__":
    asyncio.run(main())