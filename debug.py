"""
Debug utilities for testing SERVIR scraper.

These functions are for development/testing only.
"""

import asyncio
from playwright.async_api import async_playwright
from servir.config.field_definitions import FIELD_ORDER


async def test_all_pages_pagination():
    """
    Test pagination across ALL pages to verify:
    - Can navigate to all 264 pages
    - Can count jobs on each page
    - Handles last page (may have fewer than 10 jobs)
    - Reports progress and anomalies
    """
    async with async_playwright() as p:
        browser = await p.firefox.launch()
        page = await browser.new_page()
        
        try:
            servir_url = "https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/ofertas_laborales.xhtml"
            
            print("\n" + "="*60)
            print("TEST: Full Pagination (All 264 Pages)")
            print("="*60)
            print(f"\nNavigating to: {servir_url}")
            
            await page.goto(servir_url, wait_until="networkidle")
            await page.wait_for_timeout(3000)
            print("✓ Page loaded")
            
            # Get total pages first
            page_indicator = await page.locator('text=/Página \\d+ de \\d+/').first.text_content(timeout=5000)
            import re
            match = re.search(r'Página (\d+) de (\d+)', page_indicator)
            total_pages = int(match.group(2))
            
            print(f"\nTotal pages to process: {total_pages}")
            print("="*60)
            
            # Track statistics
            total_jobs = 0
            jobs_per_page = []
            anomalies = []  # Pages with unusual job counts
            
            # Process each page
            for current_page_num in range(1, total_pages + 1):
                try:
                    # Get jobs on this page
                    ver_mas_buttons = await page.locator('button:has-text("Ver más")').all()
                    jobs_count = len(ver_mas_buttons)
                    jobs_per_page.append(jobs_count)
                    total_jobs += jobs_count
                    
                    # Get pagination info
                    page_indicator = await page.locator('text=/Página \\d+ de \\d+/').first.text_content(timeout=5000)
                    
                    # Report progress every 10 pages, or anomalies
                    if jobs_count != 10 and current_page_num != total_pages:
                        # Anomaly: not 10 jobs on non-last page
                        anomalies.append(f"Page {current_page_num}: {jobs_count} jobs (expected 10)")
                        print(f"⚠ Page {current_page_num}: {jobs_count} jobs (unusual!)")
                    
                    if current_page_num % 10 == 0:
                        print(f"✓ Pages 1-{current_page_num}: {total_jobs} total jobs")
                    
                    # Check if we're on the last page
                    if current_page_num == total_pages:
                        print(f"\n✓ Reached last page!")
                        print(f"  Last page ({current_page_num}): {jobs_count} jobs")
                        break  # Stop here - we've reached the end
                    
                    # Navigate to next page
                    if current_page_num < total_pages:
                        next_button = page.locator('button.btn-paginator:has-text("Sig.")').first
                        is_disabled = await next_button.evaluate('el => el.getAttribute("aria-disabled")')
                        
                        if is_disabled == "false":
                            await next_button.click()
                            await page.wait_for_timeout(2000)  # Wait for page to load
                        else:
                            print(f"✗ Next button disabled at page {current_page_num}")
                            break
                    
                except Exception as e:
                    print(f"\n✗ Error at page {current_page_num}: {e}")
                    break
            
            # Print final report
            print("\n" + "="*60)
            print("TEST REPORT")
            print("="*60)
            
            print(f"\nPages processed: {len(jobs_per_page)}")
            print(f"Total jobs collected: {total_jobs}")
            
            # Statistics
            import statistics
            avg_jobs = statistics.mean(jobs_per_page)
            min_jobs = min(jobs_per_page)
            max_jobs = max(jobs_per_page)
            
            print(f"\nJobs per page statistics:")
            print(f"  Average: {avg_jobs:.1f}")
            print(f"  Min: {min_jobs}")
            print(f"  Max: {max_jobs}")
            print(f"  Standard deviation: {statistics.stdev(jobs_per_page):.2f}")
            
            # Jobs breakdown
            page_with_10 = sum(1 for j in jobs_per_page if j == 10)
            page_with_less = sum(1 for j in jobs_per_page if j < 10)
            page_with_more = sum(1 for j in jobs_per_page if j > 10)
            
            print(f"\nPages breakdown:")
            print(f"  Pages with 10 jobs: {page_with_10}")
            print(f"  Pages with < 10 jobs: {page_with_less}")
            print(f"  Pages with > 10 jobs: {page_with_more}")
            
            # Anomalies
            if anomalies:
                print(f"\nAnomalies found ({len(anomalies)}):")
                for anomaly in anomalies[:10]:  # Show first 10
                    print(f"  - {anomaly}")
                if len(anomalies) > 10:
                    print(f"  ... and {len(anomalies) - 10} more")
            else:
                print(f"\n✓ No anomalies detected!")
            
            print("\n" + "="*60)
            print("✓ Full pagination test completed successfully!")
            print("="*60)
            
        except Exception as e:
            print(f"\n✗ Critical error during test: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(test_all_pages_pagination())