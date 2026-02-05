"""
247Sports High School Recruiting Class Scraper
Scrapes recruiting class data from 247Sports composite rankings (2019-2026)
"""

import asyncio
import csv
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# =============================================================================
# CONFIGURATION
# =============================================================================

YEARS = [2019]
OUTPUT_DIR = Path("output")
TEST_MODE = os.getenv('TEST_MODE', 'false').lower() == 'true'
DIAGNOSTICS_MODE = True 
MAX_CONCURRENT = 4 

# This makes the bot look like a real Chrome browser on Windows
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# =============================================================================
# DATA STRUCTURES
# =============================================================================

CSV_HEADERS = [
    "247 ID", "Player Name", "Position", "Height", "Weight", "High School",
    "City, ST", "Class", "247 Stars", "247 Rating", "247 National Rank",
    "247 Position", "247 Position Rank", "Composite Stars", "Composite Rating",
    "Composite National Rank", "Composite Position", "Composite Position Rank",
    "Signed Date", "Signed Team", "Draft Date", "Recruiting Year",
    "Profile URL", "Scrape Date", "Data Source"
]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def extract_player_id(url: str) -> str:
    match = re.search(r'/player/[^/]+-(\d+)/', url)
    return match.group(1) if match else "NA"

def clean_text(text: str) -> str:
    if not text: return "NA"
    return text.strip().replace('\n', ' ').replace('\r', '')

def parse_rank(text: str) -> str:
    if not text: return "NA"
    match = re.search(r'#?(\d+)', text)
    return match.group(1) if match else "NA"

# =============================================================================
# LOAD MORE FUNCTIONALITY
# =============================================================================

async def click_load_more_until_complete(browser, year: int) -> list:
    print(f"\n📋 Loading all players for {year}...")
    
    # Create context with User-Agent to bypass simple bot detection
    context = await browser.new_context(user_agent=USER_AGENT)
    page = await context.new_page()
    
    url = f"https://247sports.com/season/{year}-football/compositerecruitrankings/"
    
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=60000)
        await page.wait_for_timeout(5000) # Give extra time for any redirects/checks
    except Exception as e:
        print(f"❌ Failed to load initial page for {year}: {e}")
        await context.close()
        return []

    # --- DEBUG: CHECK FOR BLOCKING ---
    initial_count = await page.locator('li.recruit').count()
    if initial_count == 0:
        print(f"⚠️  No players found on page for {year}.")
        print("    Taking screenshot to check for bot blocking...")
        
        # Save screenshot to diagnostics
        diag_dir = OUTPUT_DIR / "diagnostics"
        diag_dir.mkdir(parents=True, exist_ok=True)
        screenshot_path = diag_dir / f"blocked_debug_{year}.png"
        await page.screenshot(path=screenshot_path)
        print(f"    📸 Screenshot saved to: {screenshot_path}")
        
        # Try waiting a bit longer just in case
        await page.wait_for_timeout(5000)
        if await page.locator('li.recruit').count() == 0:
            await context.close()
            return []

    player_urls = []
    click_count = 0
    max_clicks = 500 if not TEST_MODE else 1 
    
    while click_count < max_clicks:
        current_players = await page.locator('li.recruit').count()
        load_more_button = page.locator('a.load-more, button.load-more, a:has-text("Load More")')
        
        try:
            if await load_more_button.count() > 0 and await load_more_button.first.is_visible():
                print(f"  → Click #{click_count + 1}: {current_players} players loaded...")
                await load_more_button.first.click()
                await page.wait_for_timeout(2000)
                click_count += 1
            else:
                print(f"  ✓ Load More button hidden - all players loaded!")
                break
        except Exception:
            print(f"  ✓ Load More complete (no more players to load)")
            break
    
    print(f"\n🔗 Extracting player profile URLs...")
    player_links = await page.locator('li.recruit a.recruit').all()
    
    for link in player_links:
        href = await link.get_attribute('href')
        if href and '/player/' in href:
            if href.startswith('/'): href = f"https://247sports.com{href}"
            player_urls.append(href)
    
    player_urls = list(dict.fromkeys(player_urls)) # Remove dupes
    print(f"  ✓ Found {len(player_urls)} player profiles")
    
    if TEST_MODE and len(player_urls) > 50:
        player_urls = player_urls[:50]
        print(f"  ℹ️  TEST MODE: Limited to 50 players")
    
    await context.close()
    return player_urls

# =============================================================================
# PROFILE PARSING
# =============================================================================

async def navigate_to_recruiting_profile(page) -> bool:
    try:
        recruiting_link = page.locator('a:has-text("View recruiting profile"), a:has-text("Recruiting Profile")')
        if await recruiting_link.count() > 0:
            await recruiting_link.first.click()
            await page.wait_for_load_state('domcontentloaded', timeout=30000)
            await page.wait_for_timeout(1500)
            return True
        return False
    except:
        return False

async def parse_profile(page, url: str, year: int) -> dict:
    data = {header: "NA" for header in CSV_HEADERS}
    data['Profile URL'] = url
    data['Recruiting Year'] = str(year)
    data['Scrape Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data['Data Source'] = '247Sports Composite'
    
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(1500)
        
        await navigate_to_recruiting_profile(page)
        
        html = await page.content()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        data['247 ID'] = extract_player_id(url)
        
        # --- Basic Info ---
        name_elem = soup.select_one('h1.name, div.name h1')
        if name_elem: data['Player Name'] = clean_text(name_elem.get_text())
        
        position_elem = soup.select_one('.position, div.position')
        if position_elem: data['Position'] = clean_text(position_elem.get_text())
        
        vitals = soup.select('li.vitals')
        for vital in vitals:
            text = vital.get_text(strip=True)
            if 'Height' in text or "'" in text:
                height_match = re.search(r"(\d+['\"].*?\d+)", text)
                if height_match: data['Height'] = height_match.group(1)
            elif 'Weight' in text or 'lbs' in text.lower():
                weight_match = re.search(r'(\d+)\s*(?:lbs|Pounds)', text, re.IGNORECASE)
                if weight_match: data['Weight'] = weight_match.group(1)
        
        hs_elem = soup.select_one('.highschool, .school-name')
        if hs_elem: data['High School'] = clean_text(hs_elem.get_text())
        
        location_elem = soup.select_one('.location, .hometown')
        if location_elem: data['City, ST'] = clean_text(location_elem.get_text())
        
        class_elem = soup.select_one('.class, .recruit-year')
        if class_elem:
            class_text = clean_text(class_elem.get_text())
            year_match = re.search(r'20\d{2}', class_text)
            if year_match: data['Class'] = year_match.group(0)
        if data['Class'] == "NA": data['Class'] = str(year)
        
        # --- Ratings ---
        sections = soup.select('section.rankings-section, div.ranking-section')
        for section in sections:
            title_elem = section.select_one('h3, h2, div.title')
            if not title_elem: continue
            title = clean_text(title_elem.get_text())
            
            is_247_native = "247Sports" in title and "Composite" not in title
            is_composite = "Composite" in title or "247Sports Composite" in title.lower()
            if not is_247_native and not is_composite: continue
            
            prefix = "247" if is_247_native else "Composite"
            stars = section.select('span.icon-starsolid.yellow, i.icon-starsolid.yellow')
            if stars: data[f'{prefix} Stars'] = str(len(stars))
            
            rating_elem = section.select_one('.score, .rating, .rank-block')
            if rating_elem:
                rating_text = clean_text(rating_elem.get_text())
                rating_match = re.search(r'(\d+(?:\.\d+)?)', rating_text)
                if rating_match: data[f'{prefix} Rating'] = rating_match.group(1)
            
            rank_items = section.select('li')
            for item in rank_items:
                label_elem = item.select_one('b, strong, .label')
                value_elem = item.select_one('strong, .value, a')
                if not label_elem or not value_elem: continue
                
                label = clean_text(label_elem.get_text()).lower()
                value = clean_text(value_elem.get_text())
                link_tag = item.select_one('a')
                link_url = link_tag.get('href', '') if link_tag else ''
                
                if 'national' in label:
                    data[f'{prefix} National Rank'] = parse_rank(value)
                elif 'position' in label and ':' in value:
                    pos_match = re.search(r':\s*([A-Z]+)', value)
                    if pos_match: data[f'{prefix} Position'] = pos_match.group(1)
                elif 'position' in label and '#' in value:
                    if 'Position=' in link_url or 'positionKey=' in link_url or 'State=' not in link_url:
                        data[f'{prefix} Position Rank'] = parse_rank(value)

        # --- Timeline ---
        timeline_section = soup.select_one('.timeline, .commitment-timeline, .recruiting-timeline')
        if timeline_section:
            timeline_items = timeline_section.select('.timeline-item, li')
            for item in timeline_items:
                item_text = clean_text(item.get_text())
                if 'signed' in item_text.lower() or 'commitment' in item_text.lower():
                    date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', item_text)
                    if date_match and data['Signed Date'] == "NA": data['Signed Date'] = date_match.group(1)
                    team_match = re.search(r'(?:to|with)\s+([A-Z][^,.]+)', item_text)
                    if team_match and data['Signed Team'] == "NA": data['Signed Team'] = clean_text(team_match.group(1))
                elif 'draft' in item_text.lower():
                    date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', item_text)
                    if date_match: data['Draft Date'] = date_match.group(1)
        
        if data['Signed Team'] == "NA":
            commit_banner = soup.select_one('.commit-banner, .commitment')
            if commit_banner:
                team_elem = commit_banner.select_one('span, a')
                if team_elem:
                    team_text = clean_text(team_elem.get_text())
                    if team_text.lower() not in ['committed', 'commitment']:
                        data['Signed Team'] = team_text
        
        return data
        
    except Exception as e:
        # print(f"    ❌ Error parsing profile: {e}")
        return data

# =============================================================================
# CONCURRENT SCRAPING
# =============================================================================

async def scrape_player_batch(browser, urls: list, year: int, batch_num: int) -> list:
    tasks = []
    # Create a fresh context with User Agent for this batch
    context = await browser.new_context(user_agent=USER_AGENT)
    
    for i, url in enumerate(urls):
        page = await context.new_page()
        tasks.append(scrape_player(page, url, year, batch_num * MAX_CONCURRENT + i + 1, len(urls)))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    await context.close() # Close context to free resources
    
    valid_results = []
    for result in results:
        if isinstance(result, dict):
            valid_results.append(result)
    return valid_results

async def scrape_player(page, url: str, year: int, player_num: int, total: int) -> dict:
    try:
        print(f"  [{player_num}/{total}] Scraping: {url}")
        data = await parse_profile(page, url, year)
        if data['Player Name'] != "NA":
            print(f"    ✓ {data['Player Name']} - {data['Position']} - {data['Composite Stars']}⭐")
        else:
            print(f"    ⚠️  Warning: Missing player name (Blocked or Empty)")
        return data
    except Exception as e:
        print(f"    ❌ Error: {e}")
        return {header: "NA" for header in CSV_HEADERS}
    finally:
        await page.close()

# =============================================================================
# MAIN SCRAPER
# =============================================================================

async def scrape_year(browser, year: int) -> list:
    print(f"\n{'='*80}")
    print(f"🎓 SCRAPING {year} RECRUITING CLASS")
    print(f"{'='*80}")
    
    # Pass browser so it can create its own context
    player_urls = await click_load_more_until_complete(browser, year)
    
    if not player_urls:
        print(f"  ❌ No players found for {year}")
        return []
    
    print(f"\n🔄 Scraping {len(player_urls)} player profiles...")
    all_data = []
    
    for i in range(0, len(player_urls), MAX_CONCURRENT):
        batch = player_urls[i:i + MAX_CONCURRENT]
        batch_num = i // MAX_CONCURRENT
        print(f"\n  📦 Batch {batch_num + 1}/{(len(player_urls) + MAX_CONCURRENT - 1) // MAX_CONCURRENT}")
        batch_data = await scrape_player_batch(browser, batch, year, batch_num)
        all_data.extend(batch_data)
        print(f"    → Progress: {len(all_data)}/{len(player_urls)} players")
    
    print(f"\n✅ Completed {year}: {len(all_data)} players scraped")
    return all_data

async def main():
    print("\n" + "="*80)
    print("🏈 247SPORTS HIGH SCHOOL RECRUITING CLASS SCRAPER")
    print("="*80)
    print(f"📅 Years: {YEARS}")
    print(f"🧪 Test Mode: {TEST_MODE}")
    print(f"🔍 Diagnostics: {DIAGNOSTICS_MODE}")
    print(f"⚡ Concurrency: {MAX_CONCURRENT}")
    print("="*80)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if DIAGNOSTICS_MODE:
        (OUTPUT_DIR / "diagnostics").mkdir(parents=True, exist_ok=True)
    
    async with async_playwright() as p:
        # Launch browser without context, contexts are created per task
        browser = await p.chromium.launch(headless=True)
        all_players = []
        for year in YEARS:
            year_data = await scrape_year(browser, year)
            all_players.extend(year_data)
        await browser.close()
    
    if not all_players:
        print("\n❌ CRITICAL: No data scraped from any year.")
        print("   Exiting with error code 1 to notify GitHub Actions.")
        sys.exit(1)

    year_range = f"{min(YEARS)}-{max(YEARS)}" if len(YEARS) > 1 else str(YEARS[0])
    timestamp = datetime.now().strftime('%Y%m%d')
    filename = OUTPUT_DIR / f"recruiting_class_{year_range}_{timestamp}.csv"
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(all_players)
    
    print(f"\n{'='*80}")
    print(f"✅ SCRAPING COMPLETE!")
    print(f"{'='*80}")
    print(f"📊 Total Players: {len(all_players)}")
    print(f"📁 Output File: {filename}")
    print(f"📏 File Size: {filename.stat().st_size / 1024:.1f} KB")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"\n❌ FATAL SCRIPT ERROR: {e}")
        sys.exit(1)
