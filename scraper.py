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

YEARS = [2019]  # Change to scrape different years
OUTPUT_DIR = Path("output")
TEST_MODE = os.getenv('TEST_MODE', 'false').lower() == 'true'
DIAGNOSTICS_MODE = True 
MAX_CONCURRENT = 4 

# User-Agent to look like a real browser (Critical for 247Sports)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# =============================================================================
# DATA STRUCTURES
# =============================================================================

CSV_HEADERS = [
    "247 ID", "Player Name", "Position", "Height", "Weight", "High School",
    "City, ST", "Class", "247 Stars", "247 Rating", "247 National Rank",
    "247 Position", "247 Position Rank", "Composite Stars", "Composite Rating",
    "Composite National Rank", "Composite Position", "Composite Position Rank",
    "Signed Date", "Signed Team", "Draft Date", "Draft Team", "Recruiting Year",
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

def normalize_date(date_str: str) -> str:
    """Converts various date formats to MM/DD/YYYY"""
    if not date_str: return "NA"
    date_str = clean_text(date_str)
    
    formats = [
        "%m/%d/%Y",       # 01/29/2017
        "%b %d, %Y",      # Jan 29, 2017
        "%B %d, %Y"       # January 29, 2017
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%m/%d/%Y")
        except ValueError:
            continue
    return date_str

# =============================================================================
# LOAD MORE FUNCTIONALITY
# =============================================================================

async def click_load_more_until_complete(browser, year: int) -> list:
    print(f"\n📋 Loading all players for {year}...")
    
    context = await browser.new_context(user_agent=USER_AGENT)
    page = await context.new_page()
    
    url = f"https://247sports.com/season/{year}-football/compositerecruitrankings/"
    
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=60000)
        await page.wait_for_timeout(3000) 
    except Exception as e:
        print(f"❌ Failed to load initial page for {year}: {e}")
        await context.close()
        return []

    selectors = [
        "li.rankings-page__list-item",      # New 247Sports class
        "li.recruit",                       # Old class
        ".rankings-page__container ul > li" # Generic fallback
    ]
    
    valid_selector = None
    for selector in selectors:
        count = await page.locator(selector).count()
        if count > 0:
            valid_selector = selector
            print(f"  ✓ Found {count} players using selector: '{selector}'")
            break
            
    if not valid_selector:
        print(f"⚠️  No players found with any known selector.")
        if DIAGNOSTICS_MODE:
            diag_dir = OUTPUT_DIR / "diagnostics"
            diag_dir.mkdir(parents=True, exist_ok=True)
            await page.screenshot(path=diag_dir / f"blocked_debug_{year}.png")
        await context.close()
        return []

    click_count = 0
    max_clicks = 500 if not TEST_MODE else 1 
    
    while click_count < max_clicks:
        current_players = await page.locator(valid_selector).count()
        load_more_button = page.locator('a.load-more, button.load-more, a.rankings-page__showmore, a:has-text("Load More")')
        
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
    player_links = await page.locator(f'{valid_selector} a.rankings-page__name-link, {valid_selector} a.recruit').all()
    
    if not player_links:
         player_links = await page.locator(f'{valid_selector} a[href*="/player/"]').all()

    player_urls = []
    for link in player_links:
        href = await link.get_attribute('href')
        if href and '/player/' in href:
            if href.startswith('/'): href = f"https://247sports.com{href}"
            player_urls.append(href)
    
    player_urls = list(dict.fromkeys(player_urls))
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

async def parse_timeline(page, data):
    """
    Parses timeline from either the main page or the dedicated 'See all entries' page.
    """
    try:
        html = await page.content()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        # Determine if we are on the main profile or the full timeline page
        is_full_timeline = soup.select_one('ul.timeline-event-index_lst') is not None
        
        items = []
        if is_full_timeline:
            # Full Timeline Page Structure
            items = soup.select('ul.timeline-event-index_lst li')
        else:
            # Main Profile Page Structure
            # Also checking .vertical-timeline-element-content for Draft info
            items = soup.select('.timeline-item, .timeline li, ul.timeline > li, .vertical-timeline-element-content')

        for item in items:
            item_text = clean_text(item.get_text())
            
            # --- DRAFT LOGIC ---
            if 'draft' in item_text.lower():
                # Date match (Jan 29, 2017 or 01/29/2017)
                date_match = re.search(r'([A-Z][a-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})', item_text)
                if date_match and data['Draft Date'] == "NA":
                     data['Draft Date'] = normalize_date(date_match.group(1))
                
                # Team match
                team_match = re.search(r'^([A-Za-z0-9\s]+?)\s+(?:select|picked|drafted)', item_text, re.IGNORECASE)
                if team_match and data['Draft Team'] == "NA":
                    data['Draft Team'] = clean_text(team_match.group(1))
            
            # --- SIGNED / ENROLLED / COMMITMENT LOGIC ---
            # We prioritize Signed > Enrolled > Commitment
            
            status_priority = {'signed': 3, 'enrolled': 2, 'commit': 1, 'hard commit': 1, 'commitment': 1}
            
            # Determine priority of this specific item
            item_priority = 0
            if 'signed' in item_text.lower(): item_priority = 3
            elif 'enrolled' in item_text.lower(): item_priority = 2
            elif 'commitment' in item_text.lower() or 'hard commit' in item_text.lower(): item_priority = 1
            
            if item_priority > 0:
                # Extract Date
                date_match = re.search(r'([A-Z][a-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})', item_text)
                found_date = normalize_date(date_match.group(1)) if date_match else "NA"
                
                # UPDATE LOGIC:
                # 1. If we have nothing yet ("NA"), take whatever we found.
                # 2. If we found a "Signed" date (Priority 3), ALWAYS take it, even if we had "Enrolled" or "Commitment" before.
                # 3. If we found "Commitment" (Priority 1), ONLY take it if we currently have "NA". Don't overwrite "Signed".
                
                should_update = False
                
                # Get current status from our internal debug tracker
                current_status_str = data.get('_debug_status', 'None')
                current_priority = 0
                if current_status_str == 'Signed': current_priority = 3
                elif current_status_str == 'Enrolled': current_priority = 2
                elif current_status_str == 'Commitment': current_priority = 1
                
                if item_priority > current_priority:
                    should_update = True
                
                if should_update and found_date != "NA":
                    data['Signed Date'] = found_date
                    
                    # Set debug status so we know what quality of date we have
                    if item_priority == 3: data['_debug_status'] = "Signed"
                    elif item_priority == 2: data['_debug_status'] = "Enrolled"
                    elif item_priority == 1: data['_debug_status'] = "Commitment"
                    
                    # Extract Team
                    team_match = re.search(r'(?:to|with|at|commits to)\s+([A-Z][^,.]+)', item_text)
                    if team_match:
                        data['Signed Team'] = clean_text(team_match.group(1))

    except Exception as e:
        pass 

async def parse_profile(page, url: str, year: int) -> dict:
    data = {header: "NA" for header in CSV_HEADERS}
    data['Profile URL'] = url
    data['Recruiting Year'] = str(year)
    data['Scrape Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data['Data Source'] = '247Sports Composite'
    data['_debug_status'] = "None" # Initialize priority tracker
    
    try:
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(1500)
        
        await navigate_to_recruiting_profile(page)
        
        html = await page.content()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        data['247 ID'] = extract_player_id(url)
        
        # --- 1. HEADER INFO ---
        name_elem = soup.select_one('.name') or soup.select_one('h1.name')
        if name_elem: data['Player Name'] = clean_text(name_elem.get_text())
        
        all_header_items = soup.select('.metrics-list li') + soup.select('.details li') + soup.select('ul.vitals li')
        for item in all_header_items:
            text = item.get_text(strip=True)
            if 'Pos' in text or 'Position' in text:
                match = re.search(r'(?:Pos|Position)[:\s]*(.*)', text, re.IGNORECASE)
                if match: data['Position'] = clean_text(match.group(1))
            elif 'Height' in text:
                match = re.search(r'Height[:\s]*(.*)', text, re.IGNORECASE)
                if match: data['Height'] = f"'{clean_text(match.group(1))}"
            elif 'Weight' in text:
                match = re.search(r'Weight[:\s]*(.*)', text, re.IGNORECASE)
                if match: data['Weight'] = clean_text(match.group(1))
            elif 'High School' in text:
                match = re.search(r'High School[:\s]*(.*)', text, re.IGNORECASE)
                if match: data['High School'] = clean_text(match.group(1))
            elif 'Home Town' in text or 'Hometown' in text or 'City' in text:
                match = re.search(r'(?:Home Town|Hometown|City)[:\s]*(.*)', text, re.IGNORECASE)
                if match: data['City, ST'] = clean_text(match.group(1))
            elif 'Class' in text:
                match = re.search(r'Class[:\s]*(.*)', text, re.IGNORECASE)
                if match: data['Class'] = clean_text(match.group(1))
        
        if data['Class'] == "NA": data['Class'] = str(year)
        
        # --- 2. RANKINGS ---
        sections = soup.select('section.rankings-section, div.ranking-section')
        for section in sections:
            title_elem = section.select_one('h3, h2, div.title')
            if not title_elem: continue
            title = clean_text(title_elem.get_text())
            
            prefix = None
            if "Composite" in title or "247Sports Composite" in title:
                prefix = "Composite"
            elif "247Sports" in title and "Composite" not in title:
                prefix = "247"
            if not prefix: continue
            
            stars = section.select('span.icon-starsolid.yellow, i.icon-starsolid.yellow')
            if stars: data[f'{prefix} Stars'] = str(min(len(stars), 5))
            
            rating_elem = section.select_one('.rank-block, .score, .rating')
            if rating_elem:
                rating_text = clean_text(rating_elem.get_text())
                rating_match = re.search(r'(\d+(?:\.\d+)?)', rating_text)
                if rating_match: data[f'{prefix} Rating'] = rating_match.group(1)
            
            rank_items = section.select('li')
            for item in rank_items:
                label_elem = item.select_one('b, strong, .label')
                value_elem = item.select_one('strong, .value, a')
                if not label_elem or not value_elem: continue
                
                label = clean_text(label_elem.get_text()).upper()
                value = clean_text(value_elem.get_text())
                
                link_tag = item.select_one('a')
                link_url = link_tag.get('href', '') if link_tag else ''
                
                if 'NATL' in label or 'NATIONAL' in label:
                    data[f'{prefix} National Rank'] = parse_rank(value)
                elif 'POS' in label or 'POSITION' in label:
                    data[f'{prefix} Position Rank'] = parse_rank(value)
                    if ':' in value:
                         pos_match = re.search(r':\s*([A-Z]+)', value)
                         if pos_match: data[f'{prefix} Position'] = pos_match.group(1)
                    elif data['Position'] != "NA":
                        data[f'{prefix} Position'] = data['Position']

        # --- 3. TIMELINE (Initial Pass on Main Page) ---
        # This will catch "Commitment" or "Signed" if visible immediately
        await parse_timeline(page, data)

        # --- 4. TIMELINE DEEP DIVE (Check for "See all" entries) ---
        # Look for the footer link: <div class="timeline-footer"><a href="...">See all 31 entries</a></div>
        see_all_link = page.locator('.timeline-footer a')
        if await see_all_link.count() > 0:
            href = await see_all_link.first.get_attribute('href')
            if href:
                full_timeline_url = f"https://247sports.com{href}" if href.startswith('/') else href
                try:
                    await page.goto(full_timeline_url, wait_until='domcontentloaded', timeout=15000)
                    await parse_timeline(page, data) # Re-run parsing on the new list page
                except Exception as e:
                    pass

        # Fallback for Signed Team (Commit Banner) if still missing
        if data['Signed Team'] == "NA":
            commit_banner = soup.select_one('.commit-banner, .commitment')
            if commit_banner:
                team_elem = commit_banner.select_one('span, a')
                if team_elem:
                    team_text = clean_text(team_elem.get_text())
                    if team_text.lower() not in ['committed', 'commitment', 'signed']:
                        data['Signed Team'] = team_text
        
        return data
        
    except Exception as e:
        return data

# =============================================================================
# CONCURRENT SCRAPING
# =============================================================================

async def scrape_player_batch(browser, urls: list, year: int, batch_num: int) -> list:
    tasks = []
    context = await browser.new_context(user_agent=USER_AGENT)
    for i, url in enumerate(urls):
        page = await context.new_page()
        tasks.append(scrape_player(page, url, year, batch_num * MAX_CONCURRENT + i + 1, len(urls)))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    await context.close()
    
    valid_results = []
    for result in results:
        if isinstance(result, dict):
            # Clean up internal debug fields before saving
            if '_debug_status' in result: del result['_debug_status']
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
