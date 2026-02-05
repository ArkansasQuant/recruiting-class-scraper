"""
247Sports High School Recruiting Class Scraper
Scrapes recruiting class data from 247Sports composite rankings (2019-2026)
"""

import asyncio
import csv
import os
import re
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# =============================================================================
# CONFIGURATION - EDIT THESE VALUES
# =============================================================================

YEARS = [2019]  # Change to scrape different years: [2019, 2020, 2021, etc.]
OUTPUT_DIR = Path("output")
TEST_MODE = os.getenv('TEST_MODE', 'false').lower() == 'true'
DIAGNOSTICS_MODE = True  # Set to True to save problem HTML files
MAX_CONCURRENT = 4  # Number of parallel player profile scrapes

# =============================================================================
# DATA STRUCTURES
# =============================================================================

CSV_HEADERS = [
    "247 ID",
    "Player Name",
    "Position",
    "Height",
    "Weight",
    "High School",
    "City, ST",
    "Class",
    "247 Stars",
    "247 Rating",
    "247 National Rank",
    "247 Position",
    "247 Position Rank",
    "Composite Stars",
    "Composite Rating",
    "Composite National Rank",
    "Composite Position",
    "Composite Position Rank",
    "Signed Date",
    "Signed Team",
    "Draft Date",
    "Recruiting Year",
    "Profile URL",
    "Scrape Date",
    "Data Source"
]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def extract_player_id(url: str) -> str:
    """Extract player ID from 247Sports URL"""
    match = re.search(r'/player/[^/]+-(\d+)/', url)
    return match.group(1) if match else "NA"

def clean_text(text: str) -> str:
    """Clean and normalize text"""
    if not text:
        return "NA"
    return text.strip().replace('\n', ' ').replace('\r', '')

def parse_rank(text: str) -> str:
    """Extract rank number from text like '#45' or 'National: #45'"""
    if not text:
        return "NA"
    match = re.search(r'#?(\d+)', text)
    return match.group(1) if match else "NA"

# =============================================================================
# LOAD MORE FUNCTIONALITY
# =============================================================================

async def click_load_more_until_complete(page, year: int) -> list:
    """
    Click 'Load More Players' button until all players are loaded
    Returns list of player profile URLs
    """
    print(f"\n📋 Loading all players for {year}...")
    
    url = f"https://247sports.com/season/{year}-football/compositerecruitrankings/"
    await page.goto(url, wait_until='domcontentloaded', timeout=60000)
    await page.wait_for_timeout(3000)
    
    player_urls = []
    click_count = 0
    max_clicks = 500 if not TEST_MODE else 1  # Limit for testing
    
    while click_count < max_clicks:
        # Get current player count
        current_players = await page.locator('li.recruit').count()
        
        # Try to find and click "Load More" button
        load_more_button = page.locator('a.load-more, button.load-more, a:has-text("Load More")')
        
        try:
            # Check if button exists and is visible
            if await load_more_button.count() > 0:
                is_visible = await load_more_button.first.is_visible()
                
                if is_visible:
                    print(f"  → Click #{click_count + 1}: {current_players} players loaded...")
                    await load_more_button.first.click()
                    await page.wait_for_timeout(2000)  # Wait for new players to load
                    click_count += 1
                else:
                    print(f"  ✓ Load More button hidden - all players loaded!")
                    break
            else:
                print(f"  ✓ No Load More button found - all players loaded!")
                break
                
        except Exception as e:
            print(f"  ✓ Load More complete (no more players to load)")
            break
    
    # Extract all player profile URLs
    print(f"\n🔗 Extracting player profile URLs...")
    player_links = await page.locator('li.recruit a.recruit').all()
    
    for link in player_links:
        href = await link.get_attribute('href')
        if href and '/player/' in href:
            # Convert to full URL if relative
            if href.startswith('/'):
                href = f"https://247sports.com{href}"
            player_urls.append(href)
    
    print(f"  ✓ Found {len(player_urls)} player profiles")
    
    if TEST_MODE and len(player_urls) > 50:
        player_urls = player_urls[:50]
        print(f"  ℹ️  TEST MODE: Limited to 50 players")
    
    return player_urls

# =============================================================================
# PROFILE PARSING
# =============================================================================

async def navigate_to_recruiting_profile(page) -> bool:
    """
    Navigate from player profile to recruiting profile
    Returns True if successful, False otherwise
    """
    try:
        # Look for "View recruiting profile" link
        recruiting_link = page.locator('a:has-text("View recruiting profile"), a:has-text("Recruiting Profile")')
        
        if await recruiting_link.count() > 0:
            await recruiting_link.first.click()
            await page.wait_for_load_state('domcontentloaded', timeout=30000)
            await page.wait_for_timeout(1500)
            return True
        else:
            print("    ⚠️  No recruiting profile link found")
            return False
            
    except Exception as e:
        print(f"    ⚠️  Error navigating to recruiting profile: {e}")
        return False

async def parse_profile(page, url: str, year: int) -> dict:
    """
    Parse a single player's recruiting profile
    Returns dictionary with all data fields
    """
    data = {header: "NA" for header in CSV_HEADERS}
    data['Profile URL'] = url
    data['Recruiting Year'] = str(year)
    data['Scrape Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data['Data Source'] = '247Sports Composite'
    
    try:
        # Navigate to main player page first
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        await page.wait_for_timeout(1500)
        
        # Navigate to recruiting profile
        if not await navigate_to_recruiting_profile(page):
            return data
        
        # Get page HTML for parsing
        html = await page.content()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract Player ID from URL
        data['247 ID'] = extract_player_id(url)
        
        # =====================================================================
        # BASIC PLAYER INFO
        # =====================================================================
        
        # Player Name
        name_elem = soup.select_one('h1.name, div.name h1')
        if name_elem:
            data['Player Name'] = clean_text(name_elem.get_text())
        
        # Position
        position_elem = soup.select_one('.position, div.position')
        if position_elem:
            data['Position'] = clean_text(position_elem.get_text())
        
        # Height & Weight
        vitals = soup.select('li.vitals')
        for vital in vitals:
            text = vital.get_text(strip=True)
            if 'Height' in text or "'" in text or '"' in text:
                height_match = re.search(r"(\d+['\"].*?\d+)", text)
                if height_match:
                    data['Height'] = height_match.group(1)
            elif 'Weight' in text or 'lbs' in text.lower():
                weight_match = re.search(r'(\d+)\s*(?:lbs|Pounds)', text, re.IGNORECASE)
                if weight_match:
                    data['Weight'] = weight_match.group(1)
        
        # High School
        hs_elem = soup.select_one('.highschool, .school-name')
        if hs_elem:
            data['High School'] = clean_text(hs_elem.get_text())
        
        # City, ST
        location_elem = soup.select_one('.location, .hometown')
        if location_elem:
            data['City, ST'] = clean_text(location_elem.get_text())
        
        # Class (recruiting class year)
        class_elem = soup.select_one('.class, .recruit-year')
        if class_elem:
            class_text = clean_text(class_elem.get_text())
            # Extract 4-digit year
            year_match = re.search(r'20\d{2}', class_text)
            if year_match:
                data['Class'] = year_match.group(0)
        
        # If class not found, use recruiting year
        if data['Class'] == "NA":
            data['Class'] = str(year)
        
        # =====================================================================
        # RATING SECTIONS: 247Sports & 247Sports Composite
        # =====================================================================
        
        sections = soup.select('section.rankings-section, div.ranking-section')
        
        for section in sections:
            # Get section title to identify which rating system
            title_elem = section.select_one('h3, h2, div.title')
            if not title_elem:
                continue
            
            title = clean_text(title_elem.get_text())
            
            # Determine if this is 247Sports or Composite
            is_247_native = "247Sports" in title and "Composite" not in title
            is_composite = "Composite" in title or "247Sports Composite" in title.lower()
            
            if not is_247_native and not is_composite:
                continue  # Skip sections that aren't rating sections
            
            prefix = "247" if is_247_native else "Composite"
            
            # Extract Stars
            stars = section.select('span.icon-starsolid.yellow, i.icon-starsolid.yellow')
            if stars:
                data[f'{prefix} Stars'] = str(len(stars))
            
            # Extract Rating (composite score)
            rating_elem = section.select_one('.score, .rating, .rank-block')
            if rating_elem:
                rating_text = clean_text(rating_elem.get_text())
                rating_match = re.search(r'(\d+(?:\.\d+)?)', rating_text)
                if rating_match:
                    data[f'{prefix} Rating'] = rating_match.group(1)
            
            # Extract Rankings from list items
            rank_items = section.select('li')
            
            for item in rank_items:
                label_elem = item.select_one('b, strong, .label')
                value_elem = item.select_one('strong, .value, a')
                
                if not label_elem or not value_elem:
                    continue
                
                label = clean_text(label_elem.get_text()).lower()
                value = clean_text(value_elem.get_text())
                
                # Get link URL to distinguish position ranks from state ranks
                link_tag = item.select_one('a')
                link_url = link_tag.get('href', '') if link_tag else ''
                
                # National Rank
                if 'national' in label:
                    data[f'{prefix} National Rank'] = parse_rank(value)
                
                # Position
                elif 'position' in label and ':' in value:
                    # Format is "Position: DE" or "POS: QB"
                    pos_match = re.search(r':\s*([A-Z]+)', value)
                    if pos_match:
                        data[f'{prefix} Position'] = pos_match.group(1)
                
                # Position Rank
                elif 'position' in label and '#' in value:
                    # Verify this is position rank, not state rank
                    if 'Position=' in link_url or 'positionKey=' in link_url or 'State=' not in link_url:
                        data[f'{prefix} Position Rank'] = parse_rank(value)
        
        # =====================================================================
        # TIMELINE DATA
        # =====================================================================
        
        timeline_section = soup.select_one('.timeline, .commitment-timeline, .recruiting-timeline')
        
        if timeline_section:
            timeline_items = timeline_section.select('.timeline-item, li')
            
            for item in timeline_items:
                item_text = clean_text(item.get_text())
                
                # Signed Date & Team
                if 'signed' in item_text.lower() or 'commitment' in item_text.lower():
                    # Extract date (MM/DD/YYYY format)
                    date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', item_text)
                    if date_match and data['Signed Date'] == "NA":
                        data['Signed Date'] = date_match.group(1)
                    
                    # Extract team name
                    # Look for team after "to" or "with"
                    team_match = re.search(r'(?:to|with)\s+([A-Z][^,.]+)', item_text)
                    if team_match and data['Signed Team'] == "NA":
                        data['Signed Team'] = clean_text(team_match.group(1))
                
                # Draft Date
                elif 'draft' in item_text.lower():
                    # Extract date
                    date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', item_text)
                    if date_match:
                        data['Draft Date'] = date_match.group(1)
        
        # Alternative: Look for commitment banner
        if data['Signed Team'] == "NA":
            commit_banner = soup.select_one('.commit-banner, .commitment')
            if commit_banner:
                team_elem = commit_banner.select_one('span, a')
                if team_elem:
                    team_text = clean_text(team_elem.get_text())
                    # Filter out "Commitment" or "Committed" text
                    if team_text.lower() not in ['committed', 'commitment']:
                        data['Signed Team'] = team_text
        
        return data
        
    except Exception as e:
        print(f"    ❌ Error parsing profile: {e}")
        
        if DIAGNOSTICS_MODE:
            # Save problem HTML for debugging
            problem_file = OUTPUT_DIR / "diagnostics" / f"error_{data['247 ID']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            problem_file.parent.mkdir(parents=True, exist_ok=True)
            problem_file.write_text(await page.content(), encoding='utf-8')
            print(f"    💾 Saved diagnostic HTML: {problem_file.name}")
        
        return data

# =============================================================================
# CONCURRENT SCRAPING
# =============================================================================

async def scrape_player_batch(browser, urls: list, year: int, batch_num: int) -> list:
    """Scrape a batch of player profiles concurrently"""
    tasks = []
    
    for i, url in enumerate(urls):
        page = await browser.new_page()
        tasks.append(scrape_player(page, url, year, batch_num * MAX_CONCURRENT + i + 1, len(urls)))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Close all pages
    for task in tasks:
        try:
            await task
        except:
            pass
    
    # Filter out exceptions
    valid_results = []
    for result in results:
        if isinstance(result, dict):
            valid_results.append(result)
        elif isinstance(result, Exception):
            print(f"    ⚠️  Batch error: {result}")
    
    return valid_results

async def scrape_player(page, url: str, year: int, player_num: int, total: int) -> dict:
    """Scrape single player with error handling"""
    try:
        print(f"  [{player_num}/{total}] Scraping: {url}")
        data = await parse_profile(page, url, year)
        
        # Validate required fields
        if data['Player Name'] != "NA":
            print(f"    ✓ {data['Player Name']} - {data['Position']} - {data['Composite Stars']}⭐")
        else:
            print(f"    ⚠️  Warning: Missing player name")
        
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
    """Scrape all players for a given recruiting class year"""
    print(f"\n{'='*80}")
    print(f"🎓 SCRAPING {year} RECRUITING CLASS")
    print(f"{'='*80}")
    
    # Load player list page
    page = await browser.new_page()
    player_urls = await click_load_more_until_complete(page, year)
    await page.close()
    
    if not player_urls:
        print(f"  ❌ No players found for {year}")
        return []
    
    # Scrape profiles in batches
    print(f"\n🔄 Scraping {len(player_urls)} player profiles...")
    all_data = []
    
    for i in range(0, len(player_urls), MAX_CONCURRENT):
        batch = player_urls[i:i + MAX_CONCURRENT]
        batch_num = i // MAX_CONCURRENT
        
        print(f"\n  📦 Batch {batch_num + 1}/{(len(player_urls) + MAX_CONCURRENT - 1) // MAX_CONCURRENT}")
        batch_data = await scrape_player_batch(browser, batch, year, batch_num)
        all_data.extend(batch_data)
        
        # Progress update
        print(f"    → Progress: {len(all_data)}/{len(player_urls)} players")
    
    print(f"\n✅ Completed {year}: {len(all_data)} players scraped")
    return all_data

async def main():
    """Main entry point"""
    print("\n" + "="*80)
    print("🏈 247SPORTS HIGH SCHOOL RECRUITING CLASS SCRAPER")
    print("="*80)
    print(f"📅 Years: {YEARS}")
    print(f"🧪 Test Mode: {TEST_MODE}")
    print(f"🔍 Diagnostics: {DIAGNOSTICS_MODE}")
    print(f"⚡ Concurrency: {MAX_CONCURRENT}")
    print("="*80)
    
    # Create output directories
    OUTPUT_DIR.mkdir(exist_ok=True)
    if DIAGNOSTICS_MODE:
        (OUTPUT_DIR / "diagnostics").mkdir(exist_ok=True)
    
    # Launch browser
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        all_players = []
        
        # Scrape each year
        for year in YEARS:
            year_data = await scrape_year(browser, year)
            all_players.extend(year_data)
        
        await browser.close()
    
    # Save to CSV
    if all_players:
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
        
        # Calculate completeness
        total_fields = len(CSV_HEADERS) * len(all_players)
        filled_fields = sum(1 for player in all_players for field in CSV_HEADERS if player[field] != "NA")
        completeness = (filled_fields / total_fields) * 100
        
        print(f"✨ Data Completeness: {completeness:.1f}%")
        print(f"{'='*80}\n")
        
    else:
        print("\n❌ No data scraped - check logs for errors")

if __name__ == "__main__":
    asyncio.run(main())
