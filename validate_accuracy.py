"""
Accuracy Validation Script
Re-scrapes a random sample of players to verify accuracy of CSV data
"""

import asyncio
import csv
import random
import sys
from pathlib import Path
from playwright.async_api import async_playwright

# Import parse_profile function from scraper
import importlib.util
spec = importlib.util.spec_from_file_location("scraper", "scraper.py")
scraper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scraper)

async def validate_accuracy(csv_path: Path, sample_size: int = 20):
    """
    Validate accuracy by re-scraping random sample
    
    Args:
        csv_path: Path to CSV file to validate
        sample_size: Number of players to re-scrape
    """
    
    print("\n" + "="*80)
    print("🔍 ACCURACY VALIDATION")
    print("="*80)
    print(f"📁 File: {csv_path.name}")
    print(f"🎲 Sample Size: {sample_size} players")
    print("="*80)
    
    # Read CSV
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    if len(rows) < sample_size:
        sample_size = len(rows)
        print(f"⚠️  CSV has only {len(rows)} rows, sampling all")
    
    # Random sample
    sample = random.sample(rows, sample_size)
    
    print(f"\n🔄 Re-scraping {sample_size} players to verify accuracy...\n")
    
    # Launch browser
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        mismatches = []
        perfect_matches = 0
        
        for i, original_data in enumerate(sample):
            print(f"[{i+1}/{sample_size}] Verifying: {original_data['Player Name']} ({original_data['247 ID']})")
            
            # Re-scrape this player
            page = await browser.new_page()
            url = original_data['Profile URL']
            year = int(original_data['Recruiting Year'])
            
            fresh_data = await scraper.parse_profile(page, url, year)
            await page.close()
            
            # Compare key fields
            fields_to_check = [
                'Player Name',
                'Position',
                'Height',
                'Weight',
                'Class',
                '247 Stars',
                '247 Rating',
                'Composite Stars',
                'Composite Rating',
                'Signed Team'
            ]
            
            player_mismatches = []
            
            for field in fields_to_check:
                original_value = original_data.get(field, "NA")
                fresh_value = fresh_data.get(field, "NA")
                
                # Normalize for comparison
                orig_normalized = str(original_value).strip().upper()
                fresh_normalized = str(fresh_value).strip().upper()
                
                if orig_normalized != fresh_normalized:
                    player_mismatches.append({
                        'field': field,
                        'original': original_value,
                        'fresh': fresh_value
                    })
            
            if player_mismatches:
                print(f"  ⚠️  Found {len(player_mismatches)} mismatches:")
                for mismatch in player_mismatches:
                    print(f"      {mismatch['field']:20s}: '{mismatch['original']}' → '{mismatch['fresh']}'")
                
                mismatches.append({
                    'player': original_data['Player Name'],
                    'id': original_data['247 ID'],
                    'url': url,
                    'mismatches': player_mismatches
                })
            else:
                print(f"  ✅ Perfect match")
                perfect_matches += 1
        
        await browser.close()
    
    # Summary report
    print("\n" + "="*80)
    print("📊 ACCURACY SUMMARY")
    print("="*80)
    
    accuracy_rate = (perfect_matches / sample_size) * 100
    
    print(f"✅ Perfect Matches: {perfect_matches}/{sample_size} ({accuracy_rate:.1f}%)")
    print(f"⚠️  Players with Mismatches: {len(mismatches)}/{sample_size}")
    
    if mismatches:
        print("\n🔍 MISMATCH DETAILS:")
        
        # Count mismatches by field
        field_mismatch_counts = {}
        for player in mismatches:
            for mismatch in player['mismatches']:
                field = mismatch['field']
                field_mismatch_counts[field] = field_mismatch_counts.get(field, 0) + 1
        
        print("\nMismatches by Field:")
        for field, count in sorted(field_mismatch_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {field:20s}: {count} occurrences")
        
        print("\nAffected Players:")
        for i, player in enumerate(mismatches, 1):
            print(f"\n{i}. {player['player']} (ID: {player['id']})")
            print(f"   URL: {player['url']}")
            for mismatch in player['mismatches']:
                print(f"   - {mismatch['field']:20s}: '{mismatch['original']}' → '{mismatch['fresh']}'")
    
    # Interpretation
    print("\n" + "="*80)
    print("💡 INTERPRETATION")
    print("="*80)
    
    if accuracy_rate == 100:
        print("✅ Perfect accuracy! All sampled data matches live profiles.")
    elif accuracy_rate >= 95:
        print("✅ Excellent accuracy. Minor mismatches may be due to:")
        print("   - Recent updates on 247Sports")
        print("   - Formatting differences (e.g., 'QB' vs 'Quarterback')")
    elif accuracy_rate >= 90:
        print("⚠️  Good accuracy, but review mismatches.")
        print("   Common causes:")
        print("   - 247Sports updated player data")
        print("   - HTML structure changes")
    else:
        print("❌ Low accuracy detected. Possible issues:")
        print("   - Scraper may need updates")
        print("   - HTML structure may have changed")
        print("   - Check logs for parsing errors")
    
    print("\n" + "="*80 + "\n")
    
    return accuracy_rate >= 90

async def main():
    """Main validation function"""
    
    # Find CSV files
    output_dir = Path("output")
    
    if not output_dir.exists():
        print("❌ Error: 'output' directory not found")
        sys.exit(1)
    
    csv_files = list(output_dir.glob("recruiting_class_*.csv"))
    
    if not csv_files:
        print("❌ Error: No CSV files found in output directory")
        sys.exit(1)
    
    # Get most recent CSV
    latest_csv = max(csv_files, key=lambda p: p.stat().st_mtime)
    
    # Run validation
    sample_size = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    success = await validate_accuracy(latest_csv, sample_size)
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
