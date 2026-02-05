"""
CSV Output Validation Script
Validates structure, completeness, and quality of recruiting class scraper output
"""

import csv
import sys
from pathlib import Path
from collections import Counter

def validate_csv(csv_path: Path):
    """Validate CSV structure and data quality"""
    
    print("\n" + "="*80)
    print("📊 CSV VALIDATION REPORT")
    print("="*80)
    print(f"📁 File: {csv_path.name}")
    print(f"📏 Size: {csv_path.stat().st_size / 1024:.1f} KB")
    print("="*80)
    
    # Read CSV
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    if not rows:
        print("❌ ERROR: CSV is empty!")
        return False
    
    print(f"\n✅ Total Players: {len(rows)}")
    
    # Expected headers
    expected_headers = [
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
    
    # Validate headers
    actual_headers = list(rows[0].keys())
    missing_headers = set(expected_headers) - set(actual_headers)
    extra_headers = set(actual_headers) - set(expected_headers)
    
    if missing_headers:
        print(f"\n❌ Missing Headers: {missing_headers}")
        return False
    
    if extra_headers:
        print(f"\n⚠️  Extra Headers: {extra_headers}")
    
    print(f"✅ Headers: All {len(expected_headers)} expected headers present")
    
    # Field completeness analysis
    print("\n" + "="*80)
    print("📈 FIELD COMPLETENESS")
    print("="*80)
    
    field_stats = {}
    
    for field in expected_headers:
        filled = sum(1 for row in rows if row[field] != "NA" and row[field].strip() != "")
        percentage = (filled / len(rows)) * 100
        field_stats[field] = {
            'filled': filled,
            'percentage': percentage
        }
    
    # Group fields by category
    categories = {
        'Core Info': ['247 ID', 'Player Name', 'Position', 'Height', 'Weight', 'High School', 'City, ST', 'Class'],
        '247Sports Ratings': ['247 Stars', '247 Rating', '247 National Rank', '247 Position', '247 Position Rank'],
        'Composite Ratings': ['Composite Stars', 'Composite Rating', 'Composite National Rank', 'Composite Position', 'Composite Position Rank'],
        'Timeline': ['Signed Date', 'Signed Team', 'Draft Date'],
        'Metadata': ['Recruiting Year', 'Profile URL', 'Scrape Date', 'Data Source']
    }
    
    for category, fields in categories.items():
        print(f"\n{category}:")
        for field in fields:
            stats = field_stats[field]
            bar_length = int(stats['percentage'] / 2)  # Scale to 50 chars max
            bar = "█" * bar_length + "░" * (50 - bar_length)
            print(f"  {field:30s} {bar} {stats['percentage']:5.1f}% ({stats['filled']}/{len(rows)})")
    
    # Quality scoring
    print("\n" + "="*80)
    print("⭐ QUALITY SCORE")
    print("="*80)
    
    # Critical fields that should be highly complete
    critical_fields = ['247 ID', 'Player Name', 'Position', 'Class', 'Composite Stars', 'Composite Rating']
    critical_completeness = sum(field_stats[f]['percentage'] for f in critical_fields) / len(critical_fields)
    
    # Optional fields (lower expected completeness)
    optional_fields = ['Composite National Rank', 'Draft Date']
    optional_completeness = sum(field_stats[f]['percentage'] for f in optional_fields) / len(optional_fields)
    
    # Overall score (weighted)
    overall_score = (critical_completeness * 0.7) + (optional_completeness * 0.3)
    
    print(f"Critical Fields Completeness: {critical_completeness:.1f}%")
    print(f"Optional Fields Completeness: {optional_completeness:.1f}%")
    print(f"Overall Quality Score: {overall_score:.1f}%")
    
    # Score interpretation
    if overall_score >= 85:
        print("✅ Excellent quality!")
    elif overall_score >= 70:
        print("⚠️  Good quality, some data missing")
    else:
        print("❌ Poor quality, significant data missing")
    
    # Duplicates check
    print("\n" + "="*80)
    print("🔍 DUPLICATE DETECTION")
    print("="*80)
    
    player_ids = [row['247 ID'] for row in rows if row['247 ID'] != "NA"]
    id_counts = Counter(player_ids)
    duplicates = {pid: count for pid, count in id_counts.items() if count > 1}
    
    if duplicates:
        print(f"⚠️  Found {len(duplicates)} duplicate player IDs:")
        for pid, count in sorted(duplicates.items(), key=lambda x: x[1], reverse=True)[:10]:
            player_name = next((row['Player Name'] for row in rows if row['247 ID'] == pid), "Unknown")
            print(f"  - {player_name} (ID: {pid}): {count} entries")
        
        if len(duplicates) > 10:
            print(f"  ... and {len(duplicates) - 10} more")
    else:
        print("✅ No duplicate player IDs found")
    
    # Year distribution
    print("\n" + "="*80)
    print("📅 YEAR DISTRIBUTION")
    print("="*80)
    
    year_counts = Counter(row['Recruiting Year'] for row in rows)
    for year in sorted(year_counts.keys()):
        print(f"  {year}: {year_counts[year]} players")
    
    # Position distribution
    print("\n" + "="*80)
    print("🏈 POSITION DISTRIBUTION (Top 10)")
    print("="*80)
    
    position_counts = Counter(row['Position'] for row in rows if row['Position'] != "NA")
    for position, count in position_counts.most_common(10):
        print(f"  {position:5s}: {count:4d} players")
    
    # Star rating distribution
    print("\n" + "="*80)
    print("⭐ COMPOSITE STAR DISTRIBUTION")
    print("="*80)
    
    star_counts = Counter(row['Composite Stars'] for row in rows if row['Composite Stars'] != "NA")
    for stars in sorted(star_counts.keys(), reverse=True):
        count = star_counts[stars]
        bar_length = int((count / len(rows)) * 50)
        bar = "⭐" * int(stars) if stars.isdigit() else ""
        print(f"  {stars} stars: {bar:<10s} {count:4d} players ({count/len(rows)*100:5.1f}%)")
    
    print("\n" + "="*80)
    print("✅ VALIDATION COMPLETE")
    print("="*80 + "\n")
    
    return True

def main():
    """Main validation function"""
    
    # Find CSV files in output directory
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
    success = validate_csv(latest_csv)
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
