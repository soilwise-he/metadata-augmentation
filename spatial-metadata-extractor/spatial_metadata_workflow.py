"""
Integrated spatial metadata extraction with link validation
Uses link checker results to filter and enrich processing
"""
import csv
import json
import asyncio
from pathlib import Path
from datetime import datetime
import argparse
from link_liveliness_checker import AsyncURLChecker
from gdal_metadata import SpatialMetadataExtractor

csv.field_size_limit(int(1e8))

SPATIAL_MEDIATYPES = {
    'application/x-shapefile',
    'application/geopackage+sqlite3',
    'application/vnd.google-earth.kml+xml',
    'application/geo+json',
    'application/gml+xml',
    'image/tiff',
    'image/geotiff',
    'application/x-netcdf',
    'application/netcdf',
    'application/zip',
    'application/x-zip-compressed',
}

async def check_url_validity(url: str) -> dict:
    """Check if URL is valid using link checker"""
    async with AsyncURLChecker(timeout=10) as checker:
        return await checker.check_url(url, check_ogc_capabilities=True)

def extract_urls_from_links(links_field: str) -> list[str]:
    """Extract URL list from links field"""
    if not links_field:
        return []
    
    try:
        links = json.loads(links_field)
    except:
        return []
    
    if not isinstance(links, list):
        links = [links]
    
    urls = []
    for link in links:
        if isinstance(link, dict):
            url = link.get('url') or link.get('href')
        else:
            url = str(link)
        if url:
            urls.append(url)
    
    return urls

def create_output_record(identifier: str, url: str, 
                         metadata: dict = None, error: str = None) -> dict:
    """Create standardized output record"""
    return {
        'identifier': identifier,
        'url': url,
        'metadata': metadata,
        'date': datetime.utcnow().isoformat(),
        'process': 'spatial-extractor',
        'error': error
    }

def write_output(output_f, record: dict):
    """Write record to output file if file handle exists"""
    if output_f:
        output_f.write(json.dumps(record, default=str) + '\n')
        output_f.flush()

def process_csv_with_link_check(csv_path, output_file=None, limit=None, check_links=True):
    """
    Process CSV with link validation first
    
    Output format:
    {
        "identifier": "uuid",
        "url": "https://...",
        "metadata": {...},
        "date": "2025-12-18T...",
        "process": "spatial-extractor",
        "error": null
    }
    """
    extractor = SpatialMetadataExtractor(max_features=100)
    stats = {'processed': 0, 'valid_urls': 0, 'invalid_urls': 0, 'skipped': 0, 'errors': 0}
    results = []
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        output_f = open(output_file, 'w') if output_file else None
        
        try:
            for row_idx, row in enumerate(reader, 1):
                if limit and (stats['processed'] + stats['errors']) >= limit:
                    break
                
                identifier = row.get('identifier', 'unknown')
                
                # Parse and validate links
                links_str = row.get('links', '')               
                urls = extract_urls_from_links(links_str)
                
                if not urls:
                    print(f"[{row_idx}] SKIP {identifier}: No valid URLs in links")
                    stats['skipped'] += 1
                    continue
                
                # Skip if already has geometry
                wkt = row.get('wkt_geometry')
                if wkt and wkt.strip() != 'NULL':
                    print(f"[{row_idx}] SKIP {identifier}: Already has wkt_geometry")
                    stats['skipped'] += 1
                    continue
                
                # Process each URL
                for url_idx, url in enumerate(urls):
                    if limit and (stats['processed'] + stats['errors']) >= limit:
                        break
                    
                    print(f"\n[{row_idx}.{url_idx}] PROCESS {identifier}")
                    print(f"URL: {url[:80]}...")
                    
                    # Validate URL with link checker
                    mediatype = None
                    if check_links:
                        try:
                            link_result = asyncio.run(check_url_validity(url))
                            
                            if not link_result.get('valid'):
                                print(f"    ✗ INVALID - Status: {link_result.get('status_code')}, Error: {link_result.get('error')}")
                                stats['invalid_urls'] += 1
                                error_record = create_output_record(
                                    identifier, url,
                                    error=f"Invalid URL: {link_result.get('error')} (Status: {link_result.get('status_code')})"
                                )
                                write_output(output_f, error_record)
                                results.append(error_record)
                                continue
                            
                            stats['valid_urls'] += 1
                            mediatype = link_result.get('content_type')
                            gis_capabilities = link_result.get('gis_capabilities')
                            print(f"Valid - ContentType: {mediatype}")
                            
                            # If OGC service, use capabilities directly
                            if gis_capabilities:
                                metadata = gis_capabilities.copy()  
                                metadata.update({
                                    'title': row.get('title'),
                                    'keywords': row.get('keywords'),
                                    'source_geometry': row.get('wkt_geometry'),
                                    'format': row.get('format')
                                })
                                output_record = create_output_record(identifier, url, metadata)
                                write_output(output_f, output_record)
                                results.append(output_record)
                                stats['processed'] += 1
                                print(f"SUCCESS (OGC)")
                                continue
                            
                            # Check if spatial mediatype
                            is_spatial = mediatype and mediatype.split(';')[0] in SPATIAL_MEDIATYPES
                            if not is_spatial:
                                print(f"Not spatial mediatype, skipping GDAL processing")
                                stats['skipped'] += 1
                                continue
                        except Exception as e:
                            print(f"Link check error: {e}")
                            stats['invalid_urls'] += 1
                            continue
                    
                    # Extract spatial metadata with GDAL
                    try:
                        result = extractor.process_url(url, mediatype)
                        
                        if result['success']:
                            metadata = result['metadata']
                            metadata.update({
                                'title': row.get('title'),
                                'keywords': row.get('keywords'),
                                'source_geometry': row.get('wkt_geometry'),
                                'format': row.get('format')
                            })
                            
                            output_record = create_output_record(identifier, url, metadata)
                            
                            print(f"SUCCESS - Type: {metadata.get('type')}, Driver: {metadata.get('driver')}")
                            if metadata.get('type') == 'vector':
                                print(f"Layers: {metadata.get('layer_count')}, Features: {len(metadata.get('features', []))}")
                            
                            results.append(output_record)
                            stats['processed'] += 1
                        else:
                            output_record = create_output_record(
                                identifier, url,
                                error=result.get('error', 'Unknown error')
                            )
                            print(f"GDAL ERROR: {result.get('error', '')[:80]}")
                            results.append(output_record)
                            stats['errors'] += 1
                        
                        write_output(output_f, output_record)
                    
                    except Exception as e:
                        print(f"sEXCEPTION: {e}")
                        error_record = create_output_record(identifier, url, error=str(e))
                        results.append(error_record)
                        stats['errors'] += 1
                        write_output(output_f, error_record)
        
        finally:
            if output_f:
                output_f.close()
    
    # Print summary
    print(f"\n{'='*70}")
    print(f"SUMMARY:")
    print(f"Total rows: {row_idx}")
    print(f"Successfully processed: {stats['processed']}")
    print(f"GDAL errors: {stats['errors']}")
    print(f"Valid URLs: {stats['valid_urls']}")
    print(f"Invalid URLs: {stats['invalid_urls']}")
    print(f"Skipped: {stats['skipped']}")
    print(f"{'='*70}")
    
    return results

if __name__ == "__main__":    
    parser = argparse.ArgumentParser(description='Process spatial metadata with link validation')
    parser.add_argument('csv_file', help='CSV file path')
    parser.add_argument('--output', help='Output JSONL file', default='spatial_metadata_augmented.jsonl')
    parser.add_argument('--limit', type=int, help='Limit records to process')
    parser.add_argument('--no-link-check', action='store_true', help='Skip link validation')
    
    args = parser.parse_args()
    
    if not Path(args.csv_file).exists():
        print(f"Error: {args.csv_file} not found")
        exit(1)
    
    process_csv_with_link_check(args.csv_file, args.output, args.limit, check_links=not args.no_link_check)