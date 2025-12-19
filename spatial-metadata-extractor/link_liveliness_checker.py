import asyncio
import aiohttp
from datetime import datetime
from typing import Dict, Any, Optional
from urllib.parse import urlparse, parse_qs
from ogc_capabilities import process_ogc_links

# Configuration constants
TIMEOUT = 5
USERAGENT = 'Soilwise Link Liveliness assessment v0.1.0'

class AsyncURLChecker:
    """Async URL checker that handles single URL validation"""
    
    def __init__(self, timeout=TIMEOUT):
        self.timeout = timeout
        self.session = None

    async def __aenter__(self):
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': USERAGENT}
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def check_url(self, url: str, check_ogc_capabilities: bool = True) -> Dict[str, Any]:
        """
        Check a single URL asynchronously with optional OGC capabilities detection
        """
        try:
            # First try HEAD request
            async with self.session.head(url, allow_redirects=True) as response:
                if response.status >= 400:
                    # If HEAD fails, try GET
                    async with self.session.get(url, allow_redirects=True) as get_response:
                        result = await self._process_response(get_response, url)
                else:
                    result = await self._process_response(response, url)
            
            # Check OGC capabilities if requested (even if status code is not 2xx)
            # OGC services may return error codes but still have valid capability info
            if check_ogc_capabilities:
                ogc_capabilities = self._check_ogc_capabilities(url)
                result['gis_capabilities'] = ogc_capabilities
            else:
                result['gis_capabilities'] = None
                
            return result
                    
        except asyncio.TimeoutError:
            return {
                'url': url,
                'error': 'Request timeout',
                'status_code': None,
                'is_redirect': None,
                'valid': False,
                'content_type': None,
                'content_size': None,
                'last_modified': None,
                'final_url': url,
                'gis_capabilities': None
            }
        except Exception as e:
            return {
                'url': url,
                'error': str(e),
                'status_code': None,
                'is_redirect': None,
                'valid': False,
                'content_type': None,
                'content_size': None,
                'last_modified': None,
                'final_url': url,
                'gis_capabilities': None
            }

    async def _process_response(self, response, original_url: str) -> Dict[str, Any]:
        """Process HTTP response and extract relevant information"""
        content_type = response.headers.get('content-type', '').split(';')[0]
        last_modified = response.headers.get('last-modified')
        
        # Get content size from headers
        content_size = None
        if 'content-length' in response.headers:
            content_size = int(response.headers['content-length'])
        elif 'content-range' in response.headers:
            range_header = response.headers['content-range']
            if 'bytes' in range_header and '/' in range_header:
                content_size = int(range_header.split('/')[-1])

        return {
            'url': original_url,
            'status_code': response.status,
            'is_redirect': str(response.url) != original_url,
            'valid': 200 <= response.status < 400,
            'content_type': content_type,
            'content_size': content_size,
            'last_modified': last_modified,
            'final_url': str(response.url)
        }

    def _check_ogc_capabilities(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Check if URL is an OGC service and get its capabilities using the function defined in ogc_services.py
        """
        try:
            # Detect service type from URL
            service_type = self._detect_service_type(url)
            
            if service_type:
                # Use existing process_ogc_links function
                # Pass None for layer name and metadata ID since we're just checking capabilities
                capabilities = process_ogc_links(url, service_type, None, None)
                return capabilities
            
            return None
            
        except Exception as e:
            # If OGC capabilities check fails, don't fail the entire URL check
            print(f"OGC capabilities check failed for {url}: {e}")
            return None

    def _detect_service_type(self, url: str) -> Optional[str]:
        """Detect OGC service type from URL"""
        if not url:
            return None
        
        url_lower = url.lower()
        
        # Check for OGC API patterns in URL
        is_ogcapi_url = any(pattern in url_lower for pattern in [
            '/ogc/features', '/ogcapi', '/api/features'
        ])
        
        if is_ogcapi_url:
            return 'ogcapi'
        
        # Check for service parameter in query string
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        if 'service' in query_params:
            service = query_params['service'][0].lower()
            if service in ['wms', 'wmts', 'wfs', 'wcs']:
                print(f"Detected {service.upper()} service from query parameter in URL: {url}")
                return service
        
        # Check URL path for service indicators
        if '/wms' in url_lower:
            print(f"Detected WMS service in URL: {url}")
            return 'wms'
        elif '/wmts' in url_lower:
            print(f"Detected WMTS service in URL: {url}")
            return 'wmts'
        elif '/wfs' in url_lower:
            print(f"Detected WFS service in URL: {url}")
            return 'wfs'
        elif '/wcs' in url_lower:
            print(f"Detected WCS service in URL: {url}")
            return 'wcs'
        
        # No clear indication found
        return None


def diagnose_link_status(result: Dict[str, Any]) -> str:
    """Provide detailed diagnosis of link issues"""
    if result['valid']:
        return 'Link is ok'
    
    status_code = result.get('status_code')
    error = result.get('error', '')
    
    if status_code:
        if status_code == 404:
            return 'Resource not found (404) - URL may be broken or resource moved'
        elif status_code == 403:
            return 'Access forbidden (403) - May require authentication or IP restrictions'
        elif status_code == 500:
            return 'Server error (500) - Service may be temporarily down'
        elif status_code == 503:
            return 'Service unavailable (503) - Server overloaded or maintenance'
        elif 400 <= status_code < 500:
            return f'Client error ({status_code}) - Check URL format and parameters'
        elif 500 <= status_code < 600:
            return f'Server error ({status_code}) - Remote server issue'
    
    if 'timeout' in error.lower():
        return 'Connection timeout - Server not responding or very slow'
    elif 'connection' in error.lower():
        return 'Connection failed - Server may be down or unreachable'
    elif 'ssl' in error.lower():
        return 'SSL/TLS error - Certificate issue or protocol mismatch'
    elif 'name' in error.lower():
        return 'DNS resolution failed - Domain name not found'
    
    return f'Unknown error: {error}'


# TEST MAIN
async def test_main():
    """Test the URL checker with sample URLs"""
    
    test_urls = [
        # WMS
        "https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi",
        # Shapefile ZIP
        #"https://labs.waterdata.usgs.gov/api/nwis/iv/collections",
        # GeoJSON
        # "https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/wmts.cgi",
    ]
    
    async with AsyncURLChecker(timeout=30) as checker:  # Increased timeout
        for url in test_urls:
            print(f"\n{'='*80}")
            print(f"Testing URL: {url}")
            print('='*80)
            
            result = await checker.check_url(url, check_ogc_capabilities=True)
            
            print(f"✓ Valid: {result['valid']}")
            print(f"✓ Status Code: {result['status_code']}")
            print(f"✓ Content Type: {result['content_type']}")
            print(f"✓ Content Size: {result['content_size']} bytes" if result['content_size'] else "✓ Content Size: Unknown")
            
            if result['gis_capabilities']:
                print(f"\n GIS CAPABILITIES FOUND:")
                caps = result['gis_capabilities']
                print(f"   Service Type: {caps.get('service_type', 'N/A')}")
                print(f"   Layer Name: {caps.get('layer_name', 'N/A')}")
                print(f"   Title: {caps.get('title', 'N/A')}")
                if caps.get('bbox'):
                    print(f" BBox: {caps['bbox']}")
                print(f"   Available Layers: {caps.get('layer_all', [])[:3]}...")  # Show first 3
            else:
                print("\nNo GIS capabilities detected")
                print(f"   (This might be a downloadable file)")
            
            if result.get('error'):
                print(f"\nError: {result['error']}")
            
            diagnosis = diagnose_link_status(result)
            print(f"\nDiagnosis: {diagnosis}")   

if __name__ == "__main__":
    print("Starting URL Checker Test...")
    asyncio.run(test_main())