from owslib.wms import WebMapService
from owslib.wmts import WebMapTileService          
from owslib.wfs import WebFeatureService
from owslib.wcs import WebCoverageService
from owslib.ogcapi.features import Features
     
def process_ogc_links(url, ltype, lname, md_id):
    def extract_metadata_urls(urls):
        """Helper to extract metadata URLs"""
        if not urls:
            return []
        metadata_urls = []
        for mu in urls:
            if isinstance(mu, dict) and 'url' in mu:
                metadata_urls.append(mu['url'])
            elif hasattr(mu, 'url'):
                metadata_urls.append(mu.url)
            else:
                metadata_urls.append(str(mu))
        return metadata_urls

    def match_by_metadata_url(contents, md_id):
        """Find layer whose metadataUrls contain the record UUID"""
        if not md_id:
            return None
        for k, l in contents.items():
            if hasattr(l, 'metadataUrls') and l.metadataUrls:
                urls = extract_metadata_urls(l.metadataUrls)
                if any(md_id in u for u in urls):
                    print(f"Matched layer '{k}' by metadata URL containing '{md_id}'")
                    return l
        return None

    def match_by_identifier(contents, md_id):
        """Find layer whose identifier matches the record UUID"""
        if not md_id:
            return None
        for k, l in contents.items():
            layer_id = getattr(l, 'identifier', None) or getattr(l, 'id', None)
            if layer_id and md_id in str(layer_id):
                print(f"Matched layer '{k}' by identifier '{layer_id}'")
                return l
        return None

    def match_layer(contents, lname, md_id):
        """
        Match layer to fetch:
        
        1. Layer name matches `distribution.name` from the catalogue record
        2. Layer metadataUrl references the catalogue record UUID
        3. Layer identifier matches the catalogue record UUID
        4. Layer title matches `distribution.name` (fallback)
        """
        # Exact name match against distribution.name from catalogue record
        if lname and lname in contents:
            print(f"Matched layer '{lname}' by exact name")
            return contents[lname]

        # Layer's metadataUrl references the catalogue record UUID
        layer = match_by_metadata_url(contents, md_id)
        if layer:
            return layer

        # Layer's own identifier matches the catalogue record UUID
        layer = match_by_identifier(contents, md_id)
        if layer:
            return layer

        # Layer title matches distribution.name (fallback)
        if lname:
            for k, l in contents.items():
                if hasattr(l, 'title') and l.title and l.title.lower() == lname.lower():
                    print(f"Matched layer '{k}' by title")
                    return l

        return None

    match ltype:
        case 'wms':
            try:
                wms = WebMapService(url, version='1.3.0')
                layer = match_layer(wms.contents, lname, md_id)

                # No match and no lname — return service-level info
                if not layer and lname is None:
                    return {
                        'service_type': 'wms',
                        'layer_name': None,
                        'matched_by': None,
                        'queryable': None,
                        'title': getattr(getattr(wms, 'identification', None), 'title', None),
                        'abstract': getattr(getattr(wms, 'identification', None), 'abstract', None),
                        'keywords': [],
                        'bbox': None,
                        'crs4326': None,
                        'crs3857': None,
                        'styles': [],
                        'metadata_urls': []
                    }

                return {
                    'service_type': 'wms',
                    'layer_name': layer.name if layer else None,
                    'queryable': True if (layer and layer.queryable == 1) else False,
                    'title': layer.title if layer else None,
                    'abstract': layer.abstract if layer else None,
                    'keywords': list(layer.keywords) if hasattr(layer, 'keywords') else [],
                    'bbox': layer.boundingBox if hasattr(layer, 'boundingBox') else None,
                    'crs4326': ('EPSG:4326' in list(layer.crsOptions)) if hasattr(layer, 'crsOptions') else False,
                    'crs3857': ('EPSG:3857' in list(layer.crsOptions)) if hasattr(layer, 'crsOptions') else False,
                    'styles': list(layer.styles.keys()) if hasattr(layer, 'styles') else [],
                    'metadata_urls': extract_metadata_urls(layer.metadataUrls) if hasattr(layer, 'metadataUrls') else []
                }
            except Exception as e:
                print(f"Error getting WMS capabilities at {url}: {e}")
                return None

        case 'wmts':
            try:
                wmts = WebMapTileService(url)
                layer = match_layer(wmts.contents, lname, md_id)

                # Single layer — use it directly
                if not layer and len(wmts.contents) == 1:
                    layer = list(wmts.contents.values())[0]

                return {
                    'service_type': 'wmts',
                    'layer_name': layer.name if layer else None,
                    'title': layer.title if layer else None,
                    'abstract': layer.abstract if hasattr(layer, 'abstract') else None,
                    'bbox': layer.boundingBoxWGS84 if hasattr(layer, 'boundingBoxWGS84') else None,
                    'formats': list(layer.formats) if hasattr(layer, 'formats') else [],
                    'tilematrixsets': list(layer.tilematrixsets) if hasattr(layer, 'tilematrixsets') else [],
                    'metadata_urls': extract_metadata_urls(layer.metadataUrls) if hasattr(layer, 'metadataUrls') else []
                }
            except Exception as e:
                print(f"Error getting WMTS capabilities at {url}: {e}")
                return None

        case 'wfs':
            try:
                wfs = WebFeatureService(url=url, version='2.0.0')
                layer = match_layer(wfs.contents, lname, md_id)
                schema = None

                if layer:
                    try:
                        schema = wfs.get_schema(layer.id)
                    except Exception:
                        pass
                elif len(wfs.contents) == 1:
                    layer = list(wfs.contents.values())[0]
                    try:
                        schema = wfs.get_schema(layer.id)
                    except Exception:
                        pass

                return {
                    'service_type': 'wfs',
                    'layer_name': layer.id if layer else None,
                    'title': layer.title if layer else None,
                    'abstract': layer.abstract if (layer and hasattr(layer, 'abstract')) else None,
                    'keywords': list(layer.keywords) if (layer and hasattr(layer, 'keywords')) else [],
                    'bbox': layer.boundingBox if (layer and hasattr(layer, 'boundingBox')) else None,
                    'crs4326': ('EPSG:4326' in list(layer.crsOptions)) if (layer and hasattr(layer, 'crsOptions')) else False,
                    'crs3857': ('EPSG:3857' in list(layer.crsOptions)) if (layer and hasattr(layer, 'crsOptions')) else False,
                    'metadata_urls': extract_metadata_urls(layer.metadataUrls) if (layer and hasattr(layer, 'metadataUrls')) else [],
                    'schema': (schema if isinstance(schema, dict) else schema.__dict__) if schema else None
                }
            except Exception as e:
                print(f"Error getting WFS capabilities at {url}: {e}")
                return None

        case 'wcs':
            try:
                wcs = WebCoverageService(url, version='2.0.1')
                layer = match_layer(wcs.contents, lname, md_id)

                if not layer and len(wcs.contents) == 1:
                    layer = list(wcs.contents.values())[0]

                return {
                    'service_type': 'wcs',
                    'layer_name': layer.id if layer else None,
                    'title': layer.title if layer else None,
                    'abstract': layer.abstract if (layer and hasattr(layer, 'abstract')) else None,
                    'keywords': list(layer.keywords) if (layer and hasattr(layer, 'keywords')) else [],
                    'bbox': layer.boundingBox if (layer and hasattr(layer, 'boundingBox')) else None,
                    'supported_formats': list(layer.supportedFormats) if (layer and hasattr(layer, 'supportedFormat')) else [],
                    'metadata_urls': extract_metadata_urls(layer.metadataUrls) if (layer and hasattr(layer, 'metadataUrls')) else []
                }
            except Exception as e:
                print(f"Error getting WCS capabilities at {url}: {e}")
                return None

        case 'ogcapi':
            try:
                if 'collections/' in url:
                    lname2 = url.split('collections/').pop().split('/')[0].split('?')[0].split('#')[0]
                    url = url.split('collections/')[0]
                    if lname2 not in [None, '']:
                        lname = lname2

                oaf = Features(url)
                lyrs = oaf.collections()['collections']
                ls_lyrs = [l['id'] for l in lyrs]
                collection = None

                if len(ls_lyrs) == 1:
                    collection = lyrs[0]
                else:
                    for l in lyrs:
                        # Method 1 — name match
                        if lname and (lname == l.get('id', '') or lname.lower() == l.get('title', '').lower()):
                            collection = l
                            break
                        # Method 2 — metadata URL contains UUID
                        for link in l.get('links', []):
                            if md_id and md_id in link.get('href', ''):
                                print(f"Matched OGC API collection '{l.get('id')}' by metadata URL")
                                collection = l
                                break
                        if collection:
                            break

                return {
                    'service_type': 'ogcapi',
                    'layer_name': collection.get('id') if collection else None,
                    'title': collection.get('title') if collection else None,
                    'abstract': collection.get('description') if collection else None,
                    'bbox': collection.get('extent') if collection else None,
                    'crs': collection.get('crs') if collection else None
                }
            except Exception as e:
                print(f"Error getting OGC API collection at {url}: {e}")
                return None