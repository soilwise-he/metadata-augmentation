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

    match ltype:
        case 'wms':
            layer = None
            try:
                wms = WebMapService(url, version='1.3.0')
                if lname is not None and lname in list(wms.contents):
                    layer = wms.contents[lname]
                else:
                    # Search by metadata URL
                    for l in wms.contents.items():
                        if hasattr(l, 'metadataUrls') and l.metadataUrls:
                            urls = extract_metadata_urls(l.metadataUrls)
                            if md_id and any(md_id in url for url in urls):
                                layer = l
                                break
                    # Only try to match by title if lname is not None
                    if not layer and lname:
                        for k, l in wms.contents.items():
                            if hasattr(l, 'title') and hasattr(l, 'name') and l.title and lname and l.title.lower() == lname.lower():
                                layer = l
                                break

                # If no layer is found and lname is None, return service-level info
                if not layer and lname is None:
                    return {
                        'service_type': 'wms',
                        'layer_name': None,
                        'queryable': None,
                        'layer_all': list(wms.contents),
                        'title': getattr(wms, 'identification', None).title if hasattr(wms, 'identification') and hasattr(wms.identification, 'title') else None,
                        'abstract': getattr(wms, 'identification', None).abstract if hasattr(wms, 'identification') and hasattr(wms.identification, 'abstract') else None,
                        'keywords': [],
                        'bbox': None,
                        'crs4326': None,
                        'crs3857': None,
                        'styles': [],
                        'metadata_urls': []
                    }

                # Convert matched layer to dictionary
                return {
                    'service_type': 'wms',
                    'layer_name': layer.name if layer else None,
                    'queryable': True if (layer and layer.queryable == 1) else False,
                    'layer_all': list(wms.contents),
                    'title': layer.title if layer else None,
                    'abstract': layer.abstract if layer else None,
                    'keywords': list(layer.keywords) if hasattr(layer, 'keywords') else [],
                    'bbox': layer.boundingBox if hasattr(layer, 'boundingBox') else None,
                    'crs4326': ('EPSG:4326' in list(layer.crsOptions) if hasattr(layer, 'crsOptions') else []),
                    'crs3857': ('EPSG:3857' in list(layer.crsOptions) if hasattr(layer, 'crsOptions') else []),
                    'styles': list(layer.styles.keys()) if hasattr(layer, 'styles') else [],
                    'metadata_urls': extract_metadata_urls(layer.metadataUrls) if hasattr(layer, 'metadataUrls') else []
                }
            except Exception as e:
                print(f"Error getting WMS capabilities at {url}: {e}")
                return None

        case 'wmts':
            try:
                layer = None
                wmts = WebMapTileService(url)
                if lname in list(wmts.contents):
                    layer = wmts.contents[lname]
                elif len(wmts.contents) == 1:
                    layer = list(wmts.contents.values())[0]
                else:
                    for k,l in wmts.contents.items():
                        if hasattr(l, 'title') and hasattr(l, 'name') and l.title and lname and l.title.lower() == lname.lower():
                            layer = l
                            break
                    

                return {
                    'service_type': 'wmts',
                    'layer_name': layer.name if layer else None,
                    'layer_all': list(wmts.contents),
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
                feature = None
                schema = None
                
                if lname in list(wfs.contents):
                    feature = wfs.contents[lname]
                    schema = wfs.get_schema(lname)
                elif len(wfs.contents) == 1:
                    feature = list(wfs.contents.values())[0]
                    schema = wfs.get_schema(feature.id)
                else:
                    # Search by metadata URLs
                    for f in wfs.contents.values():
                        if hasattr(f, 'metadataUrls') and f.metadataUrls:
                            urls = extract_metadata_urls(f.metadataUrls)
                            if md_id and any(md_id in url for url in urls):
                                feature = f
                                schema = wfs.get_schema(f.id)
                                break
                    
                    # If still no match and lname is provided, try matching by title
                    if not feature and lname:
                        for f in wfs.contents.values():
                            if hasattr(f, 'title') and f.title and f.title.lower() == lname.lower():
                                feature = f
                                schema = wfs.get_schema(f.id)
                                break
                
                return {
                    'service_type': 'wfs',
                    'layer_name': feature.id if feature else None,
                    'layer_all': list(wfs.contents),
                    'title': feature.title if feature else None,
                    'abstract': feature.abstract if (feature and hasattr(feature, 'abstract')) else None,
                    'keywords': list(feature.keywords) if (feature and hasattr(feature, 'keywords')) else [],
                    'bbox': feature.boundingBox if (feature and hasattr(feature, 'boundingBox')) else None,
                    'crs4326': ('EPSG:4326' in list(feature.crsOptions) if (feature and hasattr(feature, 'crsOptions')) else []),
                    'crs3857': ('EPSG:3857' in list(feature.crsOptions) if (feature and hasattr(feature, 'crsOptions')) else []),
                    'metadata_urls': extract_metadata_urls(feature.metadataUrls) if (feature and hasattr(feature, 'metadataUrls')) else [],
                    'schema': (schema if isinstance(schema, dict) else schema.__dict__) if schema else None
                }
            except Exception as e:
                print(f"Error getting WFS capabilities at {url}: {e}")
                return None
    
        case 'wcs':
            try:
                wcs = WebCoverageService(url, version='2.0.1')
                coverage = None
                
                if lname in list(wcs.contents):
                    coverage = wcs.contents[lname]
                elif len(wcs.contents) == 1:
                    coverage = list(wcs.contents.values())[0]
                else:
                    # Search by metadata URLs
                    for c in wcs.contents.values():
                        if hasattr(c, 'metadataUrls') and c.metadataUrls:
                            urls = extract_metadata_urls(c.metadataUrls)
                            if md_id and any(md_id in url for url in urls):
                                coverage = c
                                break
                    
                    # If still no match and lname is provided, try matching by title
                    if not coverage and lname:
                        for c in wcs.contents.values():
                            if hasattr(c, 'title') and c.title and c.title.lower() == lname.lower():
                                coverage = c
                                break

                return {
                    'service_type': 'wcs',
                    'layer_name': coverage.id if coverage else None,
                    'layer_all': list(wcs.contents), 
                    'title': coverage.title if coverage else None,
                    'abstract': coverage.abstract if (coverage and hasattr(coverage, 'abstract')) else None,
                    'keywords': list(coverage.keywords) if (coverage and hasattr(coverage, 'keywords')) else [],
                    'bbox': coverage.boundingBox if (coverage and hasattr(coverage, 'boundingBox')) else None,
                    'supported_formats': list(coverage.supportedFormats) if (coverage and hasattr(coverage, 'supportedFormat')) else [],
                    'metadata_urls': extract_metadata_urls(coverage.metadataUrls) if (coverage and hasattr(coverage, 'metadataUrls')) else []
                }
            except Exception as e:
                print(f"Error getting WCS capabilities at {url}: {e}")
                return None
        case 'ogcapi':
            try:
                if 'collections/' in url:
                    lname2 = url.split('collections/').pop().split('/')[0].split('?')[0].split('#')[0]
                    url = url.split('collections/')[0]
                if lname2 not in [None,'']:
                    lname = lname2
                oaf = Features(url)
                lyrs = oaf.collections()['collections']
                ls_lyrs = [l['id'] for l in lyrs]
                collection = None
                if len(ls_lyrs) == 1:
                    collection = lyrs[0]
                else:    
                    for l in lyrs:
                        if lname not in [None, ''] and (lname == l.get('id','') or lname.lower() == l.get('title','')):
                            collection = l
                            break
                        # todo: check metadata link matches md_id
                return {
                    'service_type': 'ogcapi',
                    'layer_name': collection.id if hasattr(collection, 'id') else None,
                    'layer_all': ls_lyrs,
                    'title': collection.title if hasattr(collection, 'title') else None,
                    'abstract': collection.description if hasattr(collection, 'description') else None,
                    'bbox': collection.extent if hasattr(collection, 'extent') else None,
                    'crs': collection.crs if hasattr(collection, 'crs') else None
                }
            except Exception as e:
                print(f"Error getting OGC API collection at {url}: {e}")
                return None