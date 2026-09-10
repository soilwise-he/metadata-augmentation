import json
from urllib.parse import urlparse, parse_qs, parse_qsl, urlencode, urlunparse
from lxml import etree
from owslib.wms import WebMapService
from owslib.wmts import WebMapTileService
from owslib.wfs import WebFeatureService
from owslib.wcs import WebCoverageService
from owslib.ogcapi.features import Features

def get_url_version(url, default):
    """
    Use the version the server itself declares in its URL (e.g. ?version=1.1.1)
    instead of forcing one — requesting a version the server doesn't speak
    causes owslib to crash before any layer matching can even run.
    """
    query_params = {k.lower(): v for k, v in parse_qs(urlparse(url).query).items()}
    if 'version' in query_params and query_params['version']:
        return query_params['version'][0]
    return default

def strip_control_params(url):
    """
    Drop the 'request'/'service'/'version' query params (our CSV URLs always
    embed request=GetCapabilities). owslib only injects its own request=X for
    a follow-up call (DescribeCoverage/DescribeFeatureType) when that param is
    *not already present* — so leaving GetCapabilities baked into the base URL
    means every later, differently-typed request silently re-fetches
    GetCapabilities again instead. Stripping them lets owslib set the right
    one each time.
    """
    parsed = urlparse(url)
    kept = [(k, v) for k, v in parse_qsl(parsed.query) if k.lower() not in ('request', 'service', 'version')]
    return urlunparse(parsed._replace(query=urlencode(kept)))

def parse_describe_coverage_grid(desc):
    """
    Namespace-agnostic search for grid resolution in a WCS DescribeCoverage
    document — the element differs by WCS version (1.x's flat <GridOffsets>,
    2.x's GML <gml:offsetVector> per axis) so match on local tag name only.
    """
    if desc is None:
        return None
    try:
        for el in desc.iter():
            if etree.QName(el).localname == 'GridOffsets' and el.text:
                values = [float(v) for v in el.text.split()]
                if values:
                    return values
        offset_vectors = []
        for el in desc.iter():
            if etree.QName(el).localname == 'offsetVector' and el.text:
                offset_vectors.append([float(v) for v in el.text.split()])
        if offset_vectors:
            return offset_vectors
    except Exception:
        pass
    return None

def _count_decimal_places(value):
    """Same heuristic used for downloaded vector files in gdal_metadata.py."""
    try:
        text = f"{value:.12f}".rstrip('0').rstrip('.')
        if '.' in text:
            return len(text.split('.')[-1])
    except Exception:
        pass
    return 0

def _flatten_coords(coords):
    """Recursively flatten a GeoJSON 'coordinates' array (nesting depth varies
    by geometry type: Point=[x,y], Polygon=[[[x,y],...]], etc.) into flat numbers."""
    if isinstance(coords, (int, float)):
        return [coords]
    flat = []
    for c in coords:
        flat.extend(_flatten_coords(c))
    return flat

def sample_wfs_resolution(wfs, layer_id, bbox, max_features=50):
    """
    DescribeFeatureType only describes field TYPES, never coordinate
    resolution — so the only way to get a real signal is to fetch a small
    real sample of features and measure their actual coordinates, the same
    proxy technique gdal_metadata.py already uses for downloaded vector files.
    Returns (coordinate_precision, feature_density).
    """
    precisions = []
    total = None

    try:
        response = wfs.getfeature(typename=[layer_id], maxfeatures=max_features, outputFormat='application/json')
        data = json.loads(response.read())
        features = data.get('features', [])
        total = data.get('totalFeatures') or data.get('numberMatched')
        for feat in features:
            coords = (feat.get('geometry') or {}).get('coordinates')
            if coords is not None:
                precisions.extend(_count_decimal_places(v) for v in _flatten_coords(coords))
    except Exception:
        try:
            response = wfs.getfeature(typename=[layer_id], maxfeatures=max_features)
            root = etree.fromstring(response.read())
            raw_total = root.get('numberMatched') or root.get('numberOfFeatures')
            total = int(raw_total) if raw_total and raw_total.isdigit() else None
            for el in root.iter():
                if etree.QName(el).localname in ('pos', 'posList', 'coordinates') and el.text:
                    for tok in el.text.replace(',', ' ').split():
                        try:
                            precisions.append(_count_decimal_places(float(tok)))
                        except ValueError:
                            pass
        except Exception:
            return None, None

    coordinate_precision = max(precisions) if precisions else None

    feature_density = None
    if total and bbox and len(bbox) == 4:
        minx, miny, maxx, maxy = bbox
        width, height = maxx - minx, maxy - miny
        if width > 0 and height > 0:
            feature_density = total / (width * height)

    return coordinate_precision, feature_density

def process_ogc_links(url, ltype, lname, md_id):
    def safe_bbox(layer):
        """
        owslib's layer.boundingBox is (minx, miny, maxx, maxy, crs) where crs
        is a non-JSON-serializable Crs object — drop it and keep only the floats.
        """
        if not layer or not hasattr(layer, 'boundingBox') or not layer.boundingBox:
            return None
        try:
            return [float(v) for v in layer.boundingBox[:4]]
        except (TypeError, ValueError):
            return None

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
                wms = WebMapService(url, version=get_url_version(url, '1.3.0'))
                layer = match_layer(wms.contents, lname, md_id)

                def extract_scale_hint(l):
                    # 'scaleHint' is the legacy WMS 1.1.1 element (owslib attr is
                    # camelCase); 1.3.0 servers instead populate the min/max
                    # scale denominator fields, so check both.
                    if not l:
                        return None
                    if getattr(l, 'scaleHint', None):
                        return l.scaleHint
                    min_sd = getattr(l, 'min_scale_denominator', None)
                    max_sd = getattr(l, 'max_scale_denominator', None)
                    if min_sd is not None or max_sd is not None:
                        return {'min': min_sd, 'max': max_sd}
                    return None

                scale_hint = extract_scale_hint(layer)
                resolution_source = 'matched_layer' if scale_hint else None
                resolution_borrowed_from_layer = None

                # Matched layer has no scale hint — same service likely shares one
                # base resolution across sibling layers, so borrow from another layer
                if not scale_hint:
                    for other in wms.contents.values():
                        if other is layer:
                            continue
                        candidate = extract_scale_hint(other)
                        if candidate:
                            scale_hint = candidate
                            resolution_source = 'service_fallback'
                            resolution_borrowed_from_layer = other.name
                            break

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
                        'scale_hint': scale_hint,
                        'resolution_source': resolution_source,
                        'resolution_borrowed_from_layer': resolution_borrowed_from_layer,
                        'metadata_urls': []
                    }

                return {
                    'service_type': 'wms',
                    'layer_name': layer.name if layer else None,
                    'queryable': True if (layer and layer.queryable == 1) else False,
                    'title': layer.title if layer else None,
                    'abstract': layer.abstract if layer else None,
                    'keywords': list(layer.keywords) if hasattr(layer, 'keywords') else [],
                    'bbox': safe_bbox(layer),
                    'crs4326': ('EPSG:4326' in list(layer.crsOptions)) if hasattr(layer, 'crsOptions') else False,
                    'crs3857': ('EPSG:3857' in list(layer.crsOptions)) if hasattr(layer, 'crsOptions') else False,
                    'styles': list(layer.styles.keys()) if hasattr(layer, 'styles') else [],
                    'scale_hint': scale_hint,
                    'resolution_source': resolution_source,
                    'resolution_borrowed_from_layer': resolution_borrowed_from_layer,
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

                def extract_pixel_sizes(l):
                    sizes = []
                    if l and hasattr(l, 'tilematrixsets'):
                        for tms_name in l.tilematrixsets:
                            if hasattr(wmts, 'tilematrixsets') and tms_name in wmts.tilematrixsets:
                                tms = wmts.tilematrixsets[tms_name]
                                if hasattr(tms, 'tilematrices'):
                                    for tm in tms.tilematrices.values():
                                        if hasattr(tm, 'scaledenominator'):
                                            sizes.append(tm.scaledenominator)
                    return sizes

                pixel_sizes = extract_pixel_sizes(layer)
                resolution_source = 'matched_layer' if pixel_sizes else None
                resolution_borrowed_from_layer = None

                # Matched layer has no scale denominators — same service likely shares
                # one base tile grid across sibling layers, so borrow from another layer
                if not pixel_sizes:
                    for other in wmts.contents.values():
                        if other is layer:
                            continue
                        candidate = extract_pixel_sizes(other)
                        if candidate:
                            pixel_sizes = candidate
                            resolution_source = 'service_fallback'
                            resolution_borrowed_from_layer = other.name
                            break

                return {
                    'service_type': 'wmts',
                    'layer_name': layer.name if layer else None,
                    'title': layer.title if layer else None,
                    'abstract': layer.abstract if hasattr(layer, 'abstract') else None,
                    'bbox': layer.boundingBoxWGS84 if hasattr(layer, 'boundingBoxWGS84') else None,
                    'formats': list(layer.formats) if hasattr(layer, 'formats') else [],
                    'tilematrixsets': list(layer.tilematrixsets) if hasattr(layer, 'tilematrixsets') else [],
                    'pixel_sizes': pixel_sizes,
                    'resolution_source': resolution_source,
                    'resolution_borrowed_from_layer': resolution_borrowed_from_layer,
                    'metadata_urls': extract_metadata_urls(layer.metadataUrls) if hasattr(layer, 'metadataUrls') else []
                }
            except Exception as e:
                print(f"Error getting WMTS capabilities at {url}: {e}")
                return None

        case 'wfs':
            try:
                wfs = WebFeatureService(url=strip_control_params(url), version=get_url_version(url, '2.0.0'))
                layer = match_layer(wfs.contents, lname, md_id)

                if not layer and len(wfs.contents) == 1:
                    layer = list(wfs.contents.values())[0]

                schema = None
                if layer:
                    try:
                        schema = wfs.get_schema(layer.id)
                    except Exception:
                        pass

                bbox = safe_bbox(layer)

                # DescribeFeatureType only describes field TYPES, never coordinate
                # resolution — so get a real signal by sampling actual features,
                # same proxy technique used for downloaded vector files.
                coord_precision, feature_density = (
                    sample_wfs_resolution(wfs, layer.id, bbox) if layer else (None, None)
                )
                resolution_source = 'matched_layer' if coord_precision or feature_density else None
                resolution_borrowed_from_layer = None

                # Matched layer had no usable sample — try a bounded number of
                # sibling layers on the same service before giving up. Each
                # attempt costs a real feature fetch, so cap it.
                if not coord_precision and not feature_density:
                    FALLBACK_LAYER_LIMIT = 10
                    tried = 0
                    for other in wfs.contents.values():
                        if other is layer:
                            continue
                        if tried >= FALLBACK_LAYER_LIMIT:
                            break
                        tried += 1
                        other_bbox = safe_bbox(other)
                        candidate_precision, candidate_density = sample_wfs_resolution(wfs, other.id, other_bbox)
                        if candidate_precision or candidate_density:
                            coord_precision, feature_density = candidate_precision, candidate_density
                            resolution_source = 'service_fallback'
                            resolution_borrowed_from_layer = other.id
                            break

                return {
                    'service_type': 'wfs',
                    'layer_name': layer.id if layer else None,
                    'title': layer.title if layer else None,
                    'abstract': layer.abstract if (layer and hasattr(layer, 'abstract')) else None,
                    'keywords': list(layer.keywords) if (layer and hasattr(layer, 'keywords')) else [],
                    'bbox': bbox,
                    'crs4326': ('EPSG:4326' in list(layer.crsOptions)) if (layer and hasattr(layer, 'crsOptions')) else False,
                    'crs3857': ('EPSG:3857' in list(layer.crsOptions)) if (layer and hasattr(layer, 'crsOptions')) else False,
                    'coordinate_precision': coord_precision,
                    'feature_density': feature_density,
                    'resolution_source': resolution_source,
                    'resolution_borrowed_from_layer': resolution_borrowed_from_layer,
                    'metadata_urls': extract_metadata_urls(layer.metadataUrls) if (layer and hasattr(layer, 'metadataUrls')) else [],
                    'schema': (schema if isinstance(schema, dict) else schema.__dict__) if schema else None
                }
            except Exception as e:
                print(f"Error getting WFS capabilities at {url}: {e}")
                return None

        case 'wcs':
            try:
                wcs = WebCoverageService(strip_control_params(url), version=get_url_version(url, '2.0.1'))
                layer = match_layer(wcs.contents, lname, md_id)

                if not layer and len(wcs.contents) == 1:
                    layer = list(wcs.contents.values())[0]

                def extract_grid_spacing(l):
                    # 1. Cheap path: GetCapabilities summary sometimes already has it
                    if l and hasattr(l, 'grid'):
                        try:
                            if hasattr(l.grid, 'offsetvectors'):
                                caps_level = [list(v) for v in l.grid.offsetvectors]
                                if caps_level:
                                    return caps_level
                        except Exception:
                            pass
                    # 2. GetCapabilities is only a lightweight index — resolution
                    #    usually only lives in the coverage's own DescribeCoverage
                    if l and hasattr(l, 'id'):
                        try:
                            desc = wcs.getDescribeCoverage(l.id)
                            values = parse_describe_coverage_grid(desc)
                            if values:
                                return values
                        except Exception:
                            pass
                    return None

                grid_spacing = extract_grid_spacing(layer)
                resolution_source = 'matched_layer' if grid_spacing else None
                resolution_borrowed_from_layer = None

                # Matched layer has no grid info — same service likely shares one
                # base grid across sibling coverages, so borrow from another layer.
                # Each attempt costs a real DescribeCoverage fetch, so cap it.
                if not grid_spacing:
                    FALLBACK_LAYER_LIMIT = 10
                    tried = 0
                    for other in wcs.contents.values():
                        if other is layer:
                            continue
                        if tried >= FALLBACK_LAYER_LIMIT:
                            break
                        tried += 1
                        candidate = extract_grid_spacing(other)
                        if candidate:
                            grid_spacing = candidate
                            resolution_source = 'service_fallback'
                            resolution_borrowed_from_layer = other.id
                            break

                return {
                    'service_type': 'wcs',
                    'layer_name': layer.id if layer else None,
                    'title': layer.title if layer else None,
                    'abstract': layer.abstract if (layer and hasattr(layer, 'abstract')) else None,
                    'keywords': list(layer.keywords) if (layer and hasattr(layer, 'keywords')) else [],
                    'bbox': safe_bbox(layer),
                    'grid_spacing': grid_spacing,
                    'resolution_source': resolution_source,
                    'resolution_borrowed_from_layer': resolution_borrowed_from_layer,
                    'supported_formats': list(layer.supportedFormats) if (layer and hasattr(layer, 'supportedFormats')) else [],
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

                # Extract coordinate reference systems for precision inference
                crs_list = collection.get('crs', []) if collection else []
                if isinstance(crs_list, str):
                    crs_list = [crs_list]

                return {
                    'service_type': 'ogcapi',
                    'layer_name': collection.get('id') if collection else None,
                    'title': collection.get('title') if collection else None,
                    'abstract': collection.get('description') if collection else None,
                    'bbox': collection.get('extent') if collection else None,
                    'crs_list': crs_list,
                    'storage_crs': collection.get('storageCrs') if collection else None
                }
            except Exception as e:
                print(f"Error getting OGC API collection at {url}: {e}")
                return None