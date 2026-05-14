import json
import logging
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple

from osgeo import gdal, ogr, osr

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

gdal.UseExceptions()

# Spatial file extensions used for ZIP content scanning
SPATIAL_EXTENSIONS = ('.shp', '.gpkg', '.geojson', '.kml', '.gml', '.tif', '.tiff', '.nc')

# Files to skip before attempting GDAL
NON_SPATIAL_EXTENSIONS = ( '.pdf', '.html', '.doc', '.docx', '.xls', '.xlsx', 
                            '.csv', '.txt', '.7z', '.rar', '.tar', '.gz', '.bz2', '.md')
NON_SPATIAL_MEDIATYPES = {'application/pdf', 'text/html', 'text/plain', 'application/msword'}
NON_SPATIAL_ZIP_HINTS  = ('bundle', 'submission', 'review', 'manuscript', 'supplement', 'code', 'weights')

# Partial download size — enough to read any GeoTIFF/NetCDF header
PARTIAL_DOWNLOAD_BYTES = 5 * 1024 * 1024  # 5 MB


class GDALMetadataExtractor:
    """Extract spatial metadata from remote files using GDAL."""

    RASTER_EXTENSIONS = ('.tif', '.tiff', '.nc', '.png', '.jpg', '.jpeg')
    RASTER_MEDIATYPES = {
        'image/tiff', 'image/geotiff', 'image/png', 'image/jpeg',
        'application/x-netcdf', 'application/netcdf',
    }

    def __init__(self, token: Optional[str] = None):
        gdal.AllRegister()
        self.token = token

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_likely_raster(self, url: str, mediatype: Optional[str] = None) -> bool:
        if mediatype and mediatype in self.RASTER_MEDIATYPES:
            return True
        url_lower = url.lower()
        return any(url_lower.endswith(ext) for ext in self.RASTER_EXTENSIONS)

    def _forced_drivers(self, mediatype: Optional[str]) -> Optional[list]:
        """Return a GDAL driver hint list based on mediatype, or None."""
        if not mediatype:
            return None
        mt = mediatype.split(';')[0].strip().lower()
        if 'tiff' in mt or 'geotiff' in mt:
            return ['GTiff']
        if 'netcdf' in mt:
            return ['netCDF']
        if 'geojson' in mt or 'json' in mt:
            return ['GeoJSON']
        if 'gml' in mt:
            return ['GML']
        return None

    def _should_skip(self, url: str, mediatype: Optional[str]) -> Optional[str]:
        """
        Return a skip reason string if the URL should not be processed,
        or None if it should proceed.
        """
        url_lower = url.lower()

        if '.zip' in url_lower and any(h in url_lower for h in NON_SPATIAL_ZIP_HINTS):
            return "Skipped: ZIP with non-spatial name hints"

        if any(ext in url_lower for ext in NON_SPATIAL_EXTENSIONS):
            return "Skipped: non-spatial file extension"

        if mediatype and mediatype.split(';')[0].strip() in NON_SPATIAL_MEDIATYPES:
            return f"Skipped: non-spatial mediatype ({mediatype})"

        return None

    # ------------------------------------------------------------------
    # Path construction
    # ------------------------------------------------------------------

    def construct_gdal_path(self, url: str, mediatype: Optional[str] = None) -> Tuple[str, Optional[str]]:
        """
        Build the GDAL virtual path for a URL.
        For non-ZIP files returns /vsicurl/<url>.
        For ZIPs, scans the archive contents and returns the path to the
        first spatial file found (root or one subfolder deep).
        Returns: (gdal_path, inner_filename_or_None)
        """
        encoded_url = url.replace(' ', '%20')

        is_zip = (
            mediatype in ('application/zip', 'application/x-zip-compressed')
            or '.zip' in url.lower()
        )

        if not is_zip:
            return f"/vsicurl/{encoded_url}", None

        # Curly-brace vsizip syntax is required for Zenodo /content URLs
        # because ReadDir follows the redirect internally.
        vsizip_base = f"/vsizip/{{/vsicurl/{encoded_url}}}"

        try:
            root_files = gdal.ReadDir(vsizip_base)
            if not root_files:
                logger.info("ZIP is empty or unreadable")
                return vsizip_base, None

            logger.info(f"ZIP root contains: {root_files}")

            # 1. Check root level
            for filename in root_files:
                if filename.lower().endswith(SPATIAL_EXTENSIONS):
                    path = f"{vsizip_base}/{filename}"
                    logger.info(f"Found spatial file at root: {filename}")
                    return path, filename

            # 2. Check one subfolder level
            for entry in root_files:
                subdir = f"{vsizip_base}/{entry}"
                try:
                    subfiles = gdal.ReadDir(subdir)
                    if not subfiles:
                        continue
                    logger.info(f"Scanning subfolder '{entry}': {len(subfiles)} files")
                    for subfile in subfiles:
                        if subfile.lower().endswith(SPATIAL_EXTENSIONS):
                            path = f"{subdir}/{subfile}"
                            logger.info(f"Found spatial file in subfolder '{entry}': {subfile}")
                            return path, subfile
                except Exception:
                    continue

        except Exception as e:
            logger.debug(f"ZIP ReadDir failed: {e}")

        logger.info("No spatial file found in ZIP, returning base vsizip path")
        return vsizip_base, None

    # ------------------------------------------------------------------
    # Partial download fallback (for servers that block GDAL's HEAD/GET)
    # ------------------------------------------------------------------

    def _open_via_partial_download(self, url: str) -> Tuple[Optional[Any], Optional[str]]:
        """
        Download the first PARTIAL_DOWNLOAD_BYTES bytes into GDAL vsimem
        and return (dataset, vsi_path).  Caller must gdal.Unlink(vsi_path).
        Returns (None, None) on failure.
        """
        import requests

        headers = {'Range': f'bytes=0-{PARTIAL_DOWNLOAD_BYTES - 1}'}
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'

        try:
            response = requests.get(url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()

            content = b""
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                content += chunk
                if len(content) >= PARTIAL_DOWNLOAD_BYTES:
                    response.close()
                    break

            logger.info(f"Partial download: {len(content) / 1024 / 1024:.1f} MB")

            vsi_path = f"/vsimem/partial_{abs(hash(url))}.tif"
            gdal.FileFromMemBuffer(vsi_path, content)

            # Try raster first, then vector
            ds = gdal.OpenEx(vsi_path, gdal.OF_RASTER)
            if not ds:
                ds = gdal.OpenEx(vsi_path, gdal.OF_VECTOR)

            if ds:
                return ds, vsi_path

            gdal.Unlink(vsi_path)
            return None, None

        except Exception as e:
            logger.debug(f"Partial download failed: {e}")
            return None, None

    # ------------------------------------------------------------------
    # Metadata extraction
    # ------------------------------------------------------------------

    def extract_vector_metadata(self, dataset) -> Dict[str, Any]:
        metadata = {
            'type': 'vector',
            'driver': dataset.GetDriver().GetDescription() if dataset.GetDriver() else 'Unknown',
            'layer_count': dataset.GetLayerCount(),
            'layers': [],
        }
        for i in range(metadata['layer_count']):
            layer = dataset.GetLayer(i)
            metadata['layers'].append(self._extract_layer_metadata(layer, i))
        return metadata

    def _extract_layer_metadata(self, layer, index: int) -> Dict[str, Any]:
        layer_meta = {
            'layer_index': index,
            'layer_name': layer.GetName(),
            'feature_count': layer.GetFeatureCount(),
            'bbox': None,
            'crs': None,
            'epsg': None,
            'geometry_type': ogr.GeometryTypeToName(layer.GetGeomType()),
            'attributes': [],
        }

        try:
            extent = layer.GetExtent()
            if extent:
                layer_meta['bbox'] = [extent[0], extent[2], extent[1], extent[3]]
        except Exception as e:
            logger.warning(f"Could not get extent for layer {index}: {e}")

        srs = layer.GetSpatialRef()
        if srs:
            try:
                layer_meta['crs'] = srs.ExportToWkt()
                auth_code = srs.GetAuthorityCode(None)
                if auth_code:
                    layer_meta['epsg'] = f"EPSG:{auth_code}"
            except Exception as e:
                logger.debug(f"CRS extraction failed: {e}")

        layer_def = layer.GetLayerDefn()
        for j in range(min(layer_def.GetFieldCount(), 10)):
            field_def = layer_def.GetFieldDefn(j)
            layer_meta['attributes'].append({
                'name': field_def.GetName(),
                'type': field_def.GetTypeName(),
            })

        return layer_meta

    def extract_raster_metadata(self, dataset) -> Dict[str, Any]:
        metadata = {
            'type': 'raster',
            'driver': dataset.GetDriver().ShortName,
            'width': dataset.RasterXSize,
            'height': dataset.RasterYSize,
            'band_count': dataset.RasterCount,
            'bands': [],
            'bbox': None,
        }

        geotransform = dataset.GetGeoTransform()
        if geotransform:
            minx = geotransform[0]
            maxy = geotransform[3]
            maxx = minx + geotransform[1] * dataset.RasterXSize
            miny = maxy + geotransform[5] * dataset.RasterYSize
            bbox = [minx, miny, maxx, maxy]
            metadata['pixel_size'] = [geotransform[1], abs(geotransform[5])]
        else:
            bbox = None

        proj_wkt = dataset.GetProjection()
        if proj_wkt:
            srs = osr.SpatialReference(wkt=proj_wkt)
            epsg_code = srs.GetAuthorityCode(None)
            metadata['projection'] = f"EPSG:{epsg_code}" if epsg_code else proj_wkt
            metadata['epsg_code'] = epsg_code

            if epsg_code and epsg_code != '4326' and bbox:
                try:
                    wgs84 = osr.SpatialReference()
                    wgs84.ImportFromEPSG(4326)
                    transform = osr.CoordinateTransformation(srs, wgs84)
                    min_pt = transform.TransformPoint(bbox[0], bbox[1])
                    max_pt = transform.TransformPoint(bbox[2], bbox[3])
                    metadata['bbox'] = [min_pt[0], min_pt[1], max_pt[0], max_pt[1]]
                except Exception as e:
                    logger.debug(f"WGS84 transform failed: {e}")
                    metadata['bbox'] = bbox
            else:
                metadata['bbox'] = bbox

        for i in range(1, dataset.RasterCount + 1):
            band = dataset.GetRasterBand(i)
            metadata['bands'].append({
                'band_number': i,
                'data_type': gdal.GetDataTypeName(band.DataType),
                'color_interpretation': gdal.GetColorInterpretationName(band.GetColorInterpretation()),
                'no_data_value': band.GetNoDataValue(),
            })

        metadata['metadata'] = {
            domain: dataset.GetMetadata(domain)
            for domain in (dataset.GetMetadataDomainList() or [])
        }

        return metadata

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def process_url(self, url: str, mediatype: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a URL and extract spatial metadata.

        Strategy:
        1. Skip obviously non-spatial files early.
        2. Build a GDAL virtual path (vsicurl or vsizip).
        3. Try to open with GDAL directly.
        4. If GDAL fails (e.g. server blocks range requests), fall back to
            a partial HTTP download into vsimem.
        """
        result = {
            'url': url,
            'mediatype': mediatype,
            'processed_date': datetime.now(timezone.utc).isoformat(),
            'success': False,
            'metadata': None,
            'error': None,
        }

        # --- Early skip checks ---
        skip_reason = self._should_skip(url, mediatype)
        if skip_reason:
            result['error'] = skip_reason
            return result

        try:
            gdal_path, inner_file = self.construct_gdal_path(url, mediatype)
            likely_raster = self._is_likely_raster(url, mediatype) or self._is_likely_raster(inner_file or '')
            drivers  = self._forced_drivers(mediatype)
            dataset  = None
            vsi_path = None

            # --- Attempt 1: direct vsicurl/vsizip ---
            try:
                if likely_raster:
                    dataset = gdal.OpenEx(gdal_path, gdal.OF_RASTER, allowed_drivers=drivers or [])
                    if not dataset:
                        dataset = gdal.OpenEx(gdal_path, gdal.OF_VECTOR)
                else:
                    dataset = gdal.OpenEx(gdal_path, gdal.OF_VECTOR)
                    if not dataset:
                        dataset = gdal.OpenEx(gdal_path, gdal.OF_RASTER, allowed_drivers=drivers or [])
            except Exception as e:
                logger.info(f"Direct GDAL open failed ({e}), trying fallback...")
                dataset = None

            # --- Attempt 2: partial download fallback ---
            if not dataset:
                PARTIAL_DOWNLOAD_FORMATS = ('.tif', '.tiff', '.nc', '.geojson', '.gpkg', '.shp', '.gml', '.kml')
                url_lower   = url.lower()
                inner_lower = (inner_file or '').lower()

                if '.zip' in url_lower:
                    logger.info("ZIP vsizip failed — server does not support range requests")
                    result['error'] = "ZIP remote access not supported (server blocks range requests)"
                    return result

                is_direct_spatial = (
                    any(ext in url_lower   for ext in PARTIAL_DOWNLOAD_FORMATS) or
                    any(ext in inner_lower for ext in PARTIAL_DOWNLOAD_FORMATS) or
                    (mediatype and mediatype in self.RASTER_MEDIATYPES)
                )

                if is_direct_spatial:
                    logger.info("Trying partial download fallback...")
                    dataset, vsi_path = self._open_via_partial_download(url)
                else:
                    logger.info("Skipping partial download — not a known spatial format")

            # --- Extract metadata ---
            if dataset:
                try:
                    is_raster = dataset.RasterCount > 0
                except Exception:
                    is_raster = likely_raster

                try:
                    if is_raster:
                        result['metadata'] = self.extract_raster_metadata(dataset)
                    else:
                        result['metadata'] = self.extract_vector_metadata(dataset)
                    result['success'] = True
                except Exception as e:
                    logger.error(f"Metadata extraction failed: {e}")
                    result['error'] = f"Metadata extraction failed: {e}"
                finally:
                    dataset = None
                    if vsi_path:
                        gdal.Unlink(vsi_path)

                return result

            result['error'] = "Could not open file with GDAL (unsupported format or inaccessible)"

        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Error processing {url}: {e}")

        return result

# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract spatial metadata from a URL')
    parser.add_argument('url',            help='URL to process')
    parser.add_argument('--mediatype',    help='Optional mediatype hint')
    parser.add_argument('--token',        help='Optional bearer token (e.g. Zenodo API token)')
    parser.add_argument('--output',       help='Output JSON file')
    args = parser.parse_args()

    extractor = GDALMetadataExtractor(token=args.token)

    print(f"Processing: {args.url}")
    result = extractor.process_url(args.url, args.mediatype)

    if result['success']:
        print(f"\nSuccess")
        print(f"Type:   {result['metadata']['type']}")
        print(f"Driver: {result['metadata'].get('driver')}")
        output = json.dumps(result, indent=2, default=str)
        if args.output:
            with open(args.output, 'w') as f:
                f.write(output)
        else:
            print(output)
    else:
        print(f"\nError: {result['error']}")