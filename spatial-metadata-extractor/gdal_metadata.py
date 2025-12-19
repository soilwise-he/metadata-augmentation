import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple
from osgeo import gdal, ogr, osr
import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

gdal.UseExceptions()

# Spatial file extensions
SPATIAL_EXTENSIONS = ('.shp', '.gpkg', '.geojson', '.kml', '.gml', '.tif', '.tiff', '.nc')

class SpatialMetadataExtractor:
    """Extract spatial metadata from remote files using GDAL"""
    
    # Raster file extensions and mediatypes
    RASTER_EXTENSIONS = ('.tif', '.tiff', '.nc', '.png', '.jpg', '.jpeg')
    RASTER_MEDIATYPES = {
        'image/tiff', 'image/geotiff', 'image/png', 'image/jpeg',
        'application/x-netcdf', 'application/netcdf'
    }
    
    def __init__(self, max_features: int = 100):
        gdal.AllRegister()
        self.max_features = max_features
    
    def _is_likely_raster(self, url: str, mediatype: Optional[str] = None) -> bool:
        """Determine if URL likely points to raster data based on extension or mediatype"""
        if mediatype and mediatype in self.RASTER_MEDIATYPES:
            return True
        
        url_lower = url.lower()
        return any(url_lower.endswith(ext) for ext in self.RASTER_EXTENSIONS)
    
    def construct_gdal_path(self, url: str, mediatype: Optional[str] = None) -> Tuple[str, Optional[str]]:
        """
        Construct GDAL virtual file path
        Returns: (gdal_path, filename_if_found)
        """
        encoded_url = url.replace(' ', '%20')
        
        # Check if ZIP
        is_zip = (
            mediatype in ['application/zip', 'application/x-zip-compressed'] or 
            url.lower().endswith('.zip')
        )
        
        if not is_zip:
            return f"/vsicurl/{encoded_url}", None
        
        # Handle ZIP files
        vsizip_path = f"/vsizip/{{/vsicurl/{encoded_url}}}"
        
        # List ZIP contents to find spatial file 
        try:
            files = gdal.ReadDir(vsizip_path)
            if files:
                logger.info(f"ZIP contains: {files}")
                
                # Find first spatial file
                for filename in files:
                    if filename.lower().endswith(SPATIAL_EXTENSIONS):
                        full_path = f"{vsizip_path}/{filename}"
                        logger.info(f"Found spatial file: {filename}")
                        return full_path, filename
        except Exception as e:
            logger.debug(f"Failed ReadDir: {e}")
        
        # Return ZIP path, let GDAL auto-detect
        logger.info("Using ZIP path directly (GDAL will auto-detect)")
        return vsizip_path, None
    
    def extract_vector_metadata(self, dataset) -> Dict[str, Any]:
        """Extract metadata from vector dataset"""
        metadata = {
            'type': 'vector',
            'driver': dataset.GetDriver().GetDescription() if dataset.GetDriver() else 'Unknown',
            'layer_count': dataset.GetLayerCount(),
            'layers': [],
            'features': []
        }
        
        # Process all layers
        for i in range(metadata['layer_count']):
            layer = dataset.GetLayer(i)
            layer_meta = self._extract_layer_metadata(layer, i)
            metadata['layers'].append(layer_meta)
            
            # Extract features from first layer only
            if i == 0:
                metadata['features'] = self._extract_features(layer)
        
        return metadata
    
    def _extract_layer_metadata(self, layer, index: int) -> Dict[str, Any]:
        """Extract metadata from a single layer"""
        layer_meta = {
            'layer_index': index,
            'layer_name': layer.GetName(),
            'feature_count': layer.GetFeatureCount(),
            'bbox': None,
            'crs': None,
            'epsg': None,
            'geometry_type': ogr.GeometryTypeToName(layer.GetGeomType()),
            'attributes': []
        }
        
        # Get bbox
        try:
            extent = layer.GetExtent()
            if extent:
                layer_meta['bbox'] = [extent[0], extent[2], extent[1], extent[3]]
        except Exception as e:
            logger.warning(f"Could not get extent for layer {index}: {e}")
        
        # Get CRS
        srs = layer.GetSpatialRef()
        if srs:
            try:
                layer_meta['crs'] = srs.ExportToWkt()
                auth_code = srs.GetAuthorityCode(None)
                if auth_code:
                    layer_meta['epsg'] = f"EPSG:{auth_code}"
            except Exception as e:
                logger.debug(f"CRS extraction failed: {e}")
        
        # Get attributes (limit to 10)
        layer_def = layer.GetLayerDefn()
        for j in range(min(layer_def.GetFieldCount(), 10)):
            field_def = layer_def.GetFieldDefn(j)
            layer_meta['attributes'].append({
                'name': field_def.GetName(),
                'type': field_def.GetTypeName()
            })
        
        return layer_meta
    
    def _extract_features(self, layer) -> list:
        """Extract features from layer, up to max_features"""
        features = []
        layer.ResetReading()
        layer_def = layer.GetLayerDefn()
        
        logger.info(f"Extracting up to {self.max_features} features...")
        
        for count, feature in enumerate(layer):
            if count >= self.max_features:
                break
            
            try:
                # Get geometry as GeoJSON
                geom = feature.GetGeometryRef()
                geom_json = json.loads(geom.ExportToJson()) if geom else None
                
                # Get properties
                properties = {
                    layer_def.GetFieldDefn(j).GetName(): feature.GetField(j)
                    for j in range(layer_def.GetFieldCount())
                }
                
                features.append({
                    'type': 'Feature',
                    'geometry': geom_json,
                    'properties': properties
                })
            except Exception as e:
                logger.warning(f"Error extracting feature {count}: {e}")
        
        logger.info(f"Extracted {len(features)} features")
        return features
    
    def extract_raster_metadata(self, dataset) -> Dict[str, Any]:
        """Extract metadata from raster dataset"""
        metadata = {
            'type': 'raster',
            'driver': dataset.GetDriver().ShortName,
            'width': dataset.RasterXSize,
            'height': dataset.RasterYSize,
            'band_count': dataset.RasterCount,
            'bands': [],
            'bbox': None
        }
        
        # Get geotransform and bbox
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
        
        # Get projection
        proj_wkt = dataset.GetProjection()
        if proj_wkt:
            srs = osr.SpatialReference(wkt=proj_wkt)
            epsg_code = srs.GetAuthorityCode(None)
            metadata['projection'] = f"EPSG:{epsg_code}" if epsg_code else proj_wkt
            metadata['epsg_code'] = epsg_code
            
            # Transform to WGS84 if needed
            if epsg_code and epsg_code != '4326' and bbox:
                try:
                    wgs84 = osr.SpatialReference()
                    wgs84.ImportFromEPSG(4326)
                    transform = osr.CoordinateTransformation(srs, wgs84)
                    
                    min_point = transform.TransformPoint(bbox[0], bbox[1])
                    max_point = transform.TransformPoint(bbox[2], bbox[3])
                    metadata['bbox'] = [min_point[0], min_point[1], max_point[0], max_point[1]]
                except Exception as e:
                    logger.debug(f"WGS84 transformation failed: {e}")
                    metadata['bbox'] = bbox
            else:
                metadata['bbox'] = bbox
        
        # Get band information
        for i in range(1, dataset.RasterCount + 1):
            band = dataset.GetRasterBand(i)
            metadata['bands'].append({
                'band_number': i,
                'data_type': gdal.GetDataTypeName(band.DataType),
                'color_interpretation': gdal.GetColorInterpretationName(band.GetColorInterpretation()),
                'no_data_value': band.GetNoDataValue()
            })
        
        # Get metadata domains
        metadata['metadata'] = {
            domain: dataset.GetMetadata(domain)
            for domain in (dataset.GetMetadataDomainList() or [])
        }
        
        return metadata
    
    def process_url(self, url: str, mediatype: Optional[str] = None) -> Dict[str, Any]:
        """
        Process URL and extract spatial metadata
        Returns dict with metadata or error information
        """
        result = {
            'url': url,
            'mediatype': mediatype,
            'processed_date': datetime.now(timezone.utc).isoformat(),
            'success': False,
            'metadata': None,
            'error': None
        }

        try:
            gdal_path, _ = self.construct_gdal_path(url, mediatype)
            
            # Determine likely type and try that first
            likely_raster = self._is_likely_raster(url, mediatype)
            
            if likely_raster:
                # Try raster first
                dataset = gdal.OpenEx(gdal_path, gdal.OF_RASTER)
                if dataset:
                    result['metadata'] = self.extract_raster_metadata(dataset)
                    result['success'] = True
                    return result
                # Fall back to vector
                dataset = gdal.OpenEx(gdal_path, gdal.OF_VECTOR)
            else:
                # Try vector first
                dataset = gdal.OpenEx(gdal_path, gdal.OF_VECTOR)
                if dataset:
                    result['metadata'] = self.extract_vector_metadata(dataset)
                    result['success'] = True
                    return result
                # Fall back to raster
                dataset = gdal.OpenEx(gdal_path, gdal.OF_RASTER)
            
            if dataset:
                metadata_type = 'raster' if likely_raster else 'vector'
                if metadata_type == 'raster':
                    result['metadata'] = self.extract_raster_metadata(dataset)
                else:
                    result['metadata'] = self.extract_vector_metadata(dataset)
                result['success'] = True
                return result
            
            result['error'] = "Could not open file with GDAL (unsupported format or inaccessible)"
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"Error processing {url}: {e}")
        
        return result

if __name__ == "__main__":   
    parser = argparse.ArgumentParser(description='Extract spatial metadata from URLs')
    parser.add_argument('url', help='URL to process')
    parser.add_argument('--mediatype', help='Optional mediatype hint')
    parser.add_argument('--output', help='Output JSON file')
    parser.add_argument('--max-features', type=int, default=100, help='Max features to extract')
    
    args = parser.parse_args()
    
    extractor = SpatialMetadataExtractor(max_features=args.max_features)
    
    print(f"Processing: {args.url}")
    result = extractor.process_url(args.url, args.mediatype)
    
    if result['success']:
        print(f"\nSuccess")
        print(f"Type: {result['metadata']['type']}")
        print(f"Driver: {result['metadata'].get('driver')}")
        
        output = json.dumps(result, indent=2, default=str)
        
        if args.output:
            with open(args.output, 'w') as f:
                f.write(output)
        else:
            print(output)
    else:
        print(f"\nError: {result['error']}")