import os
import re
import tarfile
import zipfile
from datetime import datetime
from xml.etree.ElementTree import parse

from muninn.schema import Mapping, Text, Integer, Real
from muninn.geometry import Point, LinearRing, Polygon
from muninn.struct import Struct


# Namespaces

class Sentinel2Namespace(Mapping):
    mission = Text(index=True, optional=True)  # S2, S2A, S2B, S2C, S2D
    absolute_orbit = Integer(index=True, optional=True)
    relative_orbit = Integer(index=True, optional=True)
    orbit_direction = Text(index=True, optional=True)
    tile_number = Text(index=True, optional=True)
    datatake_id = Text(index=True, optional=True)  # GS[SS]_[YYYYMMDDTHHMMSS]_[RRRRRR]_N[xx.yy]
    processing_baseline = Integer(index=True, optional=True)  # [XXYY]
    processing_facility = Text(index=True, optional=True)
    processor_name = Text(index=True, optional=True)
    processor_version = Text(index=True, optional=True)
    cloud_cover = Real(index=True, optional=True)
    snow_cover = Real(index=True, optional=True)


def namespaces():
    return ["sentinel2"]


def namespace(namespace_name):
    return Sentinel2Namespace


# Product types

USER_PRODUCT_TYPES = [
    'MSIL1C',
    'MSIL2A',
]

PDI_PRODUCT_TYPES = [
    'MSI_L1C_DS',
    'MSI_L1C_TL',
    'MSI_L2A_DS',
    'MSI_L2A_TL',
]

AUX_EOF_PRODUCT_TYPES = [
    'AUX_POEORB',
]

AUX_HDR_DBL_PRODUCT_TYPES = [
    'AUX_GNSSRD',
    'AUX_PROQUA',
]

GIPP_PRODUCT_TYPES = [
    'GIP_ATMIMA',
    'GIP_ATMSAD',
    'GIP_BLINDP',
    'GIP_CLOINV',
    'GIP_CLOPAR',
    'GIP_CONVER',
    'GIP_DATATI',
    'GIP_DECOMP',
    'GIP_EARMOD',
    'GIP_ECMWFP',
    'GIP_G2PARA',
    'GIP_G2PARE',
    'GIP_GEOPAR',
    'GIP_INTDET',
    'GIP_INVLOC',
    'GIP_JP2KPA',
    'GIP_L2ACAC',
    'GIP_L2ACSC',
    'GIP_LREXTR',
    'GIP_MASPAR',
    'GIP_OLQCPA',
    'GIP_PRDLOC',
    'GIP_PROBA2',
    'GIP_PROBAS',
    'GIP_R2ABCA',
    'GIP_R2BINN',
    'GIP_R2CRCO',
    'GIP_R2DECT',
    'GIP_R2DEFI',
    'GIP_R2DENT',
    'GIP_R2DEPI',
    'GIP_R2EOB2',
    'GIP_R2EQOG',
    'GIP_R2L2NC',
    'GIP_R2NOMO',
    'GIP_R2PARA',
    'GIP_R2SWIR',
    'GIP_R2WAFI',
    'GIP_RESPAR',
    'GIP_SPAMOD',
    'GIP_TILPAR',
    'GIP_VIEDIR',
]

IERS_PRODUCT_TYPES = [
    'AUX_UT1UTC',
]


class Sentinel2Product(object):

    def __init__(self, product_type):
        self.product_type = product_type
        self.is_multi_file_product = False
        self.filename_pattern = None

    @property
    def hash_type(self):
        return "md5"

    @property
    def namespaces(self):
        return ["sentinel2"]

    @property
    def use_enclosing_directory(self):
        return False

    def parse_filename(self, filename):
        match = re.match(self.filename_pattern, os.path.basename(filename))
        if match:
            return match.groupdict()
        return None

    def identify(self, paths):
        if len(paths) != 1:
            return False
        return re.match(self.filename_pattern, os.path.basename(paths[0])) is not None

    def read_xml_component(self, filepath, componentpath):
        if self.zipped:
            if not self.is_multi_file_product:
                componentpath = os.path.join(os.path.splitext(os.path.basename(filepath))[0], componentpath)
            with zipfile.ZipFile(filepath) as zproduct:
                with zproduct.open(componentpath) as manifest:
                    return parse(manifest).getroot()
        else:
            with open(os.path.join(filepath, componentpath)) as manifest:
                return parse(manifest).getroot()


class SAFEProduct(Sentinel2Product):

    def __init__(self, product_type, zipped=False):
        self.product_type = product_type
        self.zipped = zipped
        pattern = [
            r"^(?P<mission>S2(_|A|B|C|D))",
            r"(?P<product_type>%s)" % product_type,
            r"(?P<validity_start>[\dT]{15})",
            r"N(?P<processing_baseline>[\d]{4})",
            r"R(?P<relative_orbit>[\d]{3})",
            r"T(?P<tile_number>.{5})",
            r"(?P<creation_date>[\dT]{15})",
        ]
        if zipped:
            self.filename_pattern = "_".join(pattern) + r"\.SAFE\.zip$"
        else:
            self.filename_pattern = "_".join(pattern) + r"\.SAFE$"

    def archive_path(self, properties):
        name_attrs = self.parse_filename(properties.core.physical_name)
        mission = name_attrs['mission']
        if mission[2] == "_":
            mission = mission[0:2]
        return os.path.join(
            mission,
            name_attrs['product_type'],
            name_attrs['validity_start'][0:4],
            name_attrs['validity_start'][4:6],
            name_attrs['validity_start'][6:8],
        )

    def _analyze_mtd(self, root, properties):
        ns = {"n1": "https://psd-14.sentinel2.eo.esa.int/PSD/User_Product_Level-" + self.product_type[-2:] + ".xsd"}

        core = properties.core
        sentinel2 = properties.sentinel2
        product_info = root.find(".//Product_Info", ns)
        core.validity_start = datetime.strptime(product_info.find("./PRODUCT_START_TIME").text, "%Y-%m-%dT%H:%M:%S.%fZ")
        core.validity_stop = datetime.strptime(product_info.find("./PRODUCT_STOP_TIME").text, "%Y-%m-%dT%H:%M:%S.%fZ")
        core.creation_date = datetime.strptime(product_info.find("./GENERATION_TIME").text, "%Y-%m-%dT%H:%M:%S.%fZ")
        sentinel2.datatake_id = product_info.find("./Datatake").get("datatakeIdentifier")
        sentinel2.absolute_orbit = int(sentinel2.datatake_id[21:27])
        sentinel2.orbit_direction = product_info.find("./Datatake/SENSING_ORBIT_DIRECTION").text.lower()

        coord = [float(value) for value in root.find(".//Global_Footprint/EXT_POS_LIST").text.split()]
        linearring = LinearRing([Point(float(lon), float(lat)) for lat, lon in zip(coord[0::2], coord[1::2])])
        core.footprint = Polygon([linearring])

        qi_info = root.find("./n1:Quality_Indicators_Info", ns)
        sentinel2.cloud_cover = float(qi_info.find("./Cloud_Coverage_Assessment").text)
        snow_coverage = qi_info.find("./Snow_Coverage_Assessment")
        if snow_coverage is not None:
            sentinel2.snow_cover = float(snow_coverage.text)

    def analyze(self, paths, filename_only=False):
        inpath = paths[0]
        name_attrs = self.parse_filename(inpath)

        properties = Struct()

        core = properties.core = Struct()
        core.product_name = os.path.splitext(os.path.basename(inpath))[0]
        if self.zipped:
            core.product_name = os.path.splitext(core.product_name)[0]
        core.validity_start = datetime.strptime(name_attrs['validity_start'], "%Y%m%dT%H%M%S")

        sentinel2 = properties.sentinel2 = Struct()
        sentinel2.mission = name_attrs['mission']
        if sentinel2.mission[2] == "_":
            sentinel2.mission = sentinel2.mission[0:2]
        sentinel2.processing_baseline = int(name_attrs['processing_baseline'])
        sentinel2.relative_orbit = int(name_attrs['relative_orbit'])
        sentinel2.tile_number = name_attrs['tile_number']
        core.creation_date = datetime.strptime(name_attrs['creation_date'], "%Y%m%dT%H%M%S")

        if not filename_only:
            # Update properties based on MTD content
            self._analyze_mtd(self.read_xml_component(inpath, "MTD_" + self.product_type + ".xml"), properties)

        return properties


class PDIProduct(Sentinel2Product):

    def __init__(self, product_type, zipped=False):
        self.product_type = product_type
        self.zipped = zipped
        pattern = [
            r"^(?P<mission>S2(_|A|B|C|D))",
            r"(?P<file_class>.{4})",
            r"(?P<product_type>%s)" % product_type,
            r"(?P<site_centre>.{4})",
            r"(?P<creation_date>[\dT]{15})",
        ]
        if product_type.endswith("DS"):
            pattern.append(r"S(?P<validity_start>[\dT]{15})")
        elif product_type.endswith("TL"):
            pattern += [
                r"A(?P<absolute_orbit>[\d]{6})",
                r"T(?P<tile_number>.{5})",
            ]
        pattern.append(r"N(?P<processing_baseline>[\d]{2}\.[\d]{2})")
        if zipped:
            self.filename_pattern = "_".join(pattern) + r"\.zip$"
        else:
            self.filename_pattern = "_".join(pattern) + r"$"

    def archive_path(self, properties):
        validity_start = properties.core.validity_start.strftime("%Y%m%d")
        return os.path.join(
            properties.sentinel2.mission,
            self.product_type,
            validity_start[0:4],
            validity_start[4:6],
            validity_start[6:8],
        )

    def _analyze_inventory_metadata(self, root, properties):
        ns = {"": "https://psd-12.sentinel2.eo.esa.int/PSD/Inventory_Metadata.xsd"}

        core = properties.core
        sentinel2 = properties.sentinel2
        core.validity_start = datetime.strptime(root.find("./Validity_Start", ns).text, "UTC=%Y-%m-%dT%H:%M:%S.%f")
        core.validity_stop = datetime.strptime(root.find("./Validity_Stop", ns).text, "UTC=%Y-%m-%dT%H:%M:%S.%f")
        core.creation_date = datetime.strptime(root.find("./Generation_Time", ns).text, "UTC=%Y-%m-%dT%H:%M:%S.%f")
        points = root.find("./Geographic_Localization/List_Of_Geo_Pnt", ns)
        latitudes = [float(v.text) for v in points.findall("./Geo_Pnt/LATITUDE", ns)]
        longitudes = [float(v.text) for v in points.findall("./Geo_Pnt/LONGITUDE", ns)]
        linearring = LinearRing([Point(float(lon), float(lat)) for lat, lon in zip(latitudes, longitudes)])
        core.footprint = Polygon([linearring])
        sentinel2.datatake_id = root.find("./Group_ID", ns).text
        sentinel2.absolute_orbit = int(sentinel2.datatake_id[21:27])
        sentinel2.orbit_direction = "ascending" if root.find("./Ascending_Flag", ns).text == "true" else "descending"
        sentinel2.processing_baseline = int(sentinel2.datatake_id[-5:].replace(".", ""))
        sentinel2.cloud_cover = float(root.find("./CloudPercentage", ns).text)

    def _analyze_mtd_ds(self, root, properties):
        ns = {"n1": "https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-2A_Datastrip_Metadata.xsd"}

        core = properties.core
        sentinel2 = properties.sentinel2
        general_info = root.find("./n1:General_Info", ns)
        datatake_info = general_info.find("./Datatake_Info")
        datastrip_info = general_info.find("./Datastrip_Time_Info")
        processing_info = general_info.find("./Processing_Info")
        core.validity_start = datetime.strptime(datastrip_info.find("./DATASTRIP_SENSING_START").text,
                                                "%Y-%m-%dT%H:%M:%S.%fZ")
        core.validity_stop = datetime.strptime(datastrip_info.find("./DATASTRIP_SENSING_STOP").text,
                                               "%Y-%m-%dT%H:%M:%S.%fZ")
        core.creation_date = datetime.strptime(general_info.find("./Archiving_Info/ARCHIVING_TIME").text,
                                               "%Y-%m-%dT%H:%M:%S.%fZ")
        sentinel2.datatake_id = datatake_info.get("datatakeIdentifier")
        sentinel2.absolute_orbit = int(sentinel2.datatake_id[21:27])
        sentinel2.relative_orbit = int(datatake_info.find("./SENSING_ORBIT_NUMBER").text)
        sentinel2.orbit_direction = datatake_info.find("./SENSING_ORBIT_DIRECTION").text.lower()
        sentinel2.processing_baseline = int(sentinel2.datatake_id[-5:].replace(".", ""))
        sentinel2.processing_facility = processing_info.find("./PROCESSING_CENTER").text

    def _analyze_mtd_tl(self, root, properties):
        ns = {"n1": "https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-2A_Tile_Metadata.xsd"}

        core = properties.core
        sentinel2 = properties.sentinel2
        general_info = root.find("./n1:General_Info", ns)
        tile_id = general_info.find("./TILE_ID").text
        core.validity_start = datetime.strptime(general_info.find("./SENSING_TIME").text, "%Y-%m-%dT%H:%M:%S.%fZ")
        core.creation_date = datetime.strptime(general_info.find("./Archiving_Info/ARCHIVING_TIME").text,
                                               "%Y-%m-%dT%H:%M:%S.%fZ")
        sentinel2.absolute_orbit = int(tile_id[42:48])
        sentinel2.tile_number = tile_id[50:55]
        sentinel2.processing_baseline = int(tile_id[-5:].replace(".", ""))
        sentinel2.processing_facility = tile_id[20:24]

        qi_info = root.find("./n1:Quality_Indicators_Info", ns)
        sentinel2.cloud_cover = float(qi_info.find("./Image_Content_QI/CLOUDY_PIXEL_PERCENTAGE").text)

    def analyze(self, paths, filename_only=False):
        inpath = paths[0]
        name_attrs = self.parse_filename(inpath)

        properties = Struct()

        core = properties.core = Struct()
        core.product_name = os.path.splitext(os.path.basename(inpath))[0]
        if self.zipped:
            core.product_name = os.path.splitext(core.product_name)[0]
        if 'validity_start' in name_attrs:
            core.validity_start = datetime.strptime(name_attrs['validity_start'], "%Y%m%dT%H%M%S")
        core.creation_date = datetime.strptime(name_attrs['creation_date'], "%Y%m%dT%H%M%S")

        sentinel2 = properties.sentinel2 = Struct()
        sentinel2.mission = name_attrs['mission']
        if sentinel2.mission[2] == "_":
            sentinel2.mission = sentinel2.mission[0:2]
        sentinel2.processing_facility = name_attrs['site_centre']
        sentinel2.processing_baseline = int(name_attrs['processing_baseline'].replace(".", ""))
        if 'absolute_orbit' in name_attrs:
            sentinel2.absolute_orbit = int(name_attrs['absolute_orbit'])
        if 'tile_number' in name_attrs:
            sentinel2.tile_number = name_attrs['tile_number']

        if not filename_only:
            # Update properties based on inventory metadata content
            if self.product_type.startswith("MSI_L1C"):
                self._analyze_inventory_metadata(self.read_xml_component(inpath, "Inventory_Metadata.xml"), properties)
            elif self.product_type == "MSI_L2A_DS":
                self._analyze_mtd_ds(self.read_xml_component(inpath, "MTD_DS.xml"), properties)
            elif self.product_type == "MSI_L2A_TL":
                self._analyze_mtd_tl(self.read_xml_component(inpath, "MTD_TL.xml"), properties)

        return properties


class EOFProduct(Sentinel2Product):

    def __init__(self, product_type, split=False, zipped=False, filename_base_pattern=None, ext="EOF"):
        self.product_type = product_type
        self.is_multi_file_product = split
        self.zipped = zipped
        self.xml_namespace = {}
        if filename_base_pattern is None:
            pattern = [
                r"(?P<mission>S2(_|A|B|C|D))",
                r"(?P<file_class>.{4})",
                r"(?P<product_type>%s)" % product_type,
                r"(?P<processing_facility>.{4})",
                r"(?P<creation_date>[\dT]{15})",
                r"V(?P<validity_start>[\dT]{15})",
                r"(?P<validity_stop>[\dT]{15})"
            ]
            self.filename_pattern = "_".join(pattern)
        else:
            self.filename_pattern = filename_base_pattern
        if self.is_multi_file_product:
            if self.zipped:
                self.filename_pattern += r"\.TGZ$"
        else:
            if self.zipped:
                self.filename_pattern += r"\." + ext + r"\.zip$"
            else:
                self.filename_pattern += r"\." + ext + r"$"

    @property
    def use_enclosing_directory(self):
        return self.is_multi_file_product and not self.zipped

    def enclosing_directory(self, properties):
        return properties.core.product_name

    def identify(self, paths):
        if self.is_multi_file_product and not self.zipped:
            if len(paths) != 2:
                return False
            paths = sorted(paths)
            if re.match(self.filename_pattern + r"\.DBL$", os.path.basename(paths[0])) is None:
                return False
            if re.match(self.filename_pattern + r"\.HDR$", os.path.basename(paths[1])) is None:
                return False
            return True
        else:
            if len(paths) != 1:
                return False
            return re.match(self.filename_pattern, os.path.basename(paths[0])) is not None

    def archive_path(self, properties):
        name_attrs = self.parse_filename(properties.core.physical_name)
        mission = name_attrs['mission']
        if mission[2] == "_":
            mission = mission[0:2]
        return os.path.join(
            mission,
            name_attrs['product_type'],
            name_attrs['validity_start'][0:4],
            name_attrs['validity_start'][4:6],
            name_attrs['validity_start'][6:8],
        )

    def read_xml_header(self, filepath):
        if self.is_multi_file_product:
            if self.zipped:
                hdrpath = os.path.splitext(os.path.basename(filepath))[0] + ".HDR"
                with tarfile.open(filepath, "r:gz") as tar:
                    return parse(tar.extractfile(hdrpath)).getroot()
            else:
                with open(filepath) as hdrfile:
                    return parse(hdrfile).getroot()
        else:
            ns = self.xml_namespace
            if self.zipped:
                with zipfile.ZipFile(filepath) as zproduct:
                    eofpath = os.path.splitext(os.path.basename(filepath))[0] + ".EOF"
                    with zproduct.open(eofpath) as eoffile:
                        return parse(eoffile).getroot().find("./Earth_Explorer_Header", ns)
            else:
                with open(filepath) as eoffile:
                    return parse(eoffile).getroot().find("./Earth_Explorer_Header", ns)

    def analyze(self, paths, filename_only=False):
        if self.is_multi_file_product and not self.zipped:
            name_attrs = self.parse_filename(os.path.splitext(os.path.basename(paths[0]))[0])
            inpath = sorted(paths)[-1]  # use the .HDR for metadata extraction
        else:
            inpath = paths[0]
            name_attrs = self.parse_filename(inpath)

        properties = Struct()

        core = properties.core = Struct()
        core.product_name = os.path.splitext(os.path.basename(inpath))[0]
        if 'creation_date' in name_attrs:
            core.creation_date = datetime.strptime(name_attrs['creation_date'], "%Y%m%dT%H%M%S")
        core.validity_start = datetime.strptime(name_attrs['validity_start'], "%Y%m%dT%H%M%S")
        if name_attrs['validity_stop'] == "99999999T999999":
            core.validity_stop = datetime.max
        else:
            core.validity_stop = datetime.strptime(name_attrs['validity_stop'], "%Y%m%dT%H%M%S")

        sentinel2 = properties.sentinel2 = Struct()
        sentinel2.mission = name_attrs['mission']
        if sentinel2.mission[2] == "_":
            sentinel2.mission = sentinel2.mission[0:2]
        if 'processing_facility' in name_attrs:
            sentinel2.processing_facility = name_attrs['processing_facility']

        if not filename_only:
            header = self.read_xml_header(inpath)
            ns = self.xml_namespace
            validity_start = header.find("./Fixed_Header/Validity_Period/Validity_Start", ns).text
            core.validity_start = datetime.strptime(validity_start, "UTC=%Y-%m-%dT%H:%M:%S")
            validity_stop = header.find("./Fixed_Header/Validity_Period/Validity_Stop", ns).text
            if validity_stop == "UTC=9999-99-99T99:99:99":
                core.validity_stop = datetime.max
            else:
                core.validity_stop = datetime.strptime(validity_stop, "UTC=%Y-%m-%dT%H:%M:%S")
            creation_date = header.find("./Fixed_Header/Source/Creation_Date", ns).text
            core.creation_date = datetime.strptime(creation_date, "UTC=%Y-%m-%dT%H:%M:%S")
            sentinel2.processing_facility = header.find("./Fixed_Header/Source/System", ns).text
            sentinel2.processor_name = header.find("./Fixed_Header/Source/Creator", ns).text
            sentinel2.processor_version = header.find("./Fixed_Header/Source/Creator_Version", ns).text

        return properties


class GIPPProduct(EOFProduct):
    def __init__(self, product_type, zipped=False):
        pattern = [
            r"(?P<mission>S2(_|A|B|C|D))",
            r"(?P<file_class>.{4})",
            r"(?P<product_type>%s)" % product_type,
            r"(?P<processing_facility>.{4})",
            r"(?P<creation_date>[\dT]{15})",
            r"V(?P<validity_start>[\dT]{15})",
            r"(?P<validity_stop>[\dT]{15})",
            r"B(?P<band>(00|01|02|03|04|05|06|07|08|8A|09|10|11|12))"
        ]
        super().__init__(product_type, split=True, zipped=zipped, filename_base_pattern="_".join(pattern))
        self.xml_namespace = {"": "http://eop-cfi.esa.int/S2/S2_SCHEMAS"}


class IERSProduct(EOFProduct):
    def __init__(self, product_type, zipped=False):
        super().__init__(product_type, zipped=zipped, ext="txt")

    def analyze(self, paths, filename_only=False):
        return super().analyze(paths, filename_only=True)


_product_types = dict(
    [(product_type, SAFEProduct(product_type)) for product_type in USER_PRODUCT_TYPES] +
    [(product_type, PDIProduct(product_type)) for product_type in PDI_PRODUCT_TYPES] +
    [(product_type, EOFProduct(product_type)) for product_type in AUX_EOF_PRODUCT_TYPES] +
    [(product_type, EOFProduct(product_type, split=True)) for product_type in AUX_HDR_DBL_PRODUCT_TYPES] +
    [(product_type, GIPPProduct(product_type)) for product_type in GIPP_PRODUCT_TYPES] +
    [(product_type, IERSProduct(product_type)) for product_type in IERS_PRODUCT_TYPES]
)


def product_types():
    return _product_types.keys()


def product_type_plugin(product_type):
    return _product_types.get(product_type)
