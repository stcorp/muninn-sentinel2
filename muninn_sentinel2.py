import os
import re
import tarfile
import zipfile
from datetime import datetime
from xml.etree.ElementTree import parse

from muninn.schema import Mapping, Text, Integer, Real
from muninn.geometry import Point, LinearRing, Polygon
from muninn.struct import Struct
from muninn.util import copy_path


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

# Not every GIPP HDR file uses the xmlns namespace, so we need to keep track of which product type does
GIPP_PRODUCT_TYPES = {
    'GIP_ATMIMA': True,
    'GIP_ATMSAD': True,
    'GIP_BLINDP': True,
    'GIP_CLOINV': True,
    'GIP_CLOPAR': True,
    'GIP_CONVER': False,
    'GIP_DATATI': True,
    'GIP_DECOMP': False,
    'GIP_EARMOD': True,
    'GIP_ECMWFP': False,
    'GIP_G2PARA': True,
    'GIP_G2PARE': True,
    'GIP_GEOPAR': True,
    'GIP_INTDET': True,
    'GIP_INVLOC': True,
    'GIP_JP2KPA': False,
    'GIP_L2ACAC': True,
    'GIP_L2ACSC': True,
    'GIP_LREXTR': True,
    'GIP_MASPAR': True,
    'GIP_OLQCPA': True,
    'GIP_PRDLOC': True,
    'GIP_PROBA2': False,
    'GIP_PROBAS': False,
    'GIP_R2ABCA': False,
    'GIP_R2BINN': True,
    'GIP_R2CRCO': True,
    'GIP_R2DECT': True,
    'GIP_R2DEFI': True,
    'GIP_R2DENT': True,
    'GIP_R2DEPI': True,
    'GIP_R2EOB2': False,
    'GIP_R2EQOG': False,
    'GIP_R2L2NC': True,
    'GIP_R2NOMO': True,
    'GIP_R2PARA': True,
    'GIP_R2SWIR': False,
    'GIP_R2WAFI': True,
    'GIP_RESPAR': True,
    'GIP_SPAMOD': True,
    'GIP_TILPAR': True,
    'GIP_VIEDIR': True,
}

IERS_PRODUCT_TYPES = [
    'AUX_UT1UTC',
]


def package_tar(paths, target_filepath, compression=None):
    mode = "w"
    if compression is not None:
        mode += ":" + compression
    with tarfile.open(target_filepath, mode) as archive:
        for path in paths:
            rootlen = len(os.path.dirname(path)) + 1
            archive.add(path, path[rootlen:])


def package_zip(paths, target_filepath):
    with zipfile.ZipFile(target_filepath, "x", zipfile.ZIP_DEFLATED, compresslevel=1) as archive:
        for path in paths:
            rootlen = len(os.path.dirname(path)) + 1
            if os.path.isdir(path):
                for base, dirs, files in os.walk(path):
                    for file in files:
                        fn = os.path.join(base, file)
                        archive.write(fn, fn[rootlen:])
            else:
                archive.write(path, path[rootlen:])


class Sentinel2Product(object):

    def __init__(self, product_type):
        self.product_type = product_type
        self.is_multi_file_product = False
        self.filename_pattern = None
        self.packaged = False
        self.package_format = None

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

    def archive_path(self, properties):
        validity_start = properties.core.validity_start.strftime("%Y%m%d")
        return os.path.join(
            properties.sentinel2.mission,
            self.product_type,
            validity_start[0:4],
            validity_start[4:6],
            validity_start[6:8],
        )

    def read_xml_component(self, filepath, componentpath):
        if self.packaged:
            if not self.is_multi_file_product:
                componentpath = os.path.join(os.path.splitext(os.path.basename(filepath))[0], componentpath)
            if self.package_format == "zip":
                with zipfile.ZipFile(filepath) as zproduct:
                    with zproduct.open(componentpath) as manifest:
                        return parse(manifest).getroot()
            elif self.package_format == "tar":
                with tarfile.open(filepath, "r") as tproduct:
                    with tproduct.extractfile(componentpath) as manifest:
                        return parse(manifest).getroot()
            else:
                raise Exception("Unsupported package format '%s'" % self.package_format)
        else:
            with open(os.path.join(filepath, componentpath)) as manifest:
                return parse(manifest).getroot()


class SAFEProduct(Sentinel2Product):

    def __init__(self, product_type, packaged=False):
        self.product_type = product_type
        self.packaged = packaged
        self.package_format = "zip"
        pattern = [
            r"^(?P<mission>S2(_|A|B|C|D))",
            r"(?P<product_type>%s)" % product_type,
            r"(?P<validity_start>[\dT]{15})",
            r"N(?P<processing_baseline>[\d]{4})",
            r"R(?P<relative_orbit>[\d]{3})",
            r"T(?P<tile_number>.{5})",
            r"(?P<creation_date>[\dT]{15})",
        ]
        if packaged:
            self.filename_pattern = "_".join(pattern) + r"\.SAFE\.zip$"
        else:
            self.filename_pattern = "_".join(pattern) + r"\.SAFE$"

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
        if self.packaged:
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

    def export_zip(self, archive, properties, target_path, paths):
        if self.packaged:
            assert len(paths) == 1, "zipped product should be a single file"
            copy_path(paths[0], target_path)
            return os.path.join(target_path, os.path.basename(paths[0]))
        target_filepath = os.path.join(os.path.abspath(target_path), properties.core.physical_name + ".zip")
        package_zip(paths, target_filepath)
        return target_filepath


class PDIProduct(Sentinel2Product):

    def __init__(self, product_type, packaged=False):
        self.product_type = product_type
        self.is_multi_file_product = False
        self.packaged = packaged
        self.package_format = "tar"
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
        if packaged:
            self.filename_pattern = "_".join(pattern) + r"\.tar$"
        else:
            self.filename_pattern = "_".join(pattern) + r"$"

    def _analyze_inventory_metadata(self, root, properties):
        ns = {"": "https://psd-12.sentinel2.eo.esa.int/PSD/Inventory_Metadata.xsd"}

        core = properties.core
        sentinel2 = properties.sentinel2
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

    def _analyze_mtd_ds_l1(self, root, properties):
        ns = {"n1": "https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-1C_Datastrip_Metadata.xsd"}

        core = properties.core
        sentinel2 = properties.sentinel2
        general_info = root.find("./n1:General_Info", ns)
        datatake_info = general_info.find("./Datatake_Info")
        datastrip_info = general_info.find("./Datastrip_Time_Info")
        core.validity_start = datetime.strptime(datastrip_info.find("./DATASTRIP_SENSING_START").text,
                                                "%Y-%m-%dT%H:%M:%S.%fZ")
        core.validity_stop = datetime.strptime(datastrip_info.find("./DATASTRIP_SENSING_STOP").text,
                                               "%Y-%m-%dT%H:%M:%S.%fZ")
        core.creation_date = datetime.strptime(general_info.find("./Archiving_Info/ARCHIVING_TIME").text,
                                               "%Y-%m-%dT%H:%M:%S.%fZ")
        sentinel2.relative_orbit = int(datatake_info.find("./SENSING_ORBIT_NUMBER").text)

    def _analyze_mtd_tl_l1(self, root, properties):
        ns = {"n1": "https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-1C_Tile_Metadata.xsd"}

        core = properties.core
        sentinel2 = properties.sentinel2
        general_info = root.find("./n1:General_Info", ns)
        tile_id = general_info.find("./TILE_ID").text
        core.validity_start = datetime.strptime(general_info.find("./SENSING_TIME").text, "%Y-%m-%dT%H:%M:%S.%fZ")
        core.validity_stop = core.validity_start
        core.creation_date = datetime.strptime(general_info.find("./Archiving_Info/ARCHIVING_TIME").text,
                                               "%Y-%m-%dT%H:%M:%S.%fZ")
        sentinel2.tile_number = tile_id[50:55]

        qi_info = root.find("./n1:Quality_Indicators_Info", ns)
        sentinel2.cloud_cover = float(qi_info.find("./Image_Content_QI/CLOUDY_PIXEL_PERCENTAGE").text)

    def _analyze_mtd_ds_l2(self, root, properties):
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

    def _analyze_mtd_tl_l2(self, root, properties):
        ns = {"n1": "https://psd-14.sentinel2.eo.esa.int/PSD/S2_PDI_Level-2A_Tile_Metadata.xsd"}

        core = properties.core
        sentinel2 = properties.sentinel2
        general_info = root.find("./n1:General_Info", ns)
        tile_id = general_info.find("./TILE_ID").text
        core.validity_start = datetime.strptime(general_info.find("./SENSING_TIME").text, "%Y-%m-%dT%H:%M:%S.%fZ")
        core.validity_stop = core.validity_start
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
        core.product_name = os.path.basename(inpath)
        if self.packaged:
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
                metadata_filename = core.product_name[:9]+"MTD"+core.product_name[12:-7]+".xml"
                self._analyze_inventory_metadata(self.read_xml_component(inpath, "Inventory_Metadata.xml"), properties)
                if self.product_type.endswith("DS"):
                    self._analyze_mtd_ds_l1(self.read_xml_component(inpath,metadata_filename), properties)
                elif self.product_type.endswith("TL"):
                    self._analyze_mtd_tl_l1(self.read_xml_component(inpath,metadata_filename), properties)
            elif self.product_type == "MSI_L2A_DS":
                self._analyze_mtd_ds_l2(self.read_xml_component(inpath, "MTD_DS.xml"), properties)
            elif self.product_type == "MSI_L2A_TL":
                self._analyze_mtd_tl_l2(self.read_xml_component(inpath, "MTD_TL.xml"), properties)

        return properties

    def export_tar(self, archive, properties, target_path, paths):
        if self.packaged:
            assert len(paths) == 1, "tarred product should be a single file"
            copy_path(paths[0], target_path)
            return os.path.join(target_path, os.path.basename(paths[0]))
        target_filepath = os.path.join(os.path.abspath(target_path), properties.core.physical_name + ".tar")
        package_tar(paths, target_filepath)
        return target_filepath


class EOFProduct(Sentinel2Product):

    def __init__(self, product_type, split=False, packaged=False, filename_base_pattern=None, ext="EOF"):
        self.product_type = product_type
        self.is_multi_file_product = split
        self.packaged = packaged
        if self.is_multi_file_product:
            self.package_format = "tgz"
        else:
            self.package_format = "zip"
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
            if self.packaged:
                self.filename_pattern += r"\.TGZ$"
        else:
            if self.packaged:
                self.filename_pattern += r"\." + ext + r"\.zip$"
            else:
                self.filename_pattern += r"\." + ext + r"$"

    @property
    def use_enclosing_directory(self):
        return self.is_multi_file_product and not self.packaged

    def enclosing_directory(self, properties):
        return properties.core.product_name

    def identify(self, paths):
        if self.is_multi_file_product and not self.packaged:
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

    def read_xml_header(self, filepath):
        if self.is_multi_file_product:
            if self.packaged:
                hdrpath = os.path.splitext(os.path.basename(filepath))[0] + ".HDR"
                with tarfile.open(filepath, "r:gz") as tar:
                    return parse(tar.extractfile(hdrpath)).getroot()
            else:
                with open(filepath) as hdrfile:
                    return parse(hdrfile).getroot()
        else:
            ns = self.xml_namespace
            if self.packaged:
                with zipfile.ZipFile(filepath) as zproduct:
                    eofpath = os.path.splitext(os.path.basename(filepath))[0] + ".EOF"
                    with zproduct.open(eofpath) as eoffile:
                        return parse(eoffile).getroot().find("./Earth_Explorer_Header", ns)
            else:
                with open(filepath) as eoffile:
                    return parse(eoffile).getroot().find("./Earth_Explorer_Header", ns)

    def analyze(self, paths, filename_only=False):
        if self.is_multi_file_product and not self.packaged:
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

    def export_tgz(self, archive, properties, target_path, paths):
        if self.packaged and self.package_format == "tgz":
            assert len(paths) == 1, "tarred product should be a single file"
            copy_path(paths[0], target_path)
            return os.path.join(target_path, os.path.basename(paths[0]))
        target_filepath = os.path.join(os.path.abspath(target_path), properties.core.physical_name + ".TGZ")
        package_tar(paths, target_filepath, compression="gz")
        return target_filepath

    def export_zip(self, archive, properties, target_path, paths):
        if self.packaged and self.package_format == "zip":
            assert len(paths) == 1, "zipped product should be a single file"
            copy_path(paths[0], target_path)
            return os.path.join(target_path, os.path.basename(paths[0]))
        target_filepath = os.path.join(os.path.abspath(target_path), properties.core.physical_name + ".zip")
        package_zip(paths, target_filepath)
        return target_filepath


class GIPPProduct(EOFProduct):
    def __init__(self, product_type, has_xmlns, packaged=False):
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
        super().__init__(product_type, split=True, packaged=packaged, filename_base_pattern="_".join(pattern))
        if has_xmlns:
            self.xml_namespace = {"": "http://eop-cfi.esa.int/S2/S2_SCHEMAS"}


class IERSProduct(EOFProduct):
    def __init__(self, product_type, packaged=False):
        super().__init__(product_type, packaged=packaged, ext="txt")

    def analyze(self, paths, filename_only=False):
        return super().analyze(paths, filename_only=True)


_product_types = dict(
    [(product_type, SAFEProduct(product_type)) for product_type in USER_PRODUCT_TYPES] +
    [(product_type, PDIProduct(product_type)) for product_type in PDI_PRODUCT_TYPES] +
    [(product_type, EOFProduct(product_type)) for product_type in AUX_EOF_PRODUCT_TYPES] +
    [(product_type, EOFProduct(product_type, split=True)) for product_type in AUX_HDR_DBL_PRODUCT_TYPES] +
    [(product_type, GIPPProduct(product_type, has_xmlns)) for product_type, has_xmlns in GIPP_PRODUCT_TYPES.items()] +
    [(product_type, IERSProduct(product_type)) for product_type in IERS_PRODUCT_TYPES]
)


def product_types():
    return _product_types.keys()


def product_type_plugin(product_type):
    return _product_types.get(product_type)
