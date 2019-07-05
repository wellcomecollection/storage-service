import os
import logging
import mappings
import image_size
from xml_help import make_child, remove_first_child, namespaces


def add_premis_identifier(premis_file, p_type, value):
    oid = make_child(premis_file, "premis", "objectIdentifier")
    p_type_el = make_child(oid, "premis", "objectIdentifierType")
    p_type_el.text = p_type
    value_el = make_child(oid, "premis", "objectIdentifierValue")
    value_el.text = value


def add_premis_significant_prop(premis_file, p_type, value):
    oid = make_child(premis_file, "premis", "significantProperties")
    p_type_el = make_child(oid, "premis", "significantPropertiesType")
    p_type_el.text = p_type
    value_el = make_child(oid, "premis", "significantPropertiesValue")
    value_el.text = value


class UnrecognisedPRONOMFormatError(KeyError):
    pass


def make_premis_file_element(parent):
    premis_file = make_child(parent, "premis", "object")
    premis_file.set("xsi:type", "premis:file")
    # These v3 schemas conflict with the v2 asserted in the existing METS, but fix that later
    premis_file.set(
        "xsi:schemaLocation",
        "http://www.loc.gov/premis/v3 http://www.loc.gov/standards/premis/v3/premis.xsd",
    )
    premis_file.set("version", "3.0")
    return premis_file


def remodel_file_technical_metadata(root, id_map):
    logging.debug("transforming Tessella techMD")
    x_path = ".//mets:xmlData[tessella:File]"
    tessella_file_xmldata = root.findall(x_path, namespaces)
    for xmldata in tessella_file_xmldata:
        tessella_file = xmldata[0]
        premis_file = make_premis_file_element(xmldata)

        file_name = tessella_file.find("tessella:FileName", namespaces).text
        add_premis_identifier(premis_file, "local", file_name)
        uuid = tessella_file.find("tessella:ID", namespaces).text
        add_premis_identifier(premis_file, "uuid", uuid)
        id_map[uuid] = file_name

        file_properties = tessella_file.findall("tessella:FileProperty", namespaces)
        for file_property in file_properties:
            name = file_property.find(
                "tessella:FilePropertyName", namespaces
            ).text.strip()

            try:
                premis_name = mappings.SIGNIFICANT_PROPERTIES[name]
                value = file_property.find("tessella:Value", namespaces).text.strip()
                add_premis_significant_prop(premis_file, premis_name, value)
            except KeyError:
                if name in mappings.IGNORED_PROPERTIES:
                    logging.debug("Ignoring property name %s", name)
                else:
                    message = "Unknown file property: {0}".format(name)
                    raise ValueError(message)

        characteristics = make_child(premis_file, "premis", "objectCharacteristics")
        composition_level = make_child(characteristics, "premis", "compositionLevel")
        composition_level.text = 0  # ??
        fixity = make_child(characteristics, "premis", "fixity")
        algorithm = make_child(fixity, "premis", "messageDigestAlgorithm")
        algorithm_ref = tessella_file.find(
            "tessella:ChecksumAlgorithmRef", namespaces
        ).text
        algorithm.text = mappings.CHECKSUM_ALGORITHMS[algorithm_ref]
        message_digest = make_child(fixity, "premis", "messageDigest")
        message_digest.text = tessella_file.find("tessella:Checksum", namespaces).text
        file_size = make_child(characteristics, "premis", "size")
        file_size.text = tessella_file.find("tessella:FileSize", namespaces).text
        p_format = make_child(characteristics, "premis", "format")
        format_designation = make_child(p_format, "premis", "formatDesignation")
        format_name = make_child(format_designation, "premis", "formatName")
        format_name.text = tessella_file.find("tessella:FormatName", namespaces).text
        format_registry = make_child(p_format, "premis", "formatRegistry")
        format_registry_name = make_child(
            format_registry, "premis", "formatRegistryName"
        )
        format_registry_name.text = "PRONOM"  # assume this is always used
        format_registry_key = make_child(format_registry, "premis", "formatRegistryKey")

        try:
            format_registry_key.text = mappings.PRONOM[format_name.text]
        except KeyError:
            raise UnrecognisedPRONOMFormatError(format_name.text)

        remove_first_child(xmldata)


def append_poster(root, destination):
    # Add this in as an additional file in technical metadata, but not in
    # structMaps (yet), as that would be just inventing something that needs to
    # go in Archivematica

    # data we are interested in
    head, tail = os.path.split(destination)
    file_name = tail
    file_size = os.path.getsize(destination)
    width, height = image_size.get_image_size(destination)

    amds = root.find("./mets:amdSec[@ID='AMD']", namespaces)
    tech_md = make_child(amds, "mets", "techMD")
    tech_md.set("ID", "AMD_POSTER")
    md_wrap = make_child(tech_md, "mets", "mdWrap")
    md_wrap.set("MDTYPE", "OTHER")
    md_wrap.set("MIMETYPE", "text/xml")
    xmldata = make_child(md_wrap, "mets", "xmlData")
    premis_file = make_premis_file_element(xmldata)
    add_premis_identifier(premis_file, "local", file_name)
    add_premis_significant_prop(premis_file, "ImageWidth", str(width))
    add_premis_significant_prop(premis_file, "ImageHeight", str(height))
    characteristics = make_child(premis_file, "premis", "objectCharacteristics")
    file_size_el = make_child(characteristics, "premis", "size")
    file_size_el.text = str(file_size)
    p_format = make_child(characteristics, "premis", "format")
    format_designation = make_child(p_format, "premis", "formatDesignation")
    format_name = make_child(format_designation, "premis", "formatName")
    format_name.text = "JPEG Format"
    format_registry = make_child(p_format, "premis", "formatRegistry")
    format_registry_name = make_child(
        format_registry, "premis", "formatRegistryName"
    )
    format_registry_name.text = "PRONOM"
    format_registry_key = make_child(format_registry, "premis", "formatRegistryKey")
    format_registry_key.text = "fmt/44"
