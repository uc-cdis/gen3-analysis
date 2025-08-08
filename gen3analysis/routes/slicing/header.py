import io
import re
import struct

from . import exceptions, gzip


def deserialize(fmt, f):
    return struct.unpack(fmt, f.read(struct.calcsize(fmt)))[0]


def serialize(fmt, *vals):
    return struct.pack(fmt, *vals)


def bytes2header(b):
    """
    Parses and returns a dictionary representation of a SAM header byte buffer.
    """
    ret = {
        "HD": {},
        "SQ": [],
        "RG": [],
        "PG": [],
        "CO": [],
    }

    CODE_REGEX = r"^@(?P<code>[A-Z][A-Z])(?P<rest>\t.*)"
    code_regex = re.compile(CODE_REGEX)

    TAG_REGEX = r"\t(?P<tag>[A-Za-z][A-Za-z0-9]):(?P<val>[^\t]+)"
    tag_regex = re.compile(TAG_REGEX)

    TAGS_REGEX = rf"^(?:{TAG_REGEX})+$"
    tags_regex = re.compile(TAGS_REGEX)

    header = b.decode("us-ascii")

    for i, line in enumerate(header.strip("\n").split("\n")):

        match = code_regex.match(line)
        if not match:
            raise exceptions.FormatError("malformed code on line %d" % i)

        code = match.group("code")
        rest = match.group("rest")

        if code not in ret:
            raise exceptions.FormatError("unknown code on line %d" % i)

        if code == "HD" and i:
            raise exceptions.FormatError("header code found on line %d" % i)

        if code == "CO":
            ret[code].append(rest[1:])
            continue

        if tags_regex.match(rest) is None:
            raise exceptions.FormatError("malformed tags on line %d" % i)

        record = {}
        for match in tag_regex.finditer(rest):
            tag = match.group("tag")
            val = match.group("val")

            record[tag] = val

        if code == "HD":
            ret[code] = record
        else:
            ret[code].append(record)

    return ret


def bgzf2dict(bam):
    """
    Deserialize BGZF compressed BAM header  from file-like object.
    """
    inflater = gzip.GzipFile(fileobj=bam)
    return file2dict(inflater)


def file2dict(bam):
    """
    Deserialize BAM header from file-like object.
    """
    if bam.read(4) != b"BAM\x01":
        raise exceptions.FormatError("magic number not found")

    try:
        header_length = deserialize("<I", bam)
    except struct.error:
        raise exceptions.TruncatedError("unexpected EOF encountered")

    # NOTE We preload the whole BAM header into memory since we'll be
    # representing the whole thing in memory anyways, and we don't necessarily
    # know if the file-like object is performant under small read sizes.
    buffered = bam.read(header_length)
    if len(buffered) < header_length:
        raise exceptions.TruncatedError("unexpected EOF encountered")

    header_dict = bytes2header(buffered)

    # NOTE We're discarding the extra reference information, as it's encoded in
    # the SAM header that we just parsed. No need to re-read the same info, and
    # having two places with potentially conflicting information isn't great...
    remainder = (
        4 + (8 * len(header_dict["SQ"])) + sum(len(sq["SN"]) + 1 for sq in header_dict["SQ"])
    )
    bam.read(remainder)

    return header_dict


def header2bytes(header):
    """
    Parses and returns a byte buffer of a SAM header dictionary representation.
    """
    sam_buff = io.BytesIO()
    bam_buff = io.BytesIO()

    # TODO FIXME clean and generalize this

    hd = "@HD\t"
    hd += "\t".join([":".join(pair) for pair in header["HD"].items()])
    hd += "\n"

    sam_buff.write(hd.encode("us-ascii"))

    for record in header.get("SQ", []):
        name = record.pop("SN")

        sq = "@SQ\t"
        sq += f"SN:{name}\t"
        sq += "\t".join([":".join(pair) for pair in record.items()])
        sq += "\n"
        sam_buff.write(sq.encode("us-ascii"))

        record["SN"] = name

    for record in header.get("RG", []):
        rg = "@RG\t"
        rg += "ID:{}\t".format(record.pop("ID"))
        rg += "\t".join([":".join(pair) for pair in record.items()])
        rg += "\n"
        sam_buff.write(rg.encode("us-ascii"))

    for record in header.get("PG", []):
        pg = "@PG\t"
        pg += "ID:{}\t".format(record.pop("ID"))
        pg += "\t".join([":".join(pair) for pair in record.items()])
        pg += "\n"
        sam_buff.write(pg.encode("us-ascii"))

    for record in header.get("CO", []):
        co = "@CO\t"
        co += record
        co += "\n"
        sam_buff.write(co.encode("us-ascii"))

    bam_buff.write("BAM\x01".encode("us-ascii"))
    bam_buff.write(serialize("<I", len(sam_buff.getvalue())))
    bam_buff.write(sam_buff.getvalue())

    bam_buff.write(serialize("<I", len(header.get("SQ", []))))
    for ref in header.get("SQ", []):
        bam_buff.write(serialize("<I", len(ref["SN"]) + 1))
        bam_buff.write((ref["SN"] + "\x00").encode("us-ascii"))
        bam_buff.write(serialize("<I", int(ref["LN"])))

    return bam_buff.getvalue()
