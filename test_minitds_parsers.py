#!/usr/bin/env python3
##############################################################################
# Unit tests for minitds TDS parser functions.
# These tests do NOT require a live SQL Server connection.
##############################################################################
import struct
import unittest

from minitds.minitds import (
    _parse_plp,
    _parse_column,
    _parse_description_type,
    BIGBINARYTYPE,
    FLT4TYPE,
    JSONTYPE,
    NTEXTTYPE,
    UDTTYPE,
    VECTORTYPE,
    VECTOR_ELEM_FLOAT32,
    XMLTYPE,
)

# ------------------------------------------------------------------
# Header helpers
# ------------------------------------------------------------------
# _parse_description_type consumes:
#   4-byte user_type (uint) + 2-byte flags (null_ok = flags & 1) + 1-byte type_id
# then type-specific metadata, then a B_VARCHAR column name (1-byte char count + UTF-16LE).

_HEADER_NULLABLE    = b'\x00\x00\x00\x00\x01\x00'   # user_type=0, flags=1 (nullable)
_HEADER_NOT_NULL    = b'\x00\x00\x00\x00\x00\x00'   # user_type=0, flags=0


def _name_bvarchar(s):
    """Encode string s as B_VARCHAR (1-byte char count + UTF-16LE)."""
    encoded = s.encode('utf-16-le')
    return bytes([len(s)]) + encoded


def _plp_bytes(payload: bytes) -> bytes:
    """Wrap raw bytes in a single-chunk PLP encoding."""
    total_len = len(payload)
    return (
        struct.pack('<Q', total_len)        # total length (8 bytes)
        + struct.pack('<I', len(payload))   # chunk length (4 bytes)
        + payload
        + struct.pack('<I', 0)              # PLP terminator
    )


_PLP_NULL = struct.pack('<Q', 0xFFFFFFFFFFFFFFFF)
_PLP_UNKNOWN_HEADER = struct.pack('<Q', 0xFFFFFFFFFFFFFFFE)


class TestParsePlp(unittest.TestCase):

    def test_null(self):
        v, remaining = _parse_plp(_PLP_NULL + b'\xAA')
        self.assertIsNone(v)
        self.assertEqual(remaining, b'\xAA')

    def test_empty_payload(self):
        data = struct.pack('<Q', 0) + struct.pack('<I', 0)  # total=0, terminator
        v, remaining = _parse_plp(data + b'\xAA')
        self.assertEqual(v, b'')
        self.assertEqual(remaining, b'\xAA')

    def test_single_chunk(self):
        payload = b'hello'
        v, remaining = _parse_plp(_plp_bytes(payload) + b'\xAA')
        self.assertEqual(v, payload)
        self.assertEqual(remaining, b'\xAA')

    def test_multi_chunk(self):
        chunk1 = b'foo'
        chunk2 = b'bar'
        total = len(chunk1) + len(chunk2)
        data = (
            struct.pack('<Q', total)
            + struct.pack('<I', len(chunk1)) + chunk1
            + struct.pack('<I', len(chunk2)) + chunk2
            + struct.pack('<I', 0)           # terminator
        )
        v, remaining = _parse_plp(data + b'\xAA')
        self.assertEqual(v, b'foobar')
        self.assertEqual(remaining, b'\xAA')

    def test_plp_unknown_single_chunk(self):
        """PLP_UNKNOWN total length is treated as a normal stream; chunks until 0-terminator."""
        payload = b'world'
        data = (
            _PLP_UNKNOWN_HEADER
            + struct.pack('<I', len(payload)) + payload
            + struct.pack('<I', 0)
        )
        v, remaining = _parse_plp(data + b'\xBB')
        self.assertEqual(v, payload)
        self.assertEqual(remaining, b'\xBB')


class TestParseColumnFlt4(unittest.TestCase):

    def _col(self, raw):
        return _parse_column('col', FLT4TYPE, 4, -1, -1, 'utf-8', raw)

    def test_positive(self):
        v, remaining = self._col(struct.pack('<f', 0.25) + b'\xAA')
        self.assertEqual(v, 0.25)
        self.assertEqual(remaining, b'\xAA')

    def test_negative(self):
        v, _ = self._col(struct.pack('<f', -1.5))
        self.assertEqual(v, -1.5)

    def test_zero(self):
        v, _ = self._col(struct.pack('<f', 0.0))
        self.assertEqual(v, 0.0)


class TestParseColumnNtext(unittest.TestCase):

    def _col(self, raw):
        return _parse_column('col', NTEXTTYPE, 16, -1, -1, 'utf-8', raw)

    def test_null(self):
        v, remaining = self._col(b'\x00' + b'\xAA')
        self.assertIsNone(v)
        self.assertEqual(remaining, b'\xAA')

    def test_value(self):
        text = 'hello'.encode('utf-16-le')
        textptr_len = 16
        data = (
            bytes([textptr_len])
            + b'\x00' * textptr_len    # text pointer
            + b'\x00' * 8              # timestamp
            + struct.pack('<i', len(text))
            + text
            + b'\xAA'
        )
        v, remaining = self._col(data)
        self.assertEqual(v, 'hello')
        self.assertEqual(remaining, b'\xAA')

    def test_unicode_value(self):
        text = '日本語'.encode('utf-16-le')
        textptr_len = 16
        data = (
            bytes([textptr_len])
            + b'\x00' * textptr_len
            + b'\x00' * 8
            + struct.pack('<i', len(text))
            + text
        )
        v, _ = self._col(data)
        self.assertEqual(v, '日本語')


class TestParseColumnBigBinary(unittest.TestCase):

    def _col(self, raw, size=10):
        return _parse_column('col', BIGBINARYTYPE, size, -1, -1, 'utf-8', raw)

    def test_null(self):
        v, remaining = self._col(struct.pack('<h', -1) + b'\xAA')
        self.assertIsNone(v)
        self.assertEqual(remaining, b'\xAA')

    def test_value(self):
        payload = b'\x01\x02\x03'
        v, remaining = self._col(struct.pack('<h', len(payload)) + payload + b'\xAA')
        self.assertEqual(v, payload)
        self.assertEqual(remaining, b'\xAA')

    def test_empty(self):
        v, _ = self._col(struct.pack('<h', 0))
        self.assertEqual(v, b'')


class TestParseColumnXml(unittest.TestCase):

    def _col(self, raw):
        return _parse_column('col', XMLTYPE, -1, -1, -1, 'utf-8', raw)

    def test_null(self):
        v, remaining = self._col(_PLP_NULL + b'\xAA')
        self.assertIsNone(v)
        self.assertEqual(remaining, b'\xAA')

    def test_simple_xml(self):
        xml = '<root/>'
        payload = xml.encode('utf-16-le')
        v, remaining = self._col(_plp_bytes(payload) + b'\xAA')
        self.assertEqual(v, xml)
        self.assertEqual(remaining, b'\xAA')

    def test_xml_with_content(self):
        xml = '<items><item id="1">hello</item></items>'
        payload = xml.encode('utf-16-le')
        v, _ = self._col(_plp_bytes(payload))
        self.assertEqual(v, xml)


class TestParseColumnJson(unittest.TestCase):

    def _col(self, raw):
        return _parse_column('col', JSONTYPE, -1, -1, -1, 'utf-8', raw)

    def test_null(self):
        v, remaining = self._col(_PLP_NULL + b'\xAA')
        self.assertIsNone(v)
        self.assertEqual(remaining, b'\xAA')

    def test_object(self):
        json_str = '{"key": "value", "n": 42}'
        payload = json_str.encode('utf-8')
        v, remaining = self._col(_plp_bytes(payload) + b'\xAA')
        self.assertEqual(v, json_str)
        self.assertEqual(remaining, b'\xAA')

    def test_array(self):
        json_str = '[1, 2, 3]'
        payload = json_str.encode('utf-8')
        v, _ = self._col(_plp_bytes(payload))
        self.assertEqual(v, json_str)

    def test_unicode_json(self):
        json_str = '{"名前": "太郎"}'
        payload = json_str.encode('utf-8')
        v, _ = self._col(_plp_bytes(payload))
        self.assertEqual(v, json_str)


class TestParseColumnUdt(unittest.TestCase):

    def _col(self, raw):
        return _parse_column('col', UDTTYPE, -1, -1, -1, 'utf-8', raw)

    def test_null(self):
        v, remaining = self._col(_PLP_NULL + b'\xAA')
        self.assertIsNone(v)
        self.assertEqual(remaining, b'\xAA')

    def test_raw_bytes(self):
        payload = b'\xDE\xAD\xBE\xEF\x01\x02'
        v, remaining = self._col(_plp_bytes(payload) + b'\xAA')
        self.assertEqual(v, payload)
        self.assertEqual(remaining, b'\xAA')


class TestParseColumnVector(unittest.TestCase):

    def _col(self, raw, size=12):
        return _parse_column('col', VECTORTYPE, size, -1, VECTOR_ELEM_FLOAT32, 'utf-8', raw)

    def test_null(self):
        v, remaining = self._col(struct.pack('<h', -1) + b'\xAA')
        self.assertIsNone(v)
        self.assertEqual(remaining, b'\xAA')

    def test_float32_vector(self):
        floats = [1.0, 2.0, 3.0]
        raw = struct.pack('<3f', *floats)
        data = struct.pack('<h', len(raw)) + raw + b'\xAA'
        v, remaining = self._col(data, size=len(raw))
        self.assertEqual(len(v), 3)
        self.assertAlmostEqual(v[0], 1.0)
        self.assertAlmostEqual(v[1], 2.0)
        self.assertAlmostEqual(v[2], 3.0)
        self.assertEqual(remaining, b'\xAA')

    def test_single_element(self):
        raw = struct.pack('<f', 0.5)
        data = struct.pack('<h', len(raw)) + raw
        v, _ = self._col(data, size=len(raw))
        self.assertEqual(len(v), 1)
        self.assertAlmostEqual(v[0], 0.5)

    def test_empty_vector(self):
        data = struct.pack('<h', 0)
        v, _ = self._col(data, size=0)
        self.assertEqual(v, [])


class TestParseDescriptionType(unittest.TestCase):
    """Tests for _parse_description_type with crafted COLMETADATA bytes."""

    def _make_header(self, type_id_byte, nullable=True):
        flags = b'\x01\x00' if nullable else b'\x00\x00'
        return b'\x00\x00\x00\x00' + flags + bytes([type_id_byte])

    def test_flt4type(self):
        data = self._make_header(FLT4TYPE) + _name_bvarchar('x')
        type_id, name, size, precision, scale, null_ok, remaining = _parse_description_type(data)
        self.assertEqual(type_id, FLT4TYPE)
        self.assertEqual(name, 'x')
        self.assertEqual(size, 4)
        self.assertTrue(null_ok)
        self.assertEqual(remaining, b'')

    def test_ntexttype_no_table_parts(self):
        max_len = struct.pack('<i', 1073741823)  # 0x3FFFFFFF (typical ntext MaxLen)
        collation = b'\x09\x04\xD0\x00\x34'     # arbitrary 5-byte collation
        num_parts = b'\x00'
        data = (
            self._make_header(NTEXTTYPE)
            + max_len + collation + num_parts
            + _name_bvarchar('nt')
        )
        type_id, name, size, _, _, null_ok, remaining = _parse_description_type(data)
        self.assertEqual(type_id, NTEXTTYPE)
        self.assertEqual(name, 'nt')
        self.assertEqual(remaining, b'')

    def test_ntexttype_with_table_parts(self):
        max_len = struct.pack('<i', 1073741823)
        collation = b'\x09\x04\xD0\x00\x34'
        # _parse_str(data, 2) reads a 2-byte char count (US_VARCHAR)
        part1 = struct.pack('<H', 3) + 'dbo'.encode('utf-16-le')
        part2 = struct.pack('<H', 5) + 'mytbl'.encode('utf-16-le')
        num_parts = b'\x02'
        data = (
            self._make_header(NTEXTTYPE)
            + max_len + collation + num_parts + part1 + part2
            + _name_bvarchar('nt')
        )
        type_id, name, _, _, _, _, remaining = _parse_description_type(data)
        self.assertEqual(type_id, NTEXTTYPE)
        self.assertEqual(name, 'nt')
        self.assertEqual(remaining, b'')

    def test_xmltype_no_schema(self):
        data = (
            self._make_header(XMLTYPE)
            + b'\x00'   # SchemaPresent = 0
            + _name_bvarchar('x')
        )
        type_id, name, _, _, _, _, remaining = _parse_description_type(data)
        self.assertEqual(type_id, XMLTYPE)
        self.assertEqual(name, 'x')
        self.assertEqual(remaining, b'')

    def test_xmltype_with_schema(self):
        db_name   = b'\x04' + 'test'.encode('utf-16-le')
        schema    = b'\x03' + 'dbo'.encode('utf-16-le')
        xsd_coll  = b'\x05\x00' + 'myXSD'.encode('utf-16-le')  # US_VARCHAR, 5 chars
        data = (
            self._make_header(XMLTYPE)
            + b'\x01'   # SchemaPresent = 1
            + db_name + schema + xsd_coll
            + _name_bvarchar('x')
        )
        type_id, name, _, _, _, _, remaining = _parse_description_type(data)
        self.assertEqual(type_id, XMLTYPE)
        self.assertEqual(name, 'x')
        self.assertEqual(remaining, b'')

    def test_udttype(self):
        max_bytes  = struct.pack('<h', 16)   # MaxByteSize = 16
        db_name    = b'\x04' + 'test'.encode('utf-16-le')
        sch_name   = b'\x03' + 'dbo'.encode('utf-16-le')
        type_name  = b'\x05' + 'Point'.encode('utf-16-le')
        asm_name   = b'\x05\x00' + 'myasm'.encode('utf-16-le')  # US_VARCHAR, 5 chars
        data = (
            self._make_header(UDTTYPE)
            + max_bytes + db_name + sch_name + type_name + asm_name
            + _name_bvarchar('u')
        )
        type_id, name, size, _, _, _, remaining = _parse_description_type(data)
        self.assertEqual(type_id, UDTTYPE)
        self.assertEqual(name, 'u')
        self.assertEqual(size, 16)
        self.assertEqual(remaining, b'')

    def test_jsontype(self):
        data = self._make_header(JSONTYPE) + _name_bvarchar('j')
        type_id, name, _, _, _, _, remaining = _parse_description_type(data)
        self.assertEqual(type_id, JSONTYPE)
        self.assertEqual(name, 'j')
        self.assertEqual(remaining, b'')

    def test_vectortype(self):
        max_len = struct.pack('<h', 12)   # 3 float32 values = 12 bytes
        scale   = bytes([VECTOR_ELEM_FLOAT32])
        data = (
            self._make_header(VECTORTYPE)
            + max_len + scale
            + _name_bvarchar('v')
        )
        type_id, name, size, _, got_scale, _, remaining = _parse_description_type(data)
        self.assertEqual(type_id, VECTORTYPE)
        self.assertEqual(name, 'v')
        self.assertEqual(size, 12)
        self.assertEqual(got_scale, VECTOR_ELEM_FLOAT32)
        self.assertEqual(remaining, b'')


if __name__ == '__main__':
    unittest.main()
