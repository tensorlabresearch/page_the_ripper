"""Tests for tools/escl_scan.py -- eSCL protocol helpers."""

from __future__ import annotations

import pytest

from tools.escl_scan import (
    ESCLScanError,
    build_scan_request,
    ensure_http_url,
    first,
    first_int,
    parse_env_file,
    resolve_color_mode,
    resolve_format,
    resolve_resolution,
    size_dimensions,
)


class TestFirst:
    def test_returns_first_element(self):
        assert first(["a", "b"]) == "a"

    def test_returns_default_for_empty(self):
        assert first([], default="x") == "x"

    def test_returns_none_for_empty_no_default(self):
        assert first([]) is None


class TestFirstInt:
    def test_returns_int(self):
        assert first_int(["42"]) == 42

    def test_returns_default_for_empty(self):
        assert first_int([], default=7) == 7

    def test_returns_none_for_empty_no_default(self):
        assert first_int([]) is None


class TestEnsureHttpUrl:
    def test_http_url_passes(self):
        assert ensure_http_url("http://192.168.1.1") == "http://192.168.1.1"

    def test_https_url_passes(self):
        assert ensure_http_url("https://scanner.local") == "https://scanner.local"

    def test_invalid_url_raises(self):
        with pytest.raises(ESCLScanError, match="Invalid scanner URL"):
            ensure_http_url("ftp://scanner.local")


class TestResolveColorMode:
    def test_r24(self):
        assert resolve_color_mode("r24") == "RGB24"

    def test_g8(self):
        assert resolve_color_mode("g8") == "Grayscale8"

    def test_invalid(self):
        with pytest.raises(ESCLScanError, match="Invalid color mode"):
            resolve_color_mode("cmyk")


class TestResolveFormat:
    def test_jpg(self):
        assert resolve_format("jpg") == "image/jpeg"

    def test_pdf(self):
        assert resolve_format("pdf") == "application/pdf"

    def test_invalid(self):
        with pytest.raises(ESCLScanError, match="Invalid output format"):
            resolve_format("bmp")


class TestResolveResolution:
    def test_auto_selects_max(self):
        result = resolve_resolution("", options_x=["150", "300", "600"], options_y=["150", "300", "600"])
        assert result == "600"

    def test_explicit_resolution(self):
        result = resolve_resolution("300", options_x=["150", "300", "600"], options_y=["150", "300", "600"])
        assert result == "300"

    def test_unsupported_raises(self):
        with pytest.raises(ESCLScanError, match="Unsupported resolution"):
            resolve_resolution("1200", options_x=["300", "600"], options_y=["300", "600"])


class TestSizeDimensions:
    def test_us_letter(self):
        w, h = size_dimensions("us", max_width=3000, max_height=4000)
        assert w == 2550
        assert h == 3300

    def test_max_size(self):
        w, h = size_dimensions("max", max_width=5000, max_height=7000)
        assert w == 5000
        assert h == 7000

    def test_unknown_size_raises(self):
        with pytest.raises(ESCLScanError, match="Unknown paper size"):
            size_dimensions("tabloid", max_width=5000, max_height=7000)

    def test_exceeds_limits_raises(self):
        with pytest.raises(ESCLScanError, match="exceeds scanner limits"):
            size_dimensions("a4", max_width=100, max_height=100)


class TestBuildScanRequest:
    def test_produces_xml(self):
        xml = build_scan_request(
            version="2.1",
            document_format="image/jpeg",
            color_mode="RGB24",
            resolution="300",
            width=2550,
            height=3300,
        )
        assert "<?xml" in xml
        assert "RGB24" in xml
        assert "300" in xml
        assert "2550" in xml


class TestParseEnvFile:
    def test_basic_parsing(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("KEY1=value1\nKEY2=value2\n# comment\n\nKEY3=value3\n")
        result = parse_env_file(str(env_file))
        assert result == {"KEY1": "value1", "KEY2": "value2", "KEY3": "value3"}

    def test_skips_lines_without_equals(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("GOOD=yes\nBADLINE\n")
        result = parse_env_file(str(env_file))
        assert result == {"GOOD": "yes"}
