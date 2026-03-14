"""Tests for image processing helpers in main.py."""

from __future__ import annotations

import numpy as np
import pytest
from PIL import Image


def _make_white_image(width: int = 200, height: int = 300) -> Image.Image:
    return Image.new("L", (width, height), color=255)


def _make_content_image(width: int = 200, height: int = 300) -> Image.Image:
    """Create an image with a dark rectangle in the center."""
    img = Image.new("L", (width, height), color=255)
    arr = np.array(img)
    # Draw a dark block in the center
    cy, cx = height // 2, width // 2
    arr[cy - 20 : cy + 20, cx - 30 : cx + 30] = 30
    return Image.fromarray(arr)


class TestTrimWhiteBorders:
    def test_trims_surrounding_whitespace(self):
        from main import trim_white_borders

        img = _make_content_image(400, 600)
        trimmed = trim_white_borders(img)
        assert trimmed.width < img.width
        assert trimmed.height < img.height

    def test_all_white_returns_original_size(self):
        from main import trim_white_borders

        img = _make_white_image()
        trimmed = trim_white_borders(img)
        assert trimmed.size == img.size


class TestLightCleanup:
    def test_returns_image(self):
        from main import light_cleanup

        img = _make_content_image()
        result = light_cleanup(img)
        assert isinstance(result, Image.Image)
        assert result.size == img.size


class TestCreatePdfFromImages:
    def test_creates_pdf(self, tmp_path):
        from main import create_pdf_from_images

        img = Image.new("RGB", (100, 150), color=(128, 128, 128))
        out = tmp_path / "test.pdf"
        create_pdf_from_images([img], out, dpi=150)
        assert out.exists()
        content = out.read_bytes()
        assert content[:4] == b"%PDF"

    def test_multiple_pages(self, tmp_path):
        from main import create_pdf_from_images

        pages = [Image.new("RGB", (100, 150), color=(i * 50, i * 50, i * 50)) for i in range(3)]
        out = tmp_path / "multi.pdf"
        create_pdf_from_images(pages, out, dpi=300)
        assert out.exists()
        assert out.stat().st_size > 0


class TestNormalizeCropBox:
    def test_valid_crop_box(self):
        from main import normalize_crop_box

        result = normalize_crop_box([0.1, 0.2, 0.8, 0.9])
        assert result == (0.1, 0.2, 0.8, 0.9)

    def test_none_returns_none(self):
        from main import normalize_crop_box

        assert normalize_crop_box(None) is None

    def test_wrong_length_raises(self):
        from fastapi import HTTPException

        from main import normalize_crop_box

        with pytest.raises(HTTPException):
            normalize_crop_box([0.1, 0.2, 0.8])

    def test_out_of_range_raises(self):
        from fastapi import HTTPException

        from main import normalize_crop_box

        with pytest.raises(HTTPException):
            normalize_crop_box([0.1, 0.2, 1.5, 0.9])

    def test_inverted_raises(self):
        from fastapi import HTTPException

        from main import normalize_crop_box

        with pytest.raises(HTTPException):
            normalize_crop_box([0.8, 0.2, 0.1, 0.9])


class TestDetermineColorMode:
    def test_force_color(self):
        from main import determine_color_mode

        assert determine_color_mode(None, force_color=True, default_mode="Grayscale8") == "RGB24"

    def test_explicit_mode(self):
        from main import determine_color_mode

        assert determine_color_mode("RGB24", force_color=False, default_mode="Grayscale8") == "RGB24"

    def test_default_mode(self):
        from main import determine_color_mode

        assert determine_color_mode(None, force_color=False, default_mode="Grayscale8") == "Grayscale8"

    def test_invalid_mode_raises(self):
        from fastapi import HTTPException

        from main import determine_color_mode

        with pytest.raises(HTTPException):
            determine_color_mode("CMYK", force_color=False, default_mode="Grayscale8")


class TestSerializeJob:
    def test_adds_duration(self):
        from main import serialize_job

        job = {
            "id": "abc",
            "scanner": "et3850",
            "status": "completed",
            "created_at": "2025-01-01T00:00:00",
            "updated_at": "2025-01-01T00:01:00",
            "params": '{"dpi": 300}',
            "batch_count": 1,
            "batches_completed": 1,
        }
        result = serialize_job(job)
        assert result["duration_seconds"] == 60.0
        assert "params" not in result
        assert result["ocr_batch_count"] == 1
        assert result["ocr_batches_completed"] == 1
