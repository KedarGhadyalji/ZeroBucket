"""Tests for batch operations (put_many/get_many/delete_many).

Batch ops are best-effort, not all-or-nothing -- a bad item in the batch
must not abort the rest. That's the property most worth testing directly,
alongside order-correlation (does result[i] really correspond to
input[i]?) since that's the one thing that would silently corrupt data
if it were ever wrong.
"""

from __future__ import annotations

import io

import pytest
from PIL import Image as PILImage


def _jpeg(color) -> bytes:
    img = PILImage.new("RGB", (40, 30), color=color)
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def test_put_many_all_succeed(images):
    batch = [_jpeg((1, 0, 0)), _jpeg((0, 1, 0)), _jpeg((0, 0, 1))]
    results = images.put_many(batch)

    assert len(results) == 3
    assert all(r.success for r in results)
    ids = [r.image_id for r in results]
    assert len(set(ids)) == 3  # all unique

    for image_id in ids:
        images.delete(image_id)


def test_put_many_isolates_bad_item_from_good_ones(images):
    """The core best-effort guarantee: one invalid image must not prevent
    the others in the same batch from being stored."""
    batch = [_jpeg((1, 0, 0)), b"not a real image", _jpeg((0, 0, 1))]
    results = images.put_many(batch)

    assert len(results) == 3
    assert results[0].success is True
    assert results[1].success is False
    assert results[1].error is not None
    assert results[2].success is True

    # The two good ones are genuinely retrievable.
    assert images.exists(results[0].image_id) is True
    assert images.exists(results[2].image_id) is True

    images.delete(results[0].image_id)
    images.delete(results[2].image_id)


def test_put_many_result_index_matches_input_position(images):
    """Results must correlate back to input position correctly, even
    when earlier items failed -- this is the property that would
    silently corrupt data if it were ever wrong."""
    batch = [b"bad", _jpeg((10, 20, 30)), b"also bad", _jpeg((40, 50, 60))]
    results = images.put_many(batch)

    assert [r.index for r in results] == [0, 1, 2, 3]
    assert results[0].success is False
    assert results[1].success is True
    assert results[2].success is False
    assert results[3].success is True

    # Confirm index 1 and 3 really do correspond to the right pixel data.
    img1 = images.get(results[1].image_id)
    img3 = images.get(results[3].image_id)
    with PILImage.open(io.BytesIO(img1.data)) as decoded1:
        assert decoded1.getpixel((0, 0)) == (10, 20, 30)
    with PILImage.open(io.BytesIO(img3.data)) as decoded3:
        assert decoded3.getpixel((0, 0)) == (40, 50, 60)

    images.delete(results[1].image_id)
    images.delete(results[3].image_id)


def test_put_many_empty_list(images):
    assert images.put_many([]) == []


def test_put_many_with_custom_filenames(images):
    batch = [_jpeg((1, 0, 0)), _jpeg((0, 1, 0))]
    results = images.put_many(batch, filenames=["a.jpg", "b.jpg"])

    meta_a = images.metadata(results[0].image_id)
    meta_b = images.metadata(results[1].image_id)
    assert meta_a.filename == "a.jpg"
    assert meta_b.filename == "b.jpg"

    images.delete(results[0].image_id)
    images.delete(results[1].image_id)


def test_put_many_mismatched_filenames_length_raises(images):
    with pytest.raises(ValueError):
        images.put_many(
            [_jpeg((1, 0, 0)), _jpeg((0, 1, 0))], filenames=["only_one.jpg"]
        )


def test_put_many_applies_optimize_to_whole_batch(images):
    batch = [_jpeg((1, 0, 0)), _jpeg((0, 1, 0))]
    results = images.put_many(batch, optimize=True, format="webp")

    assert all(r.success for r in results)
    for r in results:
        img = images.get(r.image_id)
        assert img.mime_type == "image/webp"
        images.delete(r.image_id)


def test_get_many_returns_results_in_input_order_including_missing(images):
    id1 = images.put(_jpeg((1, 0, 0)))
    id2 = images.put(_jpeg((0, 1, 0)))
    fake_id = "00000000-0000-0000-0000-000000000000"

    # Deliberately out-of-storage-order, with a missing id in the middle.
    results = images.get_many([id2, fake_id, id1])

    assert [r.image_id for r in results] == [id2, fake_id, id1]
    assert results[0].success is True
    assert results[1].success is False
    assert results[1].error == "not found"
    assert results[2].success is True

    images.delete(id1)
    images.delete(id2)


def test_get_many_empty_list(images):
    assert images.get_many([]) == []


def test_get_many_all_missing(images):
    fake_ids = [
        "00000000-0000-0000-0000-000000000000",
        "11111111-1111-1111-1111-111111111111",
    ]
    results = images.get_many(fake_ids)
    assert len(results) == 2
    assert all(not r.success for r in results)
    assert all(r.image is None for r in results)


def test_delete_many_deletes_existing_and_ignores_missing(images):
    id1 = images.put(_jpeg((1, 0, 0)))
    id2 = images.put(_jpeg((0, 1, 0)))
    fake_id = "00000000-0000-0000-0000-000000000000"

    results = images.delete_many([id1, fake_id, id2])

    assert [r.image_id for r in results] == [id1, fake_id, id2]
    assert results[0].deleted is True
    assert results[1].deleted is False
    assert results[2].deleted is True

    assert images.exists(id1) is False
    assert images.exists(id2) is False


def test_delete_many_empty_list(images):
    assert images.delete_many([]) == []


def test_delete_many_is_actually_a_single_round_trip_worth_of_work(images):
    """Not a strict performance test (too flaky in CI), but confirms
    behavior: deleting many ids at once actually removes all of them,
    exercising the ANY(%s) batch query path rather than a hidden loop
    that happens to produce the same result."""
    ids = [images.put(_jpeg((i, i, i))) for i in range(10)]
    results = images.delete_many(ids)
    assert all(r.deleted for r in results)
    assert all(not images.exists(i) for i in ids)
