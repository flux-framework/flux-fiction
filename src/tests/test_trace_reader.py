from __future__ import annotations

import importlib.util
import logging
import math
import sys
import types

if importlib.util.find_spec("tqdm") is None:
    tqdm_stub = types.ModuleType("tqdm")
    tqdm_stub.tqdm = lambda *args, **kwargs: None
    sys.modules["tqdm"] = tqdm_stub

from flux_fiction._core.models import SacctReader


def test_sacct_reader_skips_spaces_after_delimiters(tmp_path):
    trace = tmp_path / "trace.csv"
    trace.write_text(
        "JobID,NNodes,NCPUS,Timelimit,Submit,Elapsed,NGPUS, RabbitGiB\n"
        "1,1,1,00:01:00,2020-01-22T08:06:46,00:01:00,0,15000\n",
        encoding="ascii",
    )

    reader = SacctReader(str(trace), require_gpus=True)
    reader.validate_trace()
    jobs = reader.read_trace()

    assert len(jobs) == 1
    assert math.isclose(
        jobs[0].rabbit_storage_gib,
        15000.0,
    )


def test_sacct_reader_rejects_legacy_gb_rabbit_field(tmp_path, caplog):
    trace = tmp_path / "trace.csv"
    trace.write_text(
        "JobID,NNodes,NCPUS,Timelimit,Submit,Elapsed,NGPUS,RabbitGB\n"
        "1,1,1,00:01:00,2020-01-22T08:06:46,00:01:00,0,15000\n",
        encoding="ascii",
    )

    reader = SacctReader(str(trace), require_gpus=True)
    reader.validate_trace()
    with caplog.at_level(logging.WARNING):
        jobs = reader.read_trace()

    assert len(jobs) == 1
    assert jobs[0].rabbit_storage_gib == 0.0
    assert "no longer supported" in caplog.text
    assert "RabbitGiB" in caplog.text


def test_sacct_reader_rejects_gb_unit_in_gib_field(tmp_path, caplog):
    trace = tmp_path / "trace.csv"
    trace.write_text(
        "JobID,NNodes,NCPUS,Timelimit,Submit,Elapsed,NGPUS,RabbitGiB\n"
        "1,1,1,00:01:00,2020-01-22T08:06:46,00:01:00,0,15000GB\n",
        encoding="ascii",
    )

    reader = SacctReader(str(trace), require_gpus=True)
    reader.validate_trace()
    with caplog.at_level(logging.WARNING):
        jobs = reader.read_trace()

    assert len(jobs) == 1
    assert jobs[0].rabbit_storage_gib == 0.0
    assert "must be expressed in GiB" in caplog.text
