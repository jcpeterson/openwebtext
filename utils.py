import os
import os.path as op
import tarfile
import re


def extract_month(url_file_name):
    month_re = r"(RS_.*2\d{3}-\d{2})"
    month = op.split(url_file_name)[-1]
    month = re.match(month_re, month).group()
    return month


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


def extract_archive(archive_fp, outdir="."):
    with tarfile.open(archive_fp, "r") as tar:
        tar.extractall(outdir)
    return outdir


def mkdir(fp):
    if not op.exists(fp):
        os.makedirs(fp, exist_ok=True)
    return fp
