"""Create the source package for a future Python 3.12 Flex Consumption remote build."""
import argparse
from pathlib import Path
import zipfile

ROOT = Path(__file__).resolve().parent
SOURCES = {
    "azure/functions/function_app.py":"function_app.py",
    "azure/functions/host.json":"host.json",
    "azure/functions/requirements.txt":"requirements.txt",
    "order_bridge.py":"order_bridge.py",
    "order_middleware.py":"order_middleware.py",
}

def build(path):
    path.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(path,"w") as archive:
        for source, name in sorted(SOURCES.items()):
            info=zipfile.ZipInfo(name,(2026,1,1,0,0,0))
            archive.writestr(info,(ROOT/source).read_bytes(),compress_type=zipfile.ZIP_DEFLATED)

if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("output",type=Path)
    build(p.parse_args().output)
