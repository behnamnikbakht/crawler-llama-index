from pathlib import Path


ROOT_PATH: Path = Path(__file__).parents[1]

data_path: Path = ROOT_PATH / "data"
crawl_path: Path = data_path / "crawl"
index_path: Path = data_path / "index"