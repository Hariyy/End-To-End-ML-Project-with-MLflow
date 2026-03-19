import os
import ssl
import certifi
import urllib.request as request
import requests
import zipfile
from CATProject import logger
from CATProject.entity.config_entity import (DataIngestionConfig)
from CATProject.utils.common import get_size
from pathlib import Path


class DataIngestion:
	def __init__(self, config: DataIngestionConfig):
		self.config = config

	def download_file(self):
		if os.path.exists(self.config.local_data_file):
			if zipfile.is_zipfile(self.config.local_data_file) and os.path.getsize(self.config.local_data_file) > 0:
				logger.info(f"File already exists of size: {get_size(Path(self.config.local_data_file))}")
				return
			os.remove(self.config.local_data_file)

		resp = requests.get(self.config.source_URL, stream=True)
		resp.raise_for_status()

		with open(self.config.local_data_file, "wb") as f:
			for chunk in resp.iter_content(chunk_size=8192):
				if chunk:
					f.write(chunk)
		logger.info(f"Downloaded file: {self.config.local_data_file} with headers: {resp.headers}")


		if not zipfile.is_zipfile(self.config.local_data_file):
			raise RuntimeError("Downloaded file is not a valid zip. Check the source URL/content.")

	def extract_zip_file(self):
		if not zipfile.is_zipfile(self.config.local_data_file):
			raise RuntimeError("Cannot extract: downloaded file is not a valid zip.")
		unzip_path = self.config.unzip_dir
		os.makedirs(unzip_path, exist_ok=True)
		with zipfile.ZipFile(self.config.local_data_file, "r") as zip_ref:
			zip_ref.extractall(unzip_path)