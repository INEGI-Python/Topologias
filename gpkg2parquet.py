import geopandas as geo 

from pathlib import Path
import argparse


def gpkg_to_parquet(gpkg_path):
	"""Exporta todas las capas del GeoPackage a archivos Parquet."""
	gpkg_path = Path(gpkg_path)
	layers = geo.list_layers(gpkg_path)

	for layer_name in layers["name"]:
		layer = geo.read_file(gpkg_path, layer=layer_name).to_crs("EPSG:6372", inplace=False)
		output_path = gpkg_path.with_name(f"{layer_name}.parquet")
		layer.to_parquet(output_path, index=False)
		print(f"Capa '{layer_name}' exportada a {output_path}")


if __name__ == "__main__":
	parser = argparse.ArgumentParser(
		description="Convierte todas las capas de un GeoPackage a Parquet."
	)
	parser.add_argument("--gpkg", required=True, help="Ruta del GeoPackage de entrada")
	args = parser.parse_args()
	gpkg_to_parquet(args.gpkg)


