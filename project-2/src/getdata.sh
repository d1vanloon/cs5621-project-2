#!/bin/bash -l

# This script prepares data for use with the OpenStreetMap Distances project.
# Usage: ./getdata.sh output-directory [-s]
# Flags:
#   -s	Downloads a small data file instead of the entire world. Useful for testing in a timely manner.

OUTPUT_DIR = $1

if [ "$#" -gt 2 ] && [ "$2" -ne "-s" ]; then
	SRC_ADDRESS = "www.davidvanloon.com/architecture/duluth_minnesota.osm.bz2"
	echo "Using small data file (Duluth, MN)."
else
	SRC_ADDRESS = "http://ftp5.gwdg.de/pub/misc/openstreetmap/planet.openstreetmap.org/planet/planet-latest.osm.bz2"
	echo -n "Using full data file. Are you sure? This may take over a day. (Y/n)"
	read CHOICE
	if [ "$CHOICE" -eq "n" ]; then
		exit 0
	fi
fi

echo "Downloading data..."

