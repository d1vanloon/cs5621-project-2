#!/bin/bash -l

# This script prepares data for use with the OpenStreetMap Distances project.
# Usage: ./getdata.sh [-s]
# Flags:
#   -s	Downloads a small data file instead of the entire world. Useful for testing in a timely manner.

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

wget $SRC_ADDRESS -O input_data.osm.bz2

echo "Finished downloading data."

echo "Downloading utilities..."

mkdir -p cs5621/project2/tools
wget www.davidvanloon.com/architecture/OpenStreetMapInputFlattener.java
/soft/java/jdk1.7.0_45/bin/javac OpenStreetMapInputFlattener.java
mv OpenStreetMapInputFlattener.class cs5621/project2/tools

echo "Extracting data..."

bunzip2 input_data.osm.bz2
mv *.osm input_data.osm

echo "Processing input data..."

BASEDIR=$(dirname $0)

/soft/java/jdk1.7.0_45/bin/java cs5621.project2.tools.OpenStreetMapInputFlattener $BASEDIR/input_data.osm $BASEDIR/input_data_flattened.osm

