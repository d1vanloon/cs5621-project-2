#!/bin/bash -l

# This script prepares data for use with the OpenStreetMap Distances project.
# Usage: ./getdata.sh [-s]
# Flags:
#   -s	Downloads a small data file instead of the entire world. Useful for testing in a timely manner.

SRC_ADDRESS=www.davidvanloon.com/architecture/duluth_minnesota.osm.bz2

if [[ ( $# -gt 0 ) && ( $1 == "-s" ) ]]; then
	SRC_ADDRESS=www.davidvanloon.com/architecture/duluth_minnesota.osm.bz2
	echo "Using small data file (Duluth, MN)."
else
	SRC_ADDRESS=http://ftp5.gwdg.de/pub/misc/openstreetmap/planet.openstreetmap.org/planet/planet-latest.osm.bz2
	read -p "Using full data file. Are you sure? This may take over a day. (y/N)" CHOICE
	case $CHOICE in
		[yY]* )	echo "Okay. Moving forward..."
			;;
		[nN]* ) echo "Okay. Exiting..."
			exit;;
	esac
fi

echo "Downloading data..."

wget $SRC_ADDRESS -O input_data.osm.bz2

echo "Finished downloading data."

echo "Downloading utilities..."

mkdir -p cs5621/project2/tools
wget -q www.davidvanloon.com/architecture/OpenStreetMapInputFlattener.java
/soft/java/jdk1.7.0_45/bin/javac OpenStreetMapInputFlattener.java
mv OpenStreetMapInputFlattener.class cs5621/project2/tools

echo "Extracting data..."

bunzip2 input_data.osm.bz2

echo "Processing input data..."

BASEDIR=$(dirname $0)

/soft/java/jdk1.7.0_45/bin/java cs5621.project2.tools.OpenStreetMapInputFlattener $BASEDIR/input_data.osm $BASEDIR/input_data_flattened.osm

echo "Finished processing input data."

rm input_data.osm
mv input_data_flattened.osm input_data.osm
rm -r cs5621
rm OpenStreetMapInputFlattener.java

echo "Done."
