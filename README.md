# Remote Sensing Metadata

Remote sensing metadata extracted for [Alplakes](https://www.alplakes.eawag.ch/) from the operational products produced using [Sencast](https://github.com/eawag-surface-waters-research/sencast) and the Alplakes [Airflow](https://github.com/eawag-surface-waters-research/airflow) instance. 

[![License: MIT][mit-by-shield]][mit-by] ![Python][python-by-shield]

This dockerized code operates as follows:

- Sync GeoTiff files from a S3 buckets to a local folder and record a list of changed
- Iterate over the changes, parse the GeoTiff files, extract metadata and produce lake subsets
- Sync metadata and lake subsets to an S3 bucket

## Getting started

### 1. Clone repository
```console
git clone https://github.com/eawag-surface-waters-research/alplakes-sencast-metadata.git
```

### 2. Install conda environment
Command must be run from inside the root of the repository
```console
conda env create -f environment.yml
```

### 3. Run commands

For a full list of options run
```console
python src/main.py -h
```

#### Example python call
```console
python src/main.py -u -rt s3://bucket/tiff -rtc s3://bucket/tiff_cropped -g https://eawagrs.s3.eu-central-1.amazonaws.com/metadata/lakes.json -rm s3://bucket/metadata 
```

Example docker call
```console
docker run -e AWS_ACCESS_KEY_ID=XXXXXXXX -e AWS_SECRET_ACCESS_KEY=XXXXXXXX -v /home/user/alplakes-sencast-metadata:/repository -v /home/user/local_tiff:/local_tiff -v /home/user/local_tiff_cropped:/local_tiff_cropped -v /home/user/local_metadata:/local_metadata --rm eawag/sencast-metadata:1.0.0 -u -rt s3://bucket/tiff -rtc s3://bucket/tiff_cropped -g https://eawagrs.s3.eu-central-1.amazonaws.com/metadata/lakes.json -rm s3://bucket/metadata
```

[mit-by]: https://opensource.org/licenses/MIT
[mit-by-shield]: https://img.shields.io/badge/License-MIT-g.svg
[python-by-shield]: https://img.shields.io/badge/Python-3.11-g
