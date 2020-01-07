LFM-1b-MusicBrainz-Gender-Wrangler
=========================

UNDER DEVELOPMENT...

Links the LFM-1b dataset consisting of Last FM listening histories to artist gender meta data found in a local config of the MusicBrainz db.

## Prerequisites

Before cloning the repo the LFM-1b and MusicBrainz data-set must be appropriatly configured.

* [LFM-1b](http://www.cp.jku.at/datasets/LFM-1b/) dataset: Download the file: **LFM-1b.zip** and extract to appropriate dir. 

* Rename LFM-1b_tracks.txt to LFM-1b_tracks.tsv
* Run the following cmd in psql to insert data from LFM-1b_tracks into a db: ```COPY songs FROM 'C:/../LFM-1b_tracks.tsv' DELIMITER '\t'```

* The [MusicBrainz](https://musicbrainz.org/doc/MusicBrainz_Database/Download) db: Download your prefered data dump and follow the instructions below for configuration... (TODO:)

Then check out and configure the following [musicBrainz db search tool](https://github.com/dshakes90/musicbrainzsearch) and follow the instructions in it's README.md to start the elastic search API.

## Running the Program

