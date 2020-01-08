LFM-1b-MusicBrainz-Gender-Wrangler
=========================

Links the LFM-1b dataset consisting of Last FM listening histories to artist gender meta data found in a local config of the MusicBrainz db.

UNDER DEVELOPMENT...

## Prerequisites

* Open JDK 11
* PSQL (PostgreSQL) 10.10
* Docker / Docker compose 

Before cloning the repo the LFM-1b and MusicBrainz data-set must be appropriatly configured.

* [LFM-1b](http://www.cp.jku.at/datasets/LFM-1b/) dataset: Download the file: **LFM-1b.zip** and extract to appropriate dir. 

* Rename LFM-1b_tracks.txt to LFM-1b_tracks.tsv
* Run the following cmd in psql to insert data from LFM-1b_tracks into a db: <br/>
```COPY songs FROM 'C:/../LFM-1b_tracks.tsv' DELIMITER '\t'```

* The [MusicBrainz](https://musicbrainz.org/doc/MusicBrainz_Database/Download) db dumps should be downloaded. Choose the appropriate mirror and download **mbdump.tar.bz2** and **mbdump-derived.tar.bz2** to your chosen path.   

* Clone the [mbdump](https://github.com/lalinsky/mbdata.git) repo into desired path. Then follow the instructions below to build the dump. <br/>

```
sudo su - postgres
createuser musicbrainz
createdb -l C -E UTF-8 -T template0 -O musicbrainz musicbrainz
psql musicbrainz -c 'CREATE EXTENSION cube;
psql musicbrainz -c 'CREATE EXTENSION earthdistance;

$MBDATA = your cloned mbdata path
$MBDUMP = your path to mbdump files

mbslave psql -f $MBDATA/mbdata/sql/CreateTables.sql
mbslave psql -f $MBDATA/mbdata/sql/statistics/CreateTables.sql
mbslave psql -f $MBDATA/mbdata/sql/caa/CreateTables.sql
mbslave psql -f $MBDATA/mbdata/sql/wikidocs/CreateTables.sql
mbslave psql -f $MBDATA/mbdata/sql/documentation/CreateTables.sql

mbslave import $MBDUMP/mbdump.tar.bz2 
mbslave import $MBDUMP/mbdump-derived.tar.bz2

mbslave psql -f $MBDATA/mbdata/sql/CreatePrimaryKeys.sql
mbslave psql -f $MBDATA/mbdata/sql/statistics/CreatePrimaryKeys.sql
mbslave psql -f $MBDATA/mbdata/sql/caa/CreatePrimaryKeys.sql
mbslave psql -f $MBDATA/mbdata/sql/wikidocs/CreatePrimaryKeys.sql
mbslave psql -f $MBDATA/mbdata/sql/documentation/CreatePrimaryKeys.sql
mbslave psql -f $MBDATA/mbdata/sql/CreateIndexes.sql
mbslave psql -f $MBDATA/mbdata/sql/CreateSlaveIndexes.sql
mbslave psql -f $MBDATA/mbdata/sql/statistics/CreateIndexes.sql
mbslave psql -f $MBDATA/mbdata/sql/caa/CreateIndexes.sql
mbslave psql -f $MBDATA/mbdata/sql/CreateFunctions.sql
mbslave psql -f $MBDATA/mbdata/sql/CreateViews.sql
```

Then check out and configure the following [MusicBrainz db search tool](https://github.com/dshakes90/musicbrainzsearch) and follow the instructions in it's ```README.md``` to start the elastic search API.

## Running the Program

* Change all relevent paths in ```DeriveGenderFromDb.java``` to set paths for MusicBrainz db and the LFM-1b songs table. 
* Compile and run ```LastFmMusicbrainzWrangler.java``` 
* Solutions will be output to the file ``` part-00000.txt```  as tab seperated files. This will be based off ``` LFM-1b_artists.txt```  and will have an additional collumn for artist gender. 

## Gender Classification

Gender classification is formed by considering the genders of all current and previous memebers of an artist / band defined in the MusicBrainz db. A final gender classification is output as a '/' seperated String such that gender counts are defined as follows: <br\>

```unknown gender count / male gender count / female gender count / other gender count / na gender count``` 

