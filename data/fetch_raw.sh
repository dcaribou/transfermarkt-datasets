set -xe

scraper_version=main

leagues_file=$1

# output paths defined as absolute paths are easier to deal with
clubs_file=$PWD/$2
players_file=$PWD/$3
appearances_file=$PWD/$4

# fetch clubs
docker run \
  -v "$(pwd)"/data/.:/app/parents \
  dcaribou/transfermarkt-scraper:$scraper_version \
  scrapy crawl clubs -a parents=parents/leagues.json \
  > $clubs_file

# fetch players
docker run \
  -v $clubs_file:/app/parents/clubs.json \
  dcaribou/transfermarkt-scraper:$scraper_version \
  scrapy crawl players -a parents=parents/clubs.json \
  > $players_file

# fetch appearances
docker run \
  -v $players_file:/app/parents/players.json \
  dcaribou/transfermarkt-scraper:$scraper_version \
  scrapy crawl appearances -a parents=parents/players.json \
  > $appearances_file

# save snapshot
aws s3 rm s3://player-scores/snapshots/$(date +"%Y-%m-%d")

aws s3 cp $clubs_file s3://player-scores/snapshots/$(date +"%Y-%m-%d")/clubs.json
aws s3 cp $players_file s3://player-scores/snapshots/$(date +"%Y-%m-%d")/players.json
aws s3 cp $appearances_file s3://player-scores/snapshots/$(date +"%Y-%m-%d")/appearances.json
