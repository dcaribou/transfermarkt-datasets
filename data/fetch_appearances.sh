set -xe

scraper_version=main

# relative paths
site_map_file=$1
output_file=$2

# for the output file an absolute path is actually easier to handle
output_file=$PWD/$output_file

tmp_dir=/tmp/player-scores

mkdir -p $tmp_dir

# fetch clubs
docker run \
  -v "$(pwd)"/data/.:/app/parents \
  dcaribou/transfermarkt-scraper:$scraper_version \
  scrapy crawl clubs -a parents=parents/leagues.json \
  > $tmp_dir/clubs.json

# fetch players
docker run \
  -v $tmp_dir/clubs.json:/app/parents/clubs.json \
  dcaribou/transfermarkt-scraper:$scraper_version \
  scrapy crawl players -a parents=parents/clubs.json \
  > $tmp_dir/players.json

# fetch appearances
docker run \
  -v $tmp_dir/players.json:/app/parents/players.json \
  dcaribou/transfermarkt-scraper:$scraper_version \
  scrapy crawl appearances -a parents=parents/players.json \
  > $output_file

aws s3 rm s3://player-scores/snapshots/$(date +"%Y-%m-%d")
aws s3 cp $output_file s3://player-scores/snapshots/$(date +"%Y-%m-%d")/appearances.json
