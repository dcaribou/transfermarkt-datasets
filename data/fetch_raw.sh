set -xe

scraper_version=v0.1.1

leagues_file=$PWD/$1

# output paths defined as absolute paths are easier to deal with
clubs_file=$PWD/$2
players_file=$PWD/$3
appearances_file=$PWD/$4

# fetch clubs
docker run \
  -v $leagues_file:/app/parents/leagues.json \
  -v $PWD/.scrapy:/app/.scrapy \
  dcaribou/transfermarkt-scraper:$scraper_version \
  scrapy crawl clubs -a parents=parents/leagues.json \
  > $clubs_file

# fetch players
docker run \
  -v $clubs_file:/app/parents/clubs.json \
  -v $PWD/.scrapy:/app/.scrapy \
  dcaribou/transfermarkt-scraper:$scraper_version \
  scrapy crawl players -a parents=parents/clubs.json \
  > $players_file

# fetch appearances
docker run \
  -v $players_file:/app/parents/players.json \
  -v $PWD/.scrapy:/app/.scrapy \
  dcaribou/transfermarkt-scraper:$scraper_version \
  scrapy crawl appearances -a parents=parents/players.json \
  > $appearances_file

# snapshot scrapy cache
aws s3 cp --recursive .scrapy/* s3://player-scores/scrapy-httpcache/
