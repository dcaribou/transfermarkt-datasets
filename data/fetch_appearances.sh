set -x

# this following context in assumed in the machine to be able to run the scrip
# the transfermarkt-scraper github project is cloned in the home directory
# a python virtual environment 'player-scores' is created and the Scrapy module
# is installed
transfermark_scraper_home=~/transfermarkt-scraper
virtalenv=player-scores

# relative
site_map_file=$1
output_file=$2

# absolute
site_map_file=$PWD/$site_map_file
output_file=$PWD/$output_file

source ~/.virtualenvs/$virtalenv/bin/activate
cd $transfermark_scraper_home

scrapy crawl partial -a site_map_file=$site_map_file \
  > $output_file
