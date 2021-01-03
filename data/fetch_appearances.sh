set -x

# relative paths
site_map_file=$1
output_file=$2

# for the output file an absolute path is actually easier to handle
output_file=$PWD/$output_file

docker run \
  -v "$(pwd)"/$site_map_file:/app/site_map.json \
  dcaribou/transfermarkt-scraper:latest \
  scrapy crawl partial -a site_map_file=site_map.json \
  > $output_file
