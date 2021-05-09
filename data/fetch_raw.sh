set -xe

scraper_version=main

# output paths defined as absolute paths are easier to deal with
parents_file_path=$PWD/$1
parents_file_name=$(basename $1)
childs_file_path=$PWD/$2
crawler=$(basename $2 .json)

# fetch clubs
docker run \
  -v $parents_file_path:/app/parents/$parents_file_name \
  -v $PWD/.scrapy:/app/.scrapy \
  dcaribou/transfermarkt-scraper:$scraper_version \
  scrapy crawl $crawler -a parents=parents/$parents_file_name \
  > $childs_file_path
