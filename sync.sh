set -xe

MESSAGE=$1

# snapshot scrapy cache
aws s3 cp .scrapy s3://player-scores/scrapy-httpcache/ --recursive

# snapshot prepared files
aws s3 cp data/prep s3://player-scores/snapshots/prep/ --recursive --exclude "*.json"

python publish_datasets.py "$MESSAGE"
