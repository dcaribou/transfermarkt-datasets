import re
import yaml
from typing import Dict, Tuple

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

def read_config(config_file="config.yml") -> Dict:
  with open(config_file) as config_file:
    config = yaml.load(config_file, yaml.Loader)
    return config

def parse_market_value(market_value):
	"""Parse a "market value" string into an integer number representing a GBP (british pounds) amount,
	such as "£240Th." or "£34.3m".

	:market_value: "Market value" string
	:return: An integer number representing a GBP amount
	"""

	if market_value is not None:
		match = re.search('£([0-9\.]+)(Th|m)', str(market_value))
		if match:
			factor = match.group(2)
			if factor == 'Th':
				numeric_factor = 1000
			elif factor == 'm':
				numeric_factor = 1000000
			else:
				return None
			
			value = match.group(1)
			return int(float(value)*numeric_factor)
		else:
			return None
	else:
		return None
				
def cast_metric(metric):
	if len(metric) == 0:
		return 0
	else:
		return int(metric)

def cast_minutes_played(minutes_played):
	if len(minutes_played) > 0:
		numeric = minutes_played[:-1]
		return int(numeric)
	else:
		return 0

def geocode(place: str) -> Tuple:
	"""Get coordinates from a place's name.

	Args:
		place (str): A string that describes the place.

	Returns:
		Tuple: A tuple with the coordinates (latitude, longitude)
	"""

	geolocator = Nominatim(user_agent="transfermarkt-datasets")
	geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

	output = geocode(place)

	return (output.latitude, output.longitude)
