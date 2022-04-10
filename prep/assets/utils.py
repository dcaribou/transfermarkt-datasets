import re

def parse_market_value(market_value):
      """Parse a "market value" string into an integer number representing a GBP (british pounds) amount,
      such as "£240Th." or "£34.3m".

      :market_value: "Market value" string
      :return: An integer number representing a GBP amount
      """

      if market_value is not None:
        match = re.search('£([0-9\.]+)(Th|m)', market_value)
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
