# install.packages("worldfootballR")

library(worldfootballR)

team_url = "https://www.transfermarkt.co.uk/fc-barcelona/startseite/verein/131" # barca
player_url = "https://www.transfermarkt.co.uk/robert-lewandowski/profil/spieler/38253" # lewandowski

transfers = tm_player_transfer_history(player_url)
injuries <- tm_player_injury_history(player_url)

