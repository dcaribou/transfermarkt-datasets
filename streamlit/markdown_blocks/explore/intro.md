The dataset is composed of multiple CSV files with information on competitions, games, clubs, players and appearances that is automatically updated **once a week**.
Each file contains the attributes of the entity and the IDs that can be used to join them together.

For example, the `appearances` file contains **one row per player appearance**, i.e. one row per player per game played.
For each appearance you will find attributes such as `goals`, `assists` or `yellow_cards` and IDs referencing other entities within the dataset, such as `player_id` and `game_id`.

