import sys
import logging
import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.getcwd(), '.env'), override=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

DATASET_DIR = "dataset"
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

def connect_db():
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        logger.info("Connected to MongoDB: %s", DB_NAME)
        return db, client
    except Exception as e:
        logger.error("Error connecting to MongoDB: %s", e)
        sys.exit(1)

def clean_db(db):
    logger.info("Cleaning database state...")
    col_names = db.list_collection_names()
    for col in ["players", "clubs", "competitions", "games1", "games2"]:
        if col in col_names:
            db[col].drop()
            logger.info("Dropped collection: %s", col)

def load_csv(filename):
    path = os.path.join(DATASET_DIR, filename)
    if not os.path.exists(path):
        logger.error("Error: File not found: %s", path)
        sys.exit(1)
    return pd.read_csv(path)

def setup_indexes(db):
    logger.info("Setting up indexes...")
    
    try:
        db.players.create_index([("surname", 1), ("name", 1)], unique=False)
        db.players.create_index([("surname", 1), ("name", 1), ("birthdate", 1)], unique=True)
        db.players.create_index([("plays_in_competition", 1), ("position", 1)], unique=False)
        logger.info("Indexes created for 'players'")
    except Exception as e:
        logger.warning("Warning creating indexes for players: %s", e)

    try:
        db.clubs.create_index([("name", 1)], unique=False)
        db.clubs.create_index([("name", 1), ("club_id", 1)], unique=True)
        logger.info("Indexes created for 'clubs'")
    except Exception as e:
        logger.warning("Warning creating indexes for clubs: %s", e)

    try:
        db.games1.create_index([("home_club_goals", 1), ("competition_id", 1)], unique=False)
        db.games1.create_index([("home_club_goals", 1), ("competition_id", 1), ("game_id", 1)], unique=True)
        db.games1.create_index([("stadium_name", 1), ("season", 1)], unique=False)
        logger.info("Indexes created for 'games1'")
    except Exception as e:
        logger.warning("Warning creating indexes for games1: %s", e)

    try:
        db.games2.create_index([("referee", 1)], unique=False)
        db.games2.create_index([("referee", 1), ("game_id", 1)], unique=True)
        db.games2.create_index([("referee", 1), ("game_events.type", 1)], unique=False)
        logger.info("Indexes created for 'games2'")
    except Exception as e:
        logger.warning("Warning creating indexes for games2: %s", e)

def process_players(db):
    logger.info("Processing Players...")
    try:
        players = load_csv("players.csv")
        valuations = load_csv("player_valuations.csv")
        appearances = load_csv("appearances.csv")

        val_subset = valuations[['player_id', 'market_value_in_eur', 'date']].copy()
        val_subset.rename(columns={'market_value_in_eur': 'market_value'}, inplace=True)
        val_grouped = val_subset.groupby('player_id').apply(
            lambda x: x[['market_value', 'date']].to_dict('records'),
            include_groups=False
        ).reset_index(name='player_valuation')

        app_subset = appearances[['player_id', 'competition_id']].drop_duplicates()
        app_grouped = app_subset.groupby('player_id')['competition_id'].apply(list).reset_index(name='plays_in_competition')
        
        players_final = players[['player_id', 'last_name', 'first_name', 'date_of_birth', 'position', 'country_of_citizenship']].copy()
        players_final.rename(columns={
            'last_name': 'surname',
            'first_name': 'name',
            'date_of_birth': 'birthdate',
            'country_of_citizenship': 'citizenship_country_name'
        }, inplace=True)

        players_final = players_final.merge(val_grouped, on='player_id', how='left')
        players_final = players_final.merge(app_grouped, on='player_id', how='left')

        players_final['player_valuation'] = players_final['player_valuation'].apply(lambda d: d if isinstance(d, list) else [])
        players_final['plays_in_competition'] = players_final['plays_in_competition'].apply(lambda d: d if isinstance(d, list) else [])
        
        before_count = len(players_final)
        players_final.drop_duplicates(subset=['surname', 'name', 'birthdate'], inplace=True)
        after_count = len(players_final)
        if before_count != after_count:
            logger.info("Dropped %d duplicate players to satisfy unique index.", before_count - after_count)

        records = players_final.to_dict('records')
        db.players.insert_many(records)

        logger.info("Inserted %d players", len(records))
        
    except Exception as e:
        logger.error("Error processing players: %s", e)
        
def process_clubs(db):
    logger.info("Processing Clubs...")
    try:
        clubs = load_csv("clubs.csv")
        games = load_csv("games.csv")
        players = load_csv("players.csv")
        valuations = load_csv("player_valuations.csv")

        away_games = games[['away_club_id', 'stadium']].dropna().drop_duplicates()
        away_stadiums = away_games.groupby('away_club_id')['stadium'].apply(list).reset_index(name='away_stadium_name')
        
        val_subset = valuations[['player_id', 'market_value_in_eur', 'date']].copy()
        val_subset.rename(columns={'market_value_in_eur': 'market_value'}, inplace=True)
        val_grouped = val_subset.groupby('player_id').apply(
            lambda x: x[['market_value', 'date']].to_dict('records'),
            include_groups=False
        ).reset_index(name='player_valuation')

        p_club = players[['player_id', 'current_club_id', 'first_name', 'last_name']].copy()
        p_club.rename(columns={'first_name': 'name', 'last_name': 'surname'}, inplace=True)
        
        p_club = p_club.merge(val_grouped, on='player_id', how='left')
        p_club['player_valuation'] = p_club['player_valuation'].apply(lambda d: d if isinstance(d, list) else [])

        p_club_records = p_club.groupby('current_club_id').apply(
            lambda x: x[['name', 'surname', 'player_valuation']].to_dict('records'),
            include_groups=False
        ).reset_index(name='players')

        clubs_final = clubs[['club_id', 'name', 'stadium_name']].copy()
        
        clubs_final = clubs_final.merge(away_stadiums, left_on='club_id', right_on='away_club_id', how='left')
        clubs_final['away_stadium_name'] = clubs_final['away_stadium_name'].apply(lambda d: d if isinstance(d, list) else [])
        
        clubs_final = clubs_final.merge(p_club_records, left_on='club_id', right_on='current_club_id', how='left')
        clubs_final['players'] = clubs_final['players'].apply(lambda d: d if isinstance(d, list) else [])

        insert_data = clubs_final[['club_id', 'name', 'stadium_name', 'away_stadium_name', 'players']].to_dict('records')

        db.clubs.insert_many(insert_data)
        logger.info("Inserted %d clubs", len(insert_data))

    except Exception as e:
        logger.error("Error processing clubs: %s", e)

def process_competitions(db):
    logger.info("Processing Competitions...")
    try:
        comps = load_csv("competitions.csv")
        games = load_csv("games.csv")

        g_stadiums = games[['competition_id', 'stadium']].dropna().drop_duplicates()
        stadiums_grouped = g_stadiums.groupby('competition_id')['stadium'].apply(list).reset_index(name='stadiums')

        comps_final = comps[['competition_id', 'name']].copy()
        comps_final = comps_final.merge(stadiums_grouped, on='competition_id', how='left')
        comps_final['stadiums'] = comps_final['stadiums'].apply(lambda d: d if isinstance(d, list) else [])
        
        records = []
        for _, row in comps_final.iterrows():
            records.append({
                "_id": row['competition_id'],
                "name": row['name'],
                "stadiums": row['stadiums']
            })

        db.competitions.insert_many(records)
        logger.info("Inserted %d competitions", len(records))

    except Exception as e:
        logger.error("Error processing competitions: %s", e)

def process_game1(db):
    logger.info("Processing Game_1...")
    try:
        games = load_csv("games.csv")
        clubs = load_csv("clubs.csv")
        comps = load_csv("competitions.csv")

        club_map = clubs.set_index('club_id')['name'].to_dict()

        games['home_club_name'] = games['home_club_id'].map(club_map)
        games['away_club_name'] = games['away_club_id'].map(club_map)
        
        games.rename(columns={'stadium': 'stadium_name'}, inplace=True)

        cols = ['game_id', 'home_club_goals', 'competition_id', 'date', 'season', 'home_club_name', 'away_club_name', 'stadium_name']
        games_final = games[cols].copy()

        records = games_final.to_dict('records')
        
        db.games1.insert_many(records)
        logger.info("Inserted %d games1", len(records))

    except Exception as e:
        logger.error("Error processing games1: %s", e)

def process_game2(db):
    logger.info("Processing Game_2...")
    try:
        games = load_csv("games.csv")
        events = load_csv("game_events.csv")

        events_grouped = events.groupby('game_id')['type'].apply(list).reset_index(name='game_events')

        g_subset = games[['game_id', 'referee']].copy()
        
        g_final = g_subset.merge(events_grouped, on='game_id', how='left')
        g_final['game_events'] = g_final['game_events'].apply(lambda d: d if isinstance(d, list) else [])

        records = g_final.to_dict('records')
        
        db.games2.insert_many(records)
        logger.info("Inserted %d games2", len(records))

    except Exception as e:
        logger.error("Error processing games2: %s", e)

def setup_sharding(client):
    logger.info("Setting up Sharding...")
    db_name = os.getenv("DB_NAME")
    try:
        client.admin.command('enableSharding', db_name)
        logger.info("Sharding enabled for database: %s", db_name)
        
        try:
            client.admin.command('shardCollection', f"{db_name}.players", key={"surname": 1, "name": 1})
            logger.info("Sharded 'players' with key: {surname: 1, name: 1}")
        except Exception as e:
            logger.warning("Could not shard 'players': %s", e)

        try:
            client.admin.command('shardCollection', f"{db_name}.clubs", key={"name": "hashed"})
            logger.info("Sharded 'clubs' with key: {name: 'hashed'}")
        except Exception as e:
            logger.warning("Could not shard 'clubs': %s", e)
            
        try:
            client.admin.command('shardCollection', f"{db_name}.games1", key={"home_club_goals": 1, "competition_id": 1})
            logger.info("Sharded 'games1' with key: {home_club_goals: 1, competition_id: 1}")
        except Exception as e:
            logger.warning("Could not shard 'games1': %s", e)
            
        try:
            client.admin.command('shardCollection', f"{db_name}.games2", key={"referee": "hashed"})
            logger.info("Sharded 'games2' with key: {referee: 'hashed'}")
        except Exception as e:
            logger.warning("Could not shard 'games2': %s", e)
            
        try:
            client.admin.command('shardCollection', f"{db_name}.competitions", key={"_id": "hashed"})
            logger.info("Sharded 'competitions' with key: {_id: 'hashed'}")
        except Exception as e:
            logger.warning("Could not shard 'competitions': %s", e)

    except Exception as e:
        logger.warning("Sharding setup failed (This is expected if not connected to a mongos or if already sharded): %s", e)

def main():
    db, client = connect_db()
    
    # Init
    clean_db(db)
    setup_indexes(db)
    setup_sharding(client)

    # Load
    process_players(db)
    process_clubs(db)
    process_competitions(db)
    process_game1(db)
    process_game2(db)
    
    logger.info("ETL Pipeline completed successfully")

if __name__ == "__main__":
    main()
