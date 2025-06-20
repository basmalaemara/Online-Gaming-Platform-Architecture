from cassandra.cluster import Cluster
import json
from tabulate import tabulate


def get_cassandra_session():
    cluster = Cluster(['127.0.0.1'])  # change if needed
    session = cluster.connect('monster_arena')
    return session

def query_player_stats(session, player_id):
    rows = session.execute("SELECT * FROM player_statistics WHERE player_id = %s", (player_id,))
    data = []
    for r in rows:
        data.append([
            r.player_id,
            r.snapshot_time.strftime("%Y-%m-%d %H:%M:%S") if r.snapshot_time else None,
            r.kills,
            r.damage_dealt,
            r.playtime_seconds,
            r.resources_collected
        ])
    headers = ["Player ID", "Snapshot Time", "Kills", "Damage Dealt", "Playtime (sec)", "Resources Collected"]
    print("\nPlayer Statistics:")
    print(tabulate(data, headers=headers, tablefmt="pretty"))

def query_game_events(session, player_id):
    rows = session.execute("SELECT * FROM game_analytics_events WHERE player_id = %s ALLOW FILTERING", (player_id,))
    data = []
    for r in rows:
        details = json.loads(r.details) if r.details else {}
        data.append([
            str(r.event_id),
            r.event_type,
            r.event_time.strftime("%Y-%m-%d %H:%M:%S") if r.event_time else None,
            r.player_id,
            r.game_id,
            json.dumps(details)
        ])
    headers = ["Event ID", "Event Type", "Event Time", "Player ID", "Game ID", "Details"]
    print("\nGame Analytics Events:")
    print(tabulate(data, headers=headers, tablefmt="pretty"))


def query_leaderboard_archives(session, game_id):
    print(f"\nFetching all leaderboard archives for Game ID = {game_id} (using ALLOW FILTERING)...")
    rows = session.execute(
        "SELECT * FROM leaderboard_archives WHERE game_id = %s ALLOW FILTERING",
        (game_id,)
    )
    data = []
    for r in rows:
        data.append([
            r.game_id,
            r.snapshot_time.strftime("%Y-%m-%d %H:%M:%S") if r.snapshot_time else None,
            r.player_id,
            r.rank,
            r.score
        ])
    headers = ["Game ID", "Snapshot Time", "Player ID", "Rank", "Score"]
    print("\nLeaderboard Archives:")
    print(tabulate(data, headers=headers, tablefmt="pretty"))



def main():
    session = get_cassandra_session()
    while True:
        print("\nWhat data do you want to see? (Enter number)")
        print("1. Player Statistics")
        print("2. Game Analytics Events")
        print("3. Leaderboard Archives")
        print("4. Exit")
        choice = input("Choice: ").strip()

        if choice == '1':
            player_id = int(input("Enter Player ID: "))
            query_player_stats(session, player_id)
        elif choice == '2':
            player_id = int(input("Enter Player ID: "))
            query_game_events(session, player_id)
        elif choice == '3':
            game_id = int(input("Enter Game ID: "))
            query_leaderboard_archives(session, game_id)

        elif choice == '4':
            print("Goodbye!")
            break
        else:
            print("Invalid choice, try again.")

    session.shutdown()
if __name__ == "__main__":
    main()