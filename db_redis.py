import redis
import uuid
from tabulate import tabulate

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# ---- Chat Messages (Lists) ----
def add_chat_message():
    game_id = input("Enter Game ID: ")
    channel_id = input("Enter Channel ID: ")
    player_id = input("Enter Player ID: ")
    message = input("Enter message: ")
    key = f"chat:game:{game_id}:channel:{channel_id}"
    redis_client.lpush(key, f"player_id:{player_id} message:{message}")
    print("Chat message added.")

def get_chat_messages():
    game_id = input("Enter Game ID: ")
    channel_id = input("Enter Channel ID: ")
    key = f"chat:game:{game_id}:channel:{channel_id}"
    messages = redis_client.lrange(key, 0, 9)
    data = [[i+1, msg] for i, msg in enumerate(reversed(messages))]
    print("\nRecent Chat Messages:")
    print(tabulate(data, headers=["No.", "Message"], tablefmt="pretty"))

# ---- Live Leaderboard (Sorted Set) ----
def update_leaderboard_score():
    game_id = input("Enter Game ID: ")
    player_id = input("Enter Player ID: ")
    score = float(input("Enter Score: "))
    key = f"leaderboard:game:{game_id}"
    redis_client.zadd(key, {player_id: score})
    print(f"Leaderboard updated for player {player_id}.")

def get_leaderboard():
    game_id = input("Enter Game ID: ")
    key = f"leaderboard:game:{game_id}"
    entries = redis_client.zrevrange(key, 0, 9, withscores=True)
    data = [[player, score] for player, score in entries]
    print("\nLeaderboard Top 10:")
    print(tabulate(data, headers=["Player ID", "Score"], tablefmt="pretty"))

# ---- Player State (Hash) ----
def update_player_state():
    player_id = input("Enter Player ID: ")
    print("Enter state as key=value pairs (empty line to finish):")
    state = {}
    while True:
        line = input()
        if not line.strip():
            break
        if '=' in line:
            k,v = line.split('=', 1)
            state[k.strip()] = v.strip()
        else:
            print("Invalid format, use key=value")
    key = f"player:{player_id}:state"
    redis_client.hset(key, mapping=state)
    print(f"Player {player_id} state updated.")

def get_player_state():
    player_id = input("Enter Player ID: ")
    key = f"player:{player_id}:state"
    state = redis_client.hgetall(key)
    if state:
        print(f"\nPlayer {player_id} State:")
        print(tabulate(state.items(), headers=["Field", "Value"], tablefmt="pretty"))
    else:
        print(f"No state found for player {player_id}.")

# ---- Game Object Instances (Set + Hash) ----
def add_game_object():
    game_id = input("Enter Game ID: ")
    object_type_id = input("Enter Object Type ID: ")
    position = input("Enter Position (e.g. '50,75'): ")
    health = input("Enter Current Health: ")
    status = input("Enter Status: ")
    object_id = str(uuid.uuid4())
    set_key = f"game:{game_id}:objects"
    hash_key = f"game:{game_id}:object:{object_id}"
    redis_client.sadd(set_key, object_id)
    # Use multiple hset calls if mapping arg not supported
    redis_client.hset(hash_key, "object_type_id", object_type_id)
    redis_client.hset(hash_key, "position", position)
    redis_client.hset(hash_key, "current_health", health)
    redis_client.hset(hash_key, "status", status)
    print(f"Game object {object_id} added.")


def list_game_objects():
    game_id = input("Enter Game ID: ")
    set_key = f"game:{game_id}:objects"
    object_ids = redis_client.smembers(set_key)
    data = []
    for obj_id in object_ids:
        hash_key = f"game:{game_id}:object:{obj_id}"
        obj = redis_client.hgetall(hash_key)
        obj["object_id"] = obj_id
        data.append(obj)
    if data:
        keys = sorted(data[0].keys())
        rows = [[obj.get(k, "") for k in keys] for obj in data]
        print(f"\nGame Objects in Game {game_id}:")
        print(tabulate(rows, headers=keys, tablefmt="pretty"))
    else:
        print(f"No game objects found for game {game_id}.")

# ---- Main Interactive Menu ----
def main():
    while True:
        print("\nChoose Redis action:")
        print("1. Add chat message")
        print("2. Get chat messages")
        print("3. Update leaderboard score")
        print("4. Get leaderboard top 10")
        print("5. Update player state")
        print("6. Get player state")
        print("7. Add game object")
        print("8. List game objects")
        print("9. Exit")
        choice = input("Choice: ").strip()
        if choice == '1':
            add_chat_message()
        elif choice == '2':
            get_chat_messages()
        elif choice == '3':
            update_leaderboard_score()
        elif choice == '4':
            get_leaderboard()
        elif choice == '5':
            update_player_state()
        elif choice == '6':
            get_player_state()
        elif choice == '7':
            add_game_object()
        elif choice == '8':
            list_game_objects()
        elif choice == '9':
            print("Exiting. Goodbye!")
            break
        else:
            print("Invalid choice, please try again.")

if __name__ == "__main__":
    main()
