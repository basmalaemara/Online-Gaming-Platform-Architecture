import streamlit as st
import redis
from cassandra.cluster import Cluster
from datetime import datetime
import uuid
import json
import random
import psycopg2

# ------------------ Redis Setup ------------------
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# ---------------- Cassandra Setup ----------------
@st.cache_resource
def get_cassandra_session():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('monster_arena')
    return session

cassandra_session = get_cassandra_session()

# -------------- PostgreSQL Setup ----------------
@st.cache_resource
def get_postgres_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="monster_arena",
        user="basmalayasser",      # update as needed
        password="Abeersherif2004"   # update as needed
    )
    conn.autocommit = True
    return conn

pg_conn = get_postgres_connection()

# ---------------- Game Logic ----------------------
HIT_DAMAGE_PERCENT = 5  # fixed damage per hit %

# Initialize health bars in session state
if "health_p1" not in st.session_state:
    st.session_state.health_p1 = 100
if "health_p2" not in st.session_state:
    st.session_state.health_p2 = 100
if "game_over" not in st.session_state:
    st.session_state.game_over = False
if "winner" not in st.session_state:
    st.session_state.winner = ""

def reset_game():
    st.session_state.health_p1 = 100
    st.session_state.health_p2 = 100
    st.session_state.game_over = False
    st.session_state.winner = ""

def player_hit(attacker_id, defender_id):
    damage = HIT_DAMAGE_PERCENT
    kills = 1
    playtime = 0
    resources = 0
    now = datetime.now()

    # Update Redis leaderboard
    redis_client.zincrby("leaderboard:game:1", damage, attacker_id)

    # Update Cassandra player stats
    cassandra_session.execute("""
        INSERT INTO player_statistics (player_id, snapshot_time, kills, damage_dealt, playtime_seconds, resources_collected)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (attacker_id, now, kills, damage, playtime, resources))

    # Cassandra game event
    details = json.dumps({"from": attacker_id, "to": defender_id, "damage": damage})
    cassandra_session.execute("""
        INSERT INTO game_analytics_events (event_id, event_type, event_time, player_id, game_id, details)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (uuid.uuid4(), "hit", now, attacker_id, 1, details))

    # Update PostgreSQL player stats table
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO player_stats (player_id, timestamp, kills, damage, playtime, resources)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (attacker_id, now, kills, damage, playtime, resources))
    except Exception as e:
        st.error(f"PostgreSQL insert error: {e}")

    return damage

def get_leaderboard():
    entries = redis_client.zrevrange("leaderboard:game:1", 0, 9, withscores=True)
    return [(pid, score) for pid, score in entries]

def get_latest_cassandra_stats(player_id):
    try:
        rows = cassandra_session.execute("""
            SELECT * FROM player_statistics WHERE player_id = %s LIMIT 5
        """, (player_id,))
        return list(rows)
    except Exception as e:
        st.error(f"Cassandra query error: {e}")
        return []

def get_latest_postgres_stats(player_id):
    try:
        with pg_conn.cursor() as cursor:
            cursor.execute("""
                SELECT timestamp, kills, damage FROM player_stats
                WHERE player_id = %s ORDER BY timestamp DESC LIMIT 5
            """, (player_id,))
            return cursor.fetchall()
    except Exception as e:
        st.error(f"PostgreSQL query error: {e}")
        return []

# ---------------- Streamlit UI -------------------
st.title("👻 Monster Arena Game with Redis, Cassandra & PostgreSQL")

player1 = st.number_input("Player 1 ID", min_value=1, value=1)
player2 = st.number_input("Player 2 ID", min_value=1, value=2)

st.markdown("### Player Health:")

col_health1, col_health2 = st.columns(2)
with col_health1:
    st.progress(st.session_state.health_p1 / 100)
    st.write(f"Player {player1}: {st.session_state.health_p1}% health")
with col_health2:
    st.progress(st.session_state.health_p2 / 100)
    st.write(f"Player {player2}: {st.session_state.health_p2}% health")

if st.session_state.game_over:
    st.success(f"🎉 Player {st.session_state.winner} wins the game!")
    if st.button("Restart Game"):
        reset_game()
else:
    st.markdown("### Choose which player hits:")

    col1, col2 = st.columns(2)
    with col1:
        if st.button(f"Player {player1} Hits Player {player2}"):
            damage = player_hit(player1, player2)
            st.session_state.health_p2 = max(st.session_state.health_p2 - damage, 0)
            st.success(f"Player {player1} hits Player {player2} for {damage}% damage!")

            if st.session_state.health_p2 == 0:
                st.session_state.game_over = True
                st.session_state.winner = player1

    with col2:
        if st.button(f"Player {player2} Hits Player {player1}"):
            damage = player_hit(player2, player1)
            st.session_state.health_p1 = max(st.session_state.health_p1 - damage, 0)
            st.success(f"Player {player2} hits Player {player1} for {damage}% damage!")

            if st.session_state.health_p1 == 0:
                st.session_state.game_over = True
                st.session_state.winner = player2

st.markdown("---")

cols = st.columns(3)

with cols[0]:
    st.header("🔥 Redis Leaderboard")
    leaderboard = get_leaderboard()
    for i, (pid, score) in enumerate(leaderboard, start=1):
        st.write(f"{i}. Player {pid} — {int(score)} points")

with cols[1]:
    st.header("📊 Cassandra Player Stats")
    for p in [player1, player2]:
        stats = get_latest_cassandra_stats(p)
        st.write(f"Player {p} recent stats:")
        if stats:
            for s in stats:
                st.write(f"- Time: {s.snapshot_time.strftime('%H:%M:%S')}, Kills: {s.kills}, Damage: {s.damage_dealt}")
        else:
            st.write("- No data yet")

with cols[2]:
    st.header("📈 PostgreSQL Player Stats")
    for p in [player1, player2]:
        stats = get_latest_postgres_stats(p)
        st.write(f"Player {p} recent stats:")
        if stats:
            for s in stats:
                time_str = s[0].strftime('%H:%M:%S') if s[0] else "N/A"
                kills = s[1]
                damage = s[2]
                st.write(f"- Time: {time_str}, Kills: {kills}, Damage: {damage}")
        else:
            st.write("- No data yet")
