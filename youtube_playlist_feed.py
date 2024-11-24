#!/usr/bin/env python3
# Full Python Script to Store YouTube Playlists in SQLite and Generate a Feed for AT Protocol

import googleapiclient.discovery
import googleapiclient.errors
import sqlite3
import json

import os
import sys

# Initialize the YouTube API client
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=os.getenv("YOUTUBE_API_KEY"))

try:
    # SQLite database setup
    db_file = "youtube_playlists.db"

    # Create or connect to the database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create tables to store playlist and video data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS playlists (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            id TEXT PRIMARY KEY,
            playlist_id TEXT NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            description TEXT,
            FOREIGN KEY (playlist_id) REFERENCES playlists (id)
        )
    ''')
    conn.commit()

    # Read playlist and channel IDs from JSON file
    json_file = "playlists.json"  # Replace with the path to your JSON file
    playlist_ids = []
    channel_handles_or_ids = []
    if os.path.exists(json_file):
        with open(json_file, "r") as file:
            playlist_data = json.load(file)
        playlist_ids = playlist_data.get("playlist_ids", [])
        channel_handles_or_ids = playlist_data.get("channel_ids", [])

    # Resolve channel handles to channel IDs
    channel_ids = []
    for handle_or_id in channel_handles_or_ids:
        if handle_or_id.startswith('@'):
            # Use YouTube API to resolve handle to channel ID
            request = youtube.search().list(
                part="snippet",
                q=handle_or_id,
                type="channel",
                maxResults=1
            )
            response = request.execute()
            if response["items"]:
                channel_id = response["items"][0]["snippet"]["channelId"]
                channel_ids.append(channel_id)
            else:
                print(f"Could not resolve handle: {handle_or_id}")
        else:
            # Assume it's a channel ID
            channel_ids.append(handle_or_id)

    # Fetch all public playlists for each channel ID
    for channel_id in channel_ids:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50
        )
        response = request.execute()

        for playlist in response["items"]:
            playlist_ids.append(playlist["id"])

    # Fetch details for all playlists
    for playlist_id in playlist_ids:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            id=playlist_id
        )
        response = request.execute()

        # Insert playlists into SQLite database
        for playlist in response["items"]:
            playlist_id = playlist["id"]
            title = playlist["snippet"]["title"]
            description = playlist["snippet"].get("description", "")

            cursor.execute('''
                INSERT OR REPLACE INTO playlists (id, title, description)
                VALUES (?, ?, ?)
            ''', (playlist_id, title, description))

        # Fetch videos for each playlist
        request = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50
        )
        response = request.execute()

        # Insert videos into SQLite database
        for item in response["items"]:
            video_id = item["snippet"]["resourceId"]["videoId"]
            video_title = item["snippet"]["title"]
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            video_description = item["snippet"].get("description", "")

            cursor.execute('''
                INSERT OR REPLACE INTO videos (id, playlist_id, title, url, description)
                VALUES (?, ?, ?, ?, ?)
            ''', (video_id, playlist_id, video_title, video_url, video_description))

    # Commit changes and close the connection
    conn.commit()
    conn.close()

    print(f"Playlists and videos stored successfully in {db_file}")

    # Connect to SQLite database to generate feed
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Query playlists and videos
    cursor.execute("SELECT id, title, description FROM playlists")
    playlists = cursor.fetchall()

    atproto_feed = {"feeds": []}
    for (playlist_id, title, description) in playlists:
        cursor.execute("SELECT title, url, description FROM videos WHERE playlist_id = ?", (playlist_id,))
        videos = cursor.fetchall()
        atproto_feed["feeds"].append({
            "title": title,
            "id": playlist_id,
            "description": description,
            "type": "youtube_playlist",
            "videos": [
                {
                    "title": video_title,
                    "url": video_url,
                    "description": video_description
                } for (video_title, video_url, video_description) in videos
            ]
        })

    # Write feed to JSON
    with open("feed.json", "w") as file:
        json.dump(atproto_feed, file, indent=4)

    # Close database connection
    conn.close()

    print("Feed created successfully in feed.json")

except googleapiclient.errors.HttpError as e:
    print(f"An HTTP error occurred: {e}")
    sys.exit(1)

except sqlite3.Error as e:
    print(f"An SQLite error occurred: {e}")
    sys.exit(1)

except Exception as e:
    print(f"An unexpected error occurred: {e}")
    sys.exit(1)
