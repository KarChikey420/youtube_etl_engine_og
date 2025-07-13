import os
from dotenv import load_dotenv
from googleapiclient.discovery import build

# Load API key from environment
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API")

# Build YouTube API client
youtube = build("youtube", "v3", developerKey=API_KEY)

def get_channel_videos(channel_id, max_results=10):
    """
    Fetch video IDs from a YouTube channel's uploads playlist.
    
    Args:
        channel_id (str): YouTube channel ID
        max_results (int): Max number of videos to fetch

    Returns:
        list: List of video IDs
    """
    try:
        # Get upload playlist ID
        channel_response = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        ).execute()

        uploads_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        video_ids = []
        next_page_token = None

        # Paginate through playlist to collect video IDs
        while len(video_ids) < max_results:
            playlist_response = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_id,
                maxResults=min(50, max_results - len(video_ids)),
                pageToken=next_page_token
            ).execute()

            for item in playlist_response["items"]:
                video_ids.append(item["contentDetails"]["videoId"])

            next_page_token = playlist_response.get("nextPageToken")
            if not next_page_token:
                break

        return video_ids

    except Exception as e:
        print(f"An error occurred while fetching video IDs: {e}")
        return []

def get_video_stats(video_ids):
    """
    Fetch video statistics (views, likes, comments, etc.) for a list of video IDs.
    
    Args:
        video_ids (list): List of YouTube video IDs

    Returns:
        list: List of video stat dictionaries
    """
    stats = []

    try:
        for i in range(0, len(video_ids), 50):  # YouTube API supports up to 50 IDs per request
            batch_ids = video_ids[i:i + 50]

            video_response = youtube.videos().list(
                part="snippet,statistics",
                id=",".join(batch_ids)
            ).execute()

            for item in video_response["items"]:
                stats.append({
                    "videoId": item['id'],
                    "title": item["snippet"]["title"],
                    "published_at": item["snippet"]["publishedAt"],
                    "views": int(item["statistics"].get("viewCount", 0)),
                    "likes": int(item["statistics"].get("likeCount", 0)),
                    "comments": int(item["statistics"].get("commentCount", 0)),
                })

    except Exception as e:
        print(f"An error occurred while fetching video stats: {e}")

    return stats
