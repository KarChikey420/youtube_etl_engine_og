import os
from dotenv import load_dotenv
from googleapiclient.discovery import build

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API")

youtube= build("youtube","v3", developerKey=API_KEY)

def get_channel_videos(channel_id, max_results=10):
    
    try:
        
        channel_response = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        ).execute()
        
        uploads_id= channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        video_ids=[]
        next_page_token= None
        
        while len(video_ids)< max_results:
            playlist_response=youtube.playlistItems().list(
                part="snippet",
                playlist_responseId=uploads_id,
                maxResults=min(50,max_results - len(video_ids)),
                next_token=next_page_token
            ).execute()
            
            for item in playlist_response["items"]:
                video_ids.append(item["content Details"]["videoId"])
            
            next_page_token=playlist_response.get("nextPageToken")
            if not next_page_token:
                break
        
        return video_ids
    
    except Exception as e:
        print(f"An error occurred:{e}")
        return[]
    
def get_video_status(video_ids):
    
    states=[]
    
    try:
        for i in range(0, len(video_ids),50):
            batch_ids= video_ids[i:i+50]
            
            video_response=youtube.videos().list(
                part="status",
                id=','.join(batch_ids)
            ).execute()
            
            for item in video_response["items"]:
                states.append({
                    "videoId":item['id'],
                    "title": item["snippet"]["title"],
                    "published_at": item["snippet"]["publishedAt"],
                    "views": int(item["statistics"].get("viewCount", 0)),
                    "likes": int(item["statistics"].get("likeCount", 0)),
                    "comments": int(item["statistics"].get("commentCount", 0)),
                })
                
    except Exception as e:
        print(f"an error occurred: {e}")
    
    return states
            