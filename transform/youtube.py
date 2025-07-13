import pandas as pd
from collections import Counter
import re

def clean_title(title):
    """
    Lowercase and remove non-alphanumeric characters from a title.
    """
    title = title.lower()
    title = re.sub(r'[^a-z0-9\s]', '', title)
    return title.strip()

def extract_keywords(titles):
    """
    Extract most frequent keywords from a list of video titles.
    """
    words = []
    for t in titles:
        words.extend(clean_title(t).split())

    stopwords = {
        'the', 'a', 'and', 'of', 'to', 'in', 'on', 'for', 'with',
        'how', 'what', 'is', 'this', 'that', 'your', 'you', 'my'
    }
    keywords = [word for word in words if word not in stopwords and len(word) > 2]
    return Counter(keywords).most_common(10)

def analyze_publish_times(df):
    """
    Identify best day(s) and hour(s) to publish videos based on top performers.
    """
    df['published_at'] = pd.to_datetime(df['published_at'])
    df['day'] = df['published_at'].dt.day_name()
    df['hour'] = df['published_at'].dt.hour

    top_days = df['day'].value_counts().head(2).index.tolist()
    top_hours = df['hour'].value_counts().head(2).index.tolist()
    return top_days, top_hours

def suggest_video_ideas(df):
    """
    Generate suggestions for future videos based on past video performance.
    
    Args:
        df (pd.DataFrame): DataFrame with YouTube video stats
    
    Returns:
        dict: suggestions including top keywords, best publish times, and engaging titles
    """
    # Get top 10 most viewed videos
    top_videos = df.sort_values(by="views", ascending=False).head(10)

    # Extract keywords from top titles
    top_keywords = extract_keywords(top_videos["title"].tolist())

    # Analyze top publish times
    best_days, best_hours = analyze_publish_times(top_videos)

    # Calculate engagement score (likes + comments per view)
    df["engagement"] = (df["likes"] + df["comments"]) / df["views"].replace(0, 1)
    engaging_titles = df.sort_values(by="engagement", ascending=False).head(5)["title"].tolist()

    return {
        "suggested_keywords": [kw[0] for kw in top_keywords],
        "best_days": best_days,
        "best_hours": best_hours,
        "engaging_titles": engaging_titles
    }
