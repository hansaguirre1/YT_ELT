import requests
import json

import os
from dotenv import load_dotenv

API_KEY=os.getenv("./.env")
CHANNEL_HANDLE='MrBeast'

def get_playlist_id():
    try:
        url=f'https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forUsername={CHANNEL_HANDLE}&key={API_KEY}'

        response=requests.get(url)

        response.raise_for_status()

        data=response.json()

        #print(json.dumps(data, indent=4))

        channel_items=data["items"][0]
        channel_playlistId=channel_items["contentDetails"]["relatedPlaylists"]['uploads']

        print(channel_playlistId)
        
        return channel_playlistId
    except requests.exceptions.RequestException as e:
        raise e
    
if __name__=='__nain__':
    get_playlist_id()

