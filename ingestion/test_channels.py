"""
Test all 9 Kenyan YouTube channel IDs
Run: python test_channels.py
"""
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path('..') / '.env')
load_dotenv()

import os
from googleapiclient.discovery import build

youtube = build(
    'youtube', 'v3',
    developerKey=os.getenv('YOUTUBE_API_KEY'),
    cache_discovery=False
)

CHANNELS = [
    ('Citizen TV Kenya',  'UChBQgieUidXV1CmDxSdRm3g'),
    ('NTV Kenya',         'UCqBJ47FjJcl61fmSbcadAVg'),
    ('KTN News Kenya',    'UCKVsdeoHExltrWMuK0hOWmg'),
    ('Churchill Show',    'UC0YG5UA1s2gLb5tDfUCIWtg'),
    ('Jalango TV',        'UCFG1zHs55s1my124O3Nk9DQ'),
    ('Oga Obinna',        'UCe68ABxGwMZO3J8y_gerZ6A'),
    ('Switch TV Kenya',   'UCUhrpGr_luwUzVaxiW5Jkhw'),
    ('KBC Channel 1',     'UCypNjM5hP1qcUqQZe57jNfg'),
    ('Willy Paul',        'UCgdVgtJQXxebSiSAzlhYczw'),
]

print("\nTesting all 9 Kenyan YouTube channels...\n")
ok   = 0
fail = 0

for name, channel_id in CHANNELS:
    try:
        response = youtube.channels().list(
            part='statistics',
            id=channel_id
        ).execute()

        if response.get('items'):
            stats = response['items'][0]['statistics']
            subs  = int(stats.get('subscriberCount', 0))
            views = int(stats.get('viewCount', 0))
            print(f"  OK   {name:<25} {subs:>12,} subs  |  {views:>15,} views")
            ok += 1
        else:
            print(f"  FAIL {name:<25} no data returned - wrong channel ID")
            fail += 1

    except Exception as e:
        print(f"  ERR  {name:<25} {e}")
        fail += 1

print(f"\n{'='*65}")
print(f"  Results: {ok} passed  |  {fail} failed")
print(f"{'='*65}\n")
