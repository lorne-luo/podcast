import os
import subprocess
import sys

import yt_dlp
from redis import Redis
from environs import Env
from downloader import download_youtube_audio

env = Env()
env.read_env()  # read .env file, if it exists

REDIS_CLOUD_PASSWORD = env.str("REDIS_CLOUD_PASSWORD")
VIDEO_HOME = env.str("VIDEO_HOME")

redis_host = 'redis-12452.c296.ap-southeast-2-1.ec2.cloud.redislabs.com'
redis_port = 12452


def get_video_ids(youtube_channel):
    redis_client = Redis(host=redis_host, port=redis_port, password=REDIS_CLOUD_PASSWORD, decode_responses=True)
    return redis_client.json().get(youtube_channel + ':VIDEO_IDS')


def download_youtube_audio(youtube_id, output):
    # yt-dlp -x --audio-format mp3 --audio-quality 0 'https://www.youtube.com/watch?v=s9gRg3_A-RM'
    ydl_opts = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
        'quiet': True,
        "format": "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "writesubtitles": False,  # Enable subtitle download
        "allsubtitles": False,
        "keepvideo": False,
        "outtmpl": output,
        # Extract audio using ffmpeg
        # 'postprocessors': [{
        #     'key': 'FFmpegExtractAudio',
        #     'preferredcodec': 'm4a',
        # }]
    }
    with yt_dlp.YoutubeDL(ydl_opts) as downloader:
        downloader.download([youtube_id])


def download(youtube_channel):
    os.makedirs(VIDEO_HOME, exist_ok=True)
    for video_id in get_video_ids(youtube_channel):
        file_path = os.path.join(VIDEO_HOME, f'{video_id}.mp4')
        if os.path.isfile(file_path):
            continue

        download_youtube_audio(video_id, file_path)


def main(youtube_channel, rtmp):
    playing_id = None
    index = 0
    while True:
        download(youtube_channel)
        video_ids = get_video_ids(youtube_channel)
        if index == 0:
            playing_id = video_ids[index]
        else:
            if playing_id != video_ids[index % len(video_ids)]:
                playing_id = video_ids[index % len(video_ids)]
            else:
                playing_id = video_ids[(index + 1) % len(video_ids)]

        filepath = os.path.join(VIDEO_HOME, f'{playing_id}.mp4')
        commands = f"ffmpeg -re -i {filepath} -c:v libx264 -c:a aac -b:a 192k -strict -2 -f flv {rtmp}"

        print(f'start to podcast {os.path.basename(filepath)}')
        subprocess.run(commands, shell=True, cwd=VIDEO_HOME)


if __name__ == '__main__':
    youtube_channel = sys.argv[1]
    redis_client = Redis(host=redis_host, port=redis_port, password=REDIS_CLOUD_PASSWORD, decode_responses=True)

    rtmp = redis_client.get(youtube_channel + ':YOUTUBE_RTMP')

    main(youtube_channel, rtmp)
