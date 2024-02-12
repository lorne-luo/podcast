import os
import subprocess
import sys
from random import randint
from threading import Thread

import yt_dlp
from redis import Redis, ConnectionPool
from environs import Env

env = Env()
env.read_env()  # read .env file, if it exists

REDIS_CLOUD_PASSWORD = env.str("REDIS_CLOUD_PASSWORD")
VIDEO_HOME = env.str("VIDEO_HOME")

redis_host = 'redis-12452.c296.ap-southeast-2-1.ec2.cloud.redislabs.com'
redis_port = 12452

pool = ConnectionPool(host=redis_host, port=redis_port, password=REDIS_CLOUD_PASSWORD,
                      max_connections=1,
                      decode_responses=True)
redis_client = Redis(connection_pool=pool)

downloading_threads = {}


def get_video_ids(youtube_channel):
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


def download_videos(video_ids):
    os.makedirs(VIDEO_HOME, exist_ok=True)
    for video_id in video_ids:
        file_path = os.path.join(VIDEO_HOME, f'{video_id}.mp4')
        if os.path.isfile(file_path):
            continue

        download_youtube_audio(video_id, file_path)


def start_download(new_id):
    # launch thread to download
    if new_id in downloading_threads:
        return

    file_path = os.path.join(VIDEO_HOME, f'{new_id}.mp4')
    p = Thread(target=download_youtube_audio, args=(new_id, file_path))
    p.start()
    downloading_threads[new_id] = p


def main(youtube_channel, rtmp):
    playing_id = None
    video_ids = get_video_ids(youtube_channel)
    download_videos(video_ids)
    index = randint(0, len(video_ids))

    while True:
        for new_id in get_video_ids(youtube_channel):
            if new_id not in video_ids and new_id not in downloading_threads:
                start_download(new_id)

        if index == 0:
            playing_id = video_ids[index]
        else:
            if playing_id != video_ids[index % len(video_ids)]:
                playing_id = video_ids[index % len(video_ids)]
            else:
                playing_id = video_ids[(index + 1) % len(video_ids)]

        index += 1
        filepath = os.path.join(VIDEO_HOME, f'{playing_id}.mp4')
        if not os.path.isfile(filepath):
            continue

        commands = f"ffmpeg -hide_banner -nostdin -v error -stats -re -i {filepath} -c:v libx264 -c:a aac -r 24 -g 60 -bufsize 128m -b:v 6000k -b:a 384k -strict -2 -f flv {rtmp}"
        print(f'Start #{index} {os.path.basename(filepath)}')
        subprocess.run(commands, shell=True, cwd=VIDEO_HOME)

        keys = downloading_threads.keys()
        for downloading_id in keys:
            if not downloading_threads[downloading_id].is_alive():
                video_ids.append(downloading_id)
                downloading_threads.pop(downloading_id)


if __name__ == '__main__':
    youtube_channel = sys.argv[1]
    rtmp = redis_client.get(youtube_channel + ':YOUTUBE_RTMP')
    main(youtube_channel, rtmp)
