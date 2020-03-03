#!/usr/bin/env python3

import argparse
import urllib.parse
import os.path
import sys
import logging

import youtube_dl

import dal


class ProgressManager:
    NUM_SEARCH_RESULTS = 30
    MAX_CHANNEL_SIZE = 100

    def __init__(self, database: dal.DataAccessLayer):
        self.__database = database

    def fetch_search_job(self):
        for query, wip in self.__database.fetch_new_queries():
            if wip is None:
                start = 0
            else:
                start = int(wip)
            for page in range(start + 1, self.NUM_SEARCH_RESULTS + 1):
                yield query, page

    def mark_search_job(self, job):
        self.__database.set_query_wip(job[0], job[1])
        if job[1] >= self.NUM_SEARCH_RESULTS:
            self.__database.set_query_done(job[0])

    def fetch_channel_job(self):
        for channel_id, wip, size in self.__database.fetch_good_channels():
            if wip is None:
                start = 0
            else:
                start = int(wip)
            for item in range(start + 1, size + 1):
                yield channel_id, item

    def mark_channel_job(self, job):
        self.__database.set_channel_wip(job[0], job[1])
        if job[1] >= self.MAX_CHANNEL_SIZE:
            self.__database.set_channel_done(job[0])

    def fetch_video_job(self):
        return self.__database.fetch_new_videos()

    def mark_video_job(self, job):
        self.__database.set_video_status(job[0],
            dal.DataAccessLayer.STATUS_DONE)

    def has_job(self):
        return len(self.__database.fetch_new_queries()) != 0 \
            or len(self.__database.fetch_good_channels()) != 0


def parse_cmdline():
    p = argparse.ArgumentParser()
    p.add_argument('--dest', required=True, help='Directory to save stuff')
    p.add_argument('--lang', default='en', choices=('en', 'zh-CN'),
        help='Subtitle language.')
    p.add_argument('--ffmpeg', default='ffmpeg', help='ffmpeg path.')
    p.add_argument('--test-url',
        help='Download a given URL, for debugging purposes.')
    p.add_argument('--query-file',
        help='A file containing initial search queries.')
    p.add_argument('--dry-run', action='store_true',
        help='Don\'t download the actual audio file, for debugging purposes.')
    p.add_argument('--forced-align', action='store_true',
        help='Run "forced alignment" post processing step.')
    return p.parse_args()


def build_youtube_options(cmdline):
    os.makedirs(f'{cmdline.dest}/intermediate', exist_ok=True)

    r = {
        'nooverwrites': True,
        'format': 'bestaudio[ext=m4a]',
        'restrictfilenames': True,
        'writeinfojson': True,
        'writesubtitles': True,
        'writeautomaticsub': False,
        'subtitleslangs': [cmdline.lang],
        'subtitlesformat': 'ttml',
        'prefer_ffmpeg': True,
        'outtmpl': f'{cmdline.dest}/intermediate/%(channel_id)s/%(id)s#%(title)s.%(ext)s',
        'youtube_include_dash_manifest': False,
        'socket_timeout': 10,
        'download_archive': f'{cmdline.dest}/downloaded.txt',
        'ignoreerrors': True,
        'continuedl': True,
        'keepvideo': True,
        'skip_download': cmdline.dry_run,
    }

    script_dir = os.path.dirname(os.path.abspath(__file__))
    audio = '{}'
    exec_cmd = f'{sys.executable} {script_dir}/process.py {audio} --dest {cmdline.dest} --lang {cmdline.lang}'
    if cmdline.forced_align:
        exec_cmd += ' --forced-align'
    r['postprocessors'] = [
        {'key': 'FFmpegSubtitlesConvertor', 'format': 'vtt'},
        {'key': 'ExecAfterDownload', 'exec_cmd': exec_cmd}
    ]

    if cmdline.ffmpeg != 'ffmpeg':
        r['ffmpeg_location'] = cmdline.ffmpeg

    return r


def test_download(url, options):
    with youtube_dl.YoutubeDL(options) as yt:
        yt.download([url])


def download_forever(database, cmdline, youtube_options):
    manager = ProgressManager(database)

    while manager.has_job():
        for query, page in manager.fetch_search_job():
            logging.info("Downloading search result page: %s, %d", query, page)
            quoted = urllib.parse.quote(query)
            url = f'https://www.youtube.com/results?sp=EgQIBCgB&q={quoted}&p={page}'
            with youtube_dl.YoutubeDL(youtube_options) as youtube:
                youtube.download([url])
            manager.mark_search_job((query, page))

        # TODO implement channel crawling functionality.
        """
        for job in manager.fetch_channel_job():
            pass
        """

        for video_id, channel_id in manager.fetch_video_job():
            url = f'https://www.youtube.com/watch?v={video_id}'
            with youtube_dl.YoutubeDL(youtube_options) as youtube:
                youtube.download([url])
            manager.mark_video_job((video_id, channel_id))


def main():
    logging.basicConfig(level=logging.INFO)
    cmdline = parse_cmdline()
    options = build_youtube_options(cmdline)

    if cmdline.test_url:
        test_download(cmdline.test_url, options)
        return

    database = dal.DataAccessLayer(f'{cmdline.dest}/db.sqlite3')
    if cmdline.query_file:
        with open(cmdline.query_file) as f:
            for line in f:
                database.add_search_query(line.strip())
    download_forever(database, cmdline, options)


if __name__ == '__main__':
    main()
