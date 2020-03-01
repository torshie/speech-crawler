#!/usr/bin/env python3

import argparse
import os.path
import logging
import wave

import filter


class AudioData:
    def __init__(self, data, path):
        self.__buffer = data
        self.__path = path

    def align(self, begin, end, server):
        pass


def parse_cmdline():
    p = argparse.ArgumentParser()
    p.add_argument('--lang', required=True)
    p.add_argument('--dest', required=True)
    p.add_argument('--ffmpeg', default='ffmpeg')
    p.add_argument('--aligner', default='http://localhost:8765')
    p.add_argument('video_file', required=True)
    return p.parse_args()


def mark_subtitles_missing(video_id):
    pass


def mark_subtitles_invalid(video_id):
    pass


def force_align_subtitles(subtitles):
    pass


def export_subtitles(subtitles):
    pass


def load_video_file(ffmpeg, filename, dest):
    child = os.fork()
    target_path = dest + '/' + os.path.basename(filename)[:-3] + 'wav'
    if child == 0:
        os.execvp(ffmpeg, [ffmpeg, '-i', filename, '-ac', '1', '-r', '16000',
            '-sample_fmt', 's16', target_path])
        raise RuntimeError("Failed to start ffmpeg")

    _, status = os.waitpid(child, 0)
    if status != 0:
        raise RuntimeError("Failed to convert video file %s" % filename)

    audio_file = wave.open(target_path, 'rb')
    frame_count = audio_file.getnframes()
    data = audio_file.readframes(frame_count)
    assert len(data) == frame_count * 2

    return data, target_path


def main():
    logging.basicConfig(level=logging.INFO)
    cmdline = parse_cmdline()
    assert cmdline.video_file.endswith('.m4a')

    dot = cmdline.video_file.find('.')
    assert dot > 0

    video_id = cmdline.video_file[:dot]
    subtitles_file = cmdline.audio_file[:-3] + f'.{cmdline.lang}.vtt'
    if not os.path.isfile(subtitles_file):
        mark_subtitles_missing(video_id)
        return

    subtitles = filter.load_and_filter(subtitles_file)
    if len(subtitles) == 0:
        mark_subtitles_invalid(video_id)
        return

    if not force_align_subtitles(subtitles):
        mark_subtitles_invalid(video_id)
        return

    export_subtitles(subtitles)


if __name__ == '__main__':
    main()
