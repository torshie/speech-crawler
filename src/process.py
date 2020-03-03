#!/usr/bin/env python3

import argparse
import os.path
import logging
import wave
import sys
import datetime
import io

import requests

import filter
import dal


class AudioData:
    SAMPLE_RATE = 16
    SAMPLE_SIZE = 2

    def __init__(self, data: bytes):
        self.__wave_bytes = data

    def get_duration_ms(self):
        return len(self.__wave_bytes) // self.SAMPLE_SIZE // self.SAMPLE_RATE

    def export(self, start, end, output_file=None):
        if isinstance(start, datetime.time):
            start = get_ms(start)
        if isinstance(end, datetime.time):
            end = get_ms(end)
        start_offset = self.__timestamp_to_offset(start)
        end_offset = self.__timestamp_to_offset(end)

        content = self.__wave_bytes[start_offset:end_offset]
        if output_file is None:
            return content

        wave_file = wave.open(output_file, 'wb')
        wave_file.setnchannels(1)
        wave_file.setnframes((end_offset - start_offset) // 2)
        wave_file.setsampwidth(self.SAMPLE_SIZE)
        wave_file.setframerate(self.SAMPLE_RATE * 1000)
        wave_file.writeframes(content)
        wave_file.close()

    def __timestamp_to_offset(self, milliseconds):
        sample_offset = milliseconds * self.SAMPLE_RATE
        byte_offset = sample_offset * self.SAMPLE_SIZE
        if byte_offset >= len(self.__wave_bytes):
            byte_offset = len(self.__wave_bytes)
        return byte_offset


def get_ms(ts: datetime.time):
    return ts.hour * 3600 * 1000 + ts.minute * 60 * 1000 + ts.second * 1000 \
        + ts.microsecond // 1000


def parse_cmdline():
    p = argparse.ArgumentParser()
    p.add_argument('--lang', required=True)
    p.add_argument('--dest', required=True)
    p.add_argument('--ffmpeg', default='ffmpeg')
    p.add_argument('--alignment-service', default='http://localhost:8765')
    p.add_argument('--forced-align', action='store_true')
    p.add_argument('--fix-data', action='store_true')
    p.add_argument('video_file')
    return p.parse_args()


def mark_subtitles_missing(video_id, video_file, database: dal.DataAccessLayer):
    database.set_video_status(video_id, database.STATUS_SUBS_MISSING)
    os.remove(video_file)


def mark_subtitles_invalid(video_id, video_file, database: dal.DataAccessLayer):
    database.set_video_status(video_id, database.STATUS_INVALID_SUBS)
    os.remove(video_file)


def adjust_subtitle(sub, alignment, audio_start):
    correct = 0
    words = alignment['words']
    for word in words:
        if word['case'] == 'success':
            correct += 1
    ratio = correct / len(alignment['words'])

    if ratio < 0.8:
        print('Very bad success ratio')
        return False

    if words[0]['case'] != 'success' or words[-1]['case'] != 'success':
        return False

    actual_start = int(words[0]['start'] * 1000) + audio_start - 10
    actual_end = int(words[-1]['end'] * 1000) + audio_start + 10

    sub['ts_start'] = actual_start
    sub['ts_end'] = actual_end

    return True


def force_align_subtitles(subtitles, aligner, audio_data: AudioData):
    for i, sub in enumerate(subtitles['subtitles']):
        start = get_ms(sub['ts_start'])
        start -= 1000
        if start < 0:
            start = 0
        end = get_ms(sub['ts_end'])
        end += 1000
        with io.BytesIO() as f:
            audio_data.export(start, end, output_file=f)
            wav_data = f.getvalue()
            post_files = {
                'audio': ('audio.wav', wav_data, 'audio/wav'),
                'transcript': ('transcript.txt', sub['original_phrase'])
            }
        response = requests.post(aligner + '/transcriptions?async=false',
                files=post_files)
        alignment = response.json()
        adjust_subtitle(sub, alignment, start)

    return False


def export_subtitles(video_id, subtitles, database: dal.DataAccessLayer):
    for sub in subtitles['subtitles']:
        if isinstance(sub['ts_start'], datetime.time):
            start = get_ms(sub['ts_start'])
        else:
            start = sub['ts_start']
        if isinstance(sub['ts_end'], datetime.time):
            end = get_ms(sub['ts_end'])
        else:
            end = sub['ts_end']
        database.add_subtitle(video_id, sub['original_phrase'].lower(),
            start, end)


def load_video_file(ffmpeg, filename, dest):
    child = os.fork()
    if not os.path.isdir(dest + '/wav/'):
        os.makedirs(dest + '/wav/', exist_ok=True)
    target_path = dest + '/wav/' + os.path.basename(filename)[:-3] + 'wav'
    if child == 0:
        os.execvp(ffmpeg, [ffmpeg, '-y', '-i', filename, '-ac', '1',
            '-ar', '16000', '-sample_fmt', 's16', target_path])
        raise RuntimeError("Failed to start ffmpeg")

    _, status = os.waitpid(child, 0)
    if status != 0:
        raise RuntimeError("Failed to convert video file %s" % filename)

    audio_file = wave.open(target_path, 'rb')
    frame_count = audio_file.getnframes()
    data = audio_file.readframes(frame_count)
    assert len(data) == frame_count * 2

    return data, target_path


def get_id(video_path, dest):
    name = video_path[len(dest) + 1:]
    channel_id = os.path.dirname(name)
    basename = os.path.basename(name)
    sharp = basename.find('#')
    assert sharp > 0
    video_id = basename[:sharp]

    return video_id, channel_id


def main():
    logging.basicConfig(level=logging.INFO)
    cmdline = parse_cmdline()
    assert cmdline.video_file.endswith('.m4a')

    database = dal.DataAccessLayer(f'{cmdline.dest}/db.sqlite3')
    video_id, channel_id = get_id(cmdline.video_file, cmdline.dest)
    if not database.add_video(video_id, channel_id):
        if not cmdline.fix_data:
            return

    subtitles_file = cmdline.video_file[:-3] + f'{cmdline.lang}.vtt'
    if not os.path.isfile(subtitles_file):
        mark_subtitles_missing(video_id, cmdline.video_file, database)
        return

    subtitles = filter.load_and_filter(subtitles_file)
    if len(subtitles) == 0:
        mark_subtitles_invalid(video_id, cmdline.video_file, database)
        return

    raw_audio, wav_path = \
        load_video_file(cmdline.ffmpeg, cmdline.video_file, f'{cmdline.dest}')
    audio_data = AudioData(raw_audio)

    if cmdline.forced_align:
        if not force_align_subtitles(subtitles, cmdline.alignment_service,
                audio_data):
            mark_subtitles_invalid(video_id, cmdline.video_file, database)
            return

    database.set_video_length(video_id, audio_data.get_duration_ms())
    export_subtitles(video_id, subtitles, database)


def test_export():
    wave_file = wave.open(sys.argv[1], 'rb')
    content = wave_file.readframes(wave_file.getnframes())
    audio = AudioData(content)
    data = audio.export(int(1000 * float(sys.argv[2])), int(1000 * float(sys.argv[3])))
    with open(sys.argv[4], 'wb') as f:
        f.write(data)


if __name__ == '__main__':
    main()
