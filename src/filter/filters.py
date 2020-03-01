# This file is mostly copied from https://github.com/EgorLakomkin/KTSpeechCrawler/blob/master/crawler/filters.py

import re

from .youtube_helpers import remove_overlapping_subtitles, \
    normalize_subtitle, leave_alphanum_characters, merge_subtitles, load_all_subtitles


class Pipeline:
    """
    Pipeline class storing and applying list of filters to the input video
    """
    def __init__(self,  lst_components):
        super(Pipeline, self).__init__()
        self.lst_components = lst_components

    def __call__(self, data):
        result = data
        for component in self.lst_components:
            result = component(result)
        return result


class BaseFilter:
    def validate(self, input):
        raise NotImplementedError

    def __call__(self, input):
        raise NotImplementedError


class OverlappingSubtitlesRemover(BaseFilter):
    def __init__(self):
        super(OverlappingSubtitlesRemover, self).__init__()

    def __call__(self, input):
        subtitles = input['subtitles']
        input['subtitles'] = remove_overlapping_subtitles(subtitles)
        return input


class SubtitleMerger(BaseFilter):
    def __init__(self, min_gap_to_split_sec = 1.0, max_len_merged_sec = 15):
        super(SubtitleMerger, self).__init__()
        self.min_gap_to_split_sec = min_gap_to_split_sec
        self.max_len_merged_sec = max_len_merged_sec

    def __call__(self, input):
        subtitles = input['subtitles']
        input['subtitles'] = merge_subtitles(subtitles, min_dist=self.min_gap_to_split_sec,
                                             max_dist=self.max_len_merged_sec)
        return input


DEFAULT_BLACKLIST_CHARACTERS = {"♪", "♬", "♫"}


class SubtitleCaptionTextFilter(BaseFilter):
    def __init__(self, blacklisted_chars=None):
        super(SubtitleCaptionTextFilter, self).__init__()
        self.blacklist_chars = blacklisted_chars or DEFAULT_BLACKLIST_CHARACTERS

    def __call__(self, input):
        subtitles = input['subtitles']
        input['subtitles'] = list(filter(lambda s: all(s['original_phrase'].find(c) == -1
                                                  for c in self.blacklist_chars),
                                                  subtitles))
        return input


class MinNumberSubtitlesFilter(BaseFilter):
    def __init__(self, threshold=3):
        self.threshold = threshold

    def validate(self, input):
        assert 'subtitles' in input

    def __call__(self, input):
        return len(input['subtitles']) > self.threshold


class CaptionRegexMatcher(BaseFilter):
    def __init__(self, regexp):
        super(CaptionRegexMatcher, self).__init__()
        self.regexp = regexp

    def __call__(self, input):
        subtitles = input['subtitles']
        input['subtitles'] = list(filter(lambda s: re.match(self.regexp, s['original_phrase']) is not None, subtitles))
        return input


class CaptionNormalizer(BaseFilter):
    def __call__(self, input):
        for sub_info in input['subtitles']:
            sub_info['original_phrase'] =  normalize_subtitle(sub_info["original_phrase"])
        return input


class CaptionLengthFilter(BaseFilter):

    def __init__(self, min_length=None, max_length=None):
        super(CaptionLengthFilter, self).__init__()
        self.min_filter_func = lambda x: len(x.split()) >= min_length if min_length else lambda x: True
        self.max_filter_func = lambda x: len(x.split()) <= max_length if max_length else lambda x: True


    def __call__(self, input):
        subtitles = input['subtitles']
        input['subtitles'] = list(filter(lambda s: self.min_filter_func(s['original_phrase'])
                                  and self.max_filter_func(s['original_phrase']), subtitles))
        return input


class CaptionDurationFilter(BaseFilter):
    def __init__(self, min_length=None, max_length=None):
        super(CaptionDurationFilter, self).__init__()
        self.min_filter_func = lambda x: x['duration'] >= min_length if min_length else lambda x: True
        self.max_filter_func = lambda x: x['duration'] <= max_length if max_length else lambda x: True

    def __call__(self, input):
        subtitles = input['subtitles']
        input['subtitles'] = list(filter(lambda s: self.min_filter_func(s)
                                  and self.max_filter_func(s), subtitles))
        return input


class CaptionLeaveOnlyAlphaNumCharacters(BaseFilter):
    def __call__(self, input):
        for sub_info in input['subtitles']:
            sub_info['original_phrase'] =  leave_alphanum_characters(sub_info["original_phrase"])
        return input


def load_and_filter(filename):
    subtitles = load_all_subtitles(filename)
    print(len(subtitles))
    src = {
        'subtitles': subtitles,
        'video_file': ''
    }
    good_chars_regexp = re.compile(
        r"^[A-Za-z0-9\,\.\-\?\"\'\’\!\“\s\;\:\“\”\–\‘\’\’\/\\]+$",
        re.IGNORECASE)
    pipeline = Pipeline([
        OverlappingSubtitlesRemover(),
        SubtitleCaptionTextFilter(),
        CaptionNormalizer(),
        CaptionRegexMatcher(good_chars_regexp),
        CaptionLengthFilter(min_length=5),
        CaptionLeaveOnlyAlphaNumCharacters(),
        SubtitleMerger(max_len_merged_sec=10),
        CaptionDurationFilter(min_length=1, max_length=20.0)
    ])
    return pipeline(src)


def test():
    import sys

    from youtube_helpers import load_all_subtitles

    subtitles = load_all_subtitles(sys.argv[1])
    print(len(subtitles))
    input = {
        'subtitles': subtitles,
        'video_file': ''
    }
    good_chars_regexp = re.compile(
        r"^[A-Za-z0-9\,\.\-\?\"\'\’\!\“\s\;\:\“\”\–\‘\’\’\/\\]+$",
        re.IGNORECASE)
    pipeline = Pipeline([
        OverlappingSubtitlesRemover(),
        SubtitleCaptionTextFilter(),
        CaptionNormalizer(),
        CaptionRegexMatcher(good_chars_regexp),
        CaptionLengthFilter(min_length=5),
        CaptionLeaveOnlyAlphaNumCharacters(),
        SubtitleMerger(max_len_merged_sec=10),
        CaptionDurationFilter(min_length=1, max_length=20.0)
    ])
    processed_subtitles = pipeline(input)
    print(len(processed_subtitles['subtitles']))
    for s in processed_subtitles['subtitles']:
        print(s)


if __name__ == "__main__":
    test()
