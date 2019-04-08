import copy
import json
from os.path import split
from typing import List

from apps.transcoding.ffmpeg.utils import StreamOperator


class FfprobeFormatReport:

    def __init__(self, raw_report: dict):
        self._raw_report = raw_report

    @property
    def stream_types(self):
        streams = self._raw_report['streams']
        streams_dict = dict()

        for stream in streams:
            codec_type = stream['codec_type']
            if codec_type in streams_dict:
                streams_dict[codec_type] = streams_dict[codec_type] + 1
            else:
                streams_dict.update({codec_type: 1})
        return streams_dict

    @property
    def duration(self):
        return FuzzyDuration(self._raw_report['format']['duration'], 10)

    @property
    def start_time(self):
        try:
            return self._raw_report['format']['start_time']
        except KeyError:
            return 'not supported- key does not exists'


    def diff(self, format_report: dict, overrides: dict):
        differences = dict()
        for attr in ['stream_types', 'duration', 'start_time']:
            # TODO zrobic zmienna lokalna
            original_value = getattr(self, attr)
            modified_value = getattr(format_report, attr)

            if 'streams' in overrides and attr in overrides['streams']:
                modified_value = overrides['streams'][attr]

            if 'format' in overrides and attr in overrides['format']:
                modified_value = overrides['format'][attr]

            if modified_value != original_value:
                diff_dict = {
                    'location': 'format',
                    'attribute': attr,
                    'original value': str(original_value),
                    'modified value': str(modified_value),
                }
                differences.update(diff_dict)
        return differences

    def __eq__(self, other):
        return len(self.diff(other, {})) == 0

    @classmethod
    def build(cls, *video_paths: List[str]) -> list:
        dirs_and_basenames = dict()
        for path in video_paths:
            dir = split(path)[0]
            basename = split(path)[1]
            if dir in dirs_and_basenames:
                value = copy.deepcopy(dirs_and_basenames[dir])
                value.append(basename)
                dirs_and_basenames[dir] = value

            else:
                dirs_and_basenames.update({dir: [basename]})
        list_of_reports = []
        stream_operator = StreamOperator()

        for key in dirs_and_basenames:
            metadata = stream_operator.get_metadata(dirs_and_basenames[key], key)
            for path in metadata['data']:
                with open(path) as metadata_file:
                    list_of_reports.append(FfprobeFormatReport(json.loads(metadata_file.read())))
        return list_of_reports  # new, referenced



class FuzzyDuration:

    def __init__(self, duration, tolerance):
        self._duration = duration
        self._tolerance = tolerance

    @property
    def location(self):
        return 'format'

    @property
    def duration(self):
        return self._duration

    def __eq__(self, other):
        duration1 = float(self.duration)
        duration2 = float(other.duration)

        return True if abs(duration1-duration2) <= self._tolerance else False

    def __str__(self):
        return str(self._duration)
