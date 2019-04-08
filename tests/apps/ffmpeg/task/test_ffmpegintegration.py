import os
import os
import shutil
import time

import pytest

from apps.transcoding.common import TranscodingTaskBuilderException, \
    ffmpegException
from apps.transcoding.ffmpeg.task import ffmpegTaskTypeInfo
from golem.testutils import TestTaskIntegration
from tests.apps.ffmpeg.task.ffprobe_report import FfprobeFormatReport


class FfmpegIntegrationTestCase(TestTaskIntegration):

    def setUp(self):
        super(FfmpegIntegrationTestCase, self).setUp()

        self.RESOURCES = os.path.join(os.path.dirname(
            os.path.dirname(os.path.realpath(__file__))), 'resources')
        self.tt = ffmpegTaskTypeInfo()
        self.resource_stream = os.path.join(self.RESOURCES, 'test_video2.mp4')

    @classmethod
    def _create_task_def_for_transcoding(cls, resource_stream, result_file):
        return {
            'type': 'FFMPEG',
            'name': os.path.splitext(os.path.basename(result_file))[0],
            'timeout': '0:10:00',
            'subtask_timeout': '0:09:50',
            'subtasks_count': 2,
            'bid': 1.0,
            'resources': [resource_stream],
            'options': {
                'output_path': os.path.dirname(result_file),
                'video': {
                    'codec': 'h265',
                    'resolution': [320, 240],
                    'frame_rate': "25"
                },
                'container': os.path.splitext(result_file)[1][1:]
            }
        }

    def tearDown(self):
        super(FfmpegIntegrationTestCase, self).tearDown()

        for element in os.listdir(self.RESOURCES):
            if element in ['metadata_output', 'metadata_work']:
                shutil.rmtree(os.path.join(self.RESOURCES, element))
        try:
            shutil.rmtree(self.root_dir)
            os.mkdir(self.root_dir)
        except:
            pass

        # TODO above - fix it :)




class TestffmpegIntegration(FfmpegIntegrationTestCase):


    def _transcode_and_get_metadata_both_files(self, source_path, result_path):
        task_def = self._create_task_def_for_transcoding(source_path, result_path)

        self.execute_task(task_def)

        (report_new, report_referenced) = FfprobeFormatReport.build(source_path,
                                                                    result_path)
        self.assertEqual(report_referenced, report_new)



    def test_multiple_videos_split_and_merge_returns_valid_video(self):


        files = [
            "wmv/Catherine_Part1(codec=wmv3).wmv",
            "wmv/Video1(codec=wmv1).wmv",
            "wmv/grb_2(codec=wmv2)_video_stream_only.wmv",
            "wmv/small(codec=wmv2).wmv",
            "mov/DLP_PART_2_768k[codec=h264].mov",
            "mov/P6090053[codec=mjpeg].mov",
            "mov/small[codec=h264].mov",
            "flv/grb_2[codec=flv]_only_video_stream.flv",
            "flv/video-sample[codec=flv]!_audio_codec_unknow!.flv",
            "flv/jellyfish-25-mbps-hd-hevc[codec=flv]_only_video_stream.flv",
            "flv/star_trails[codec=flv].flv",
            "flv/page18-movie-4[codec=flv].flv",
            "mpeg/lion-sample[codec=mpeg1video].mpeg",
            "mpeg/small[codec=mpeg2video]_3_streams_!_Unsupported codec_!.mpeg",
            "mpeg/TRA3106[codec=mpeg2video]_video_and_data_stream_!_Unsupported codec_!.mpeg",
            "mpeg/metaxas-keller-Bell[codec=mpeg2video]_3_streams_!_Unsupported codec_!.mpeg",
            "mts/small(codec=h264).mts",
            "mts/Panasonic_HDC_TM_700_P_50i(codec=h264)_video_audio_subtitles_streams.mts",
            "mts/video-sample(codec=h264)_only_video_stream.mts",
            "mpg/TRA3106[codec=mpeg2video]_video_and_data_stream_@_Unsupported codec_@.mpg",
            "mpg/video-sample[codec=mpeg2video]_video_and_data_stream_@_Unsupported codec_@.mpg",
            "mpg/grb_2[codec=mpeg2video]_only_video_stream.mpg",
            "mpg/small[codec=mpeg2video]_3_streams_@_Unsupported codec_@.mpg",
            "3gp/star_trails[codec=h263].3gp",
            "3gp/TRA3106[codec=h263]_only_video_stream.3gp",
            "3gp/small[codec=h263].3gp",
            "3gp/page18-movie-4[codec=h263].3gp",
            "3gp/dolbycanyon[codec=h263].3gp",
            "3gp/jellyfish-25-mbps-hd-hevc[codec=h263]_only_video_stream.3gp",
            "m4v/page18-movie-4[codec=h264]_audio_video_index_reverse.m4v",
            "m4v/small[codec=h264].m4v",
            "m4v/grb_2[codec=h264]_only_video_stream.m4v",
            "m4v/dolbycanyon[codec=h264].m4v",
            "vob/small(codec=mpeg2video).vob",
            "vob/grb_2(codec=mpeg2video)_video_stream_only.vob",
            "vob/dolbycanyon(codec=mpeg2video)_3_streams_@_Unsupported codec_@.vob",
            "webm/star_trails(codec=vp9).webm",
            "webm/small(codec=vp8).webm",
            "mkv/dolbycanyon[codec=h264].mkv",
            "mkv/jellyfish-25-mbps-hd-hevc[codec=hevc]_only_video_stream.mkv",
            "mkv/small[codec=h264].mkv",
            "avi/Panasonic_HDC_TM_700_P_50i[codec=mpeg4].avi",
            "avi/small[codec=mpeg4].avi",
            "avi/TRA3106[codec=mjpeg]_only_video_stream.avi",
            "avi/page18-movie-4[codec=mpeg4].avi",
            "mp4/P6090053[codec=h264].mp4",
            "mp4/dolbycanyon[codec=h264].mp4",
            "mp4/small[codec=h264].mp4",
        ]

        print('\n\n')
        for file in files:

            filename_to_print = file

            source_path = os.path.join(self.RESOURCES, 'standaloneinstaller', file)
            result_path = os.path.join(self.root_dir, "transcoded-" + file)
            task_def = self._create_task_def_for_transcoding(source_path,
                                                             result_path)
            try:
                self.execute_task(task_def)
            except Exception as e:
                print(f'|{filename_to_print:85}|{str(e):50}|')
            else:
                reports = FfprobeFormatReport.build(source_path, result_path)

                report_new = reports[0]
                report_referenced = reports[1]

                diff = report_referenced.diff(report_new, {})
                if diff == {}:
                    print(f'|{filename_to_print:85}|{"OK":50}|')
                else:
                    print(f'|{filename_to_print:85}|difference in: {diff["attribute"]:35}|')






    def test_split_and_merge_should_return_valid_video(self):
        source_path = os.path.join(self.RESOURCES, 'test_video.mp4')
        result_path = os.path.join(self.root_dir, 'test_simple_case.mp4')

        task_def = self._create_task_def_for_transcoding(source_path, result_path)
        self.execute_task(task_def)

        (report_new, report_referenced) = FfprobeFormatReport.build(source_path, result_path)
        self.assertEqual(report_referenced, report_new)

    def test_split_and_merge_with_resolution_change_should_return_valid_video(self):
        source_path = os.path.join(self.RESOURCES, 'test_video.mp4')
        result_path = os.path.join(self.root_dir, 'test_simple_case.mp4')

        task_def = self._create_task_def_for_transcoding(source_path, result_path)
        task_def['options']['video']['resolution'] = [640, 480]

        self.execute_task(task_def)

        (report_new, report_referenced) = FfprobeFormatReport.build(source_path, result_path)
        self.assertEqual(report_referenced, report_new)

    def test_compare_two_videos_with_different_streams(self):

        source_path = os.path.join(self.RESOURCES, 'test5.mkv')
        result_path = os.path.join(self.RESOURCES, 'test5_shuffled.mkv')

        overrides = {'streams': {'stream_types': {'video': 1, 'audio': 2, 'subtitle': 8}}}

        (report_new, report_referenced) = FfprobeFormatReport.build(source_path, result_path)

        self.assertEqual({}, report_referenced.diff(report_new, overrides))

    def test_simple_case(self):
        result_file = os.path.join(self.root_dir, 'test_simple_case.mp4')
        task_def = self._create_task_def_for_transcoding(self.resource_stream, result_file)

        self.execute_task(task_def)

        self.run_asserts([
            self.check_file_existence(result_file)])

    def test_nonexistent_output_dir(self):
        result_file = os.path.join(self.root_dir, 'nonexistent', 'path',
                                   'test_invalid_task_definition.mp4')
        task_def = self._create_task_def_for_transcoding(self.resource_stream, result_file)

        self.execute_task(task_def)

        self.run_asserts([
            self.check_file_existence(result_file)])

    def test_nonexistent_resource(self):
        resource_stream = os.path.join(self.RESOURCES,
                                       'test_nonexistent_video.mp4')
        result_file = os.path.join(self.root_dir, 'test_nonexistent_video.mp4')
        task_def = self._create_task_def_for_transcoding(resource_stream, result_file)

        with self.assertRaises(TranscodingTaskBuilderException) as e:
            self.execute_task(task_def)
        assert str(e.exception) == f'{resource_stream} does not exist'

    def test_invalid_resource_stream(self):
        resource_stream = os.path.join(self.RESOURCES, 'invalid_test_video.mp4')
        result_file = os.path.join(self.root_dir,
                                   'test_invalid_resource_stream.mp4')

        task_def = self._create_task_def_for_transcoding(resource_stream, result_file)

        with self.assertRaises(ffmpegException) as e:
            self.execute_task(task_def)
        assert str(e.exception) == 'Subtask computation failed with exit code 1'

    def test_task_invalid_params(self):
        result_file = os.path.join(self.root_dir, 'test_invalid_params.mp4')
        task_def = self._create_task_def_for_transcoding(self.resource_stream, result_file)
        task_def['options']['video']['codec'] = 'abcd'

        with self.assertRaises(TranscodingTaskBuilderException) as e:
            self.execute_task(task_def)
        assert str(e.exception) == 'abcd is not supported'
