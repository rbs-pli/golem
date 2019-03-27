import os
import os
import shutil

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





    @staticmethod
    def _create_task_def(resource_stream, result_file):
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


class TestffmpegIntegration(FfmpegIntegrationTestCase):

    def test_compare_two_videos_with_different_streams(self):

        source_path = os.path.join(self.RESOURCES, 'test5.mkv')
        result_path = os.path.join(self.RESOURCES, 'test5_shuffled.mkv')

        overrides = {'streams': {'stream_types': {'video': 1, 'audio': 2, 'subtitle': 8}}}

        (report_new, report_referenced) = FfprobeFormatReport.build(source_path, result_path)

        self.assertEqual({}, report_referenced.diff(report_new, overrides))


    def test_split_and_merge_should_return_valid_video(self):

        source_path = os.path.join(self.RESOURCES, 'test_video.mp4')
        result_path = os.path.join(self.root_dir, 'test_simple_case.mp4')

        task_def = self._create_task_def(self.resource_stream, result_path)
        self.execute_task(task_def)

        overrides = {'streams': {'stream_types': {'video': 1, 'audio': 2, 'subtitle': 8}}}
        (report_new, report_referenced) = FfprobeFormatReport.build(source_path, result_path)
        self.assertEqual({}, report_referenced.diff(report_new, overrides))



    def test_simple_case(self):
        result_file = os.path.join(self.root_dir, 'test_simple_case.mp4')
        task_def = self._create_task_def(self.resource_stream, result_file)

        self.execute_task(task_def)

        self.run_asserts([
            self.check_file_existence(result_file)])



    def test_nonexistent_output_dir(self):
        result_file = os.path.join(self.root_dir, 'nonexistent', 'path',
                                   'test_invalid_task_definition.mp4')
        task_def = self._create_task_def(self.resource_stream, result_file)

        self.execute_task(task_def)

        self.run_asserts([
            self.check_file_existence(result_file)])

    def test_nonexistent_resource(self):
        resource_stream = os.path.join(self.RESOURCES,
                                       'test_nonexistent_video.mp4')
        result_file = os.path.join(self.root_dir, 'test_nonexistent_video.mp4')
        task_def = self._create_task_def(resource_stream, result_file)

        with self.assertRaises(TranscodingTaskBuilderException) as e:
            self.execute_task(task_def)
        assert str(e.exception) == f'{resource_stream} does not exist'

    def test_invalid_resource_stream(self):
        resource_stream = os.path.join(self.RESOURCES, 'invalid_test_video.mp4')
        result_file = os.path.join(self.root_dir,
                                   'test_invalid_resource_stream.mp4')

        task_def = self._create_task_def(resource_stream, result_file)

        with self.assertRaises(ffmpegException) as e:
            self.execute_task(task_def)
        assert str(e.exception) == 'Subtask computation failed with exit code 1'

    def test_task_invalid_params(self):
        result_file = os.path.join(self.root_dir, 'test_invalid_params.mp4')
        task_def = self._create_task_def(self.resource_stream, result_file)
        task_def['options']['video']['codec'] = 'abcd'

        with self.assertRaises(TranscodingTaskBuilderException) as e:
            self.execute_task(task_def)
        assert str(e.exception) == 'abcd is not supported'
