import enum
import json
import logging
import os
from pathlib import Path
from typing import List

from apps.transcoding import common
from apps.transcoding.common import ffmpegException
from apps.transcoding.ffmpeg.environment import ffmpegEnvironment
from golem.core.common import HandleError
from golem.docker.image import DockerImage
from golem.docker.job import DockerJob
from golem.docker.task_thread import DockerTaskThread, DockerBind
from golem.environments.environment import Environment
from golem.environments.environmentsmanager import EnvironmentsManager
from golem.resource.dirmanager import DirManager

FFMPEG_DOCKER_IMAGE = 'golemfactory/ffmpeg'
FFMPEG_DOCKER_TAG = '1.0'
FFMPEG_BASE_SCRIPT = '/golem/scripts/ffmpeg_task.py'
FFMPEG_RESULT_FILE = '/golem/scripts/ffmpeg_task.py'

logger = logging.getLogger(__name__)


class Commands(enum.Enum):
    SPLIT = ('split', 'split-results.json')
    TRANSCODE = ('transcode', '')
    MERGE = ('merge', '')
    COMPUTE_METRICS = ('compute-metrics', '')


class StreamOperator:
    @HandleError(ValueError, common.not_valid_json)
    def split_video(self, input_stream: str, parts: int,
                    dir_manager: DirManager, task_id: str):
        name = os.path.basename(input_stream)
        tmp_task_dir = dir_manager.get_task_temporary_dir(task_id)
        stream_container_path = os.path.join(tmp_task_dir, name)
        task_output_dir = dir_manager.get_task_output_dir(task_id)
        env = ffmpegEnvironment(binds=[
            DockerBind(Path(input_stream), stream_container_path, 'ro')])
        extra_data = {
            'script_filepath': FFMPEG_BASE_SCRIPT,
            'command': Commands.SPLIT.value[0],
            'path_to_stream': stream_container_path,
            'parts': parts
        }
        logger.debug('Running video splitting [params = {}]'.format(extra_data))

        result = self._do_job_in_container(
            self._get_dir_mapping(dir_manager, task_id),
            extra_data, env)
        split_result_file = os.path.join(task_output_dir,
                                         Commands.SPLIT.value[1])
        output_files = result.get('data', [])
        if split_result_file not in output_files:
            raise ffmpegException('Result file {} does not exist'.
                                  format(split_result_file))
        logger.debug('Split result file is = {} [parts = {}]'.
                     format(split_result_file, parts))
        with open(split_result_file) as f:
            params = json.load(f)  # FIXME: check status of splitting
            if params.get('status', 'Success') is not 'Success':
                raise ffmpegException('Splitting video failed')
            streams_list = list(map(lambda x: (x.get('video_segment'),
                                               x.get('playlist')),
                                    params.get('segments', [])))
            logger.info('Stream {} was successfully splitted to {}'
                        .format(input_stream, streams_list))
            return streams_list

    def _prepare_merge_job(self, task_dir, chunks):
        try:
            resources_dir = task_dir
            output_dir = os.path.join(resources_dir, 'merge', 'output')
            os.makedirs(output_dir)
            work_dir = os.path.join(resources_dir, 'merge', 'work')
            os.makedirs(work_dir)
        except OSError:
            raise ffmpegException("Failed to prepare video \
                merge directory structure")
        files = self._collect_files(resources_dir, chunks)
        return resources_dir, output_dir, work_dir, list(
            map(lambda chunk: chunk.replace(resources_dir,
                                            DockerJob.RESOURCES_DIR),
                files))

    @staticmethod
    def _collect_files(dir, files):
        # each chunk must be in the same directory
        results = list()
        for file in files:
            if not os.path.isfile(file):
                raise ffmpegException("Missing result file: {}".format(file))
            elif os.path.dirname(file) != dir:
                raise ffmpegException("Result file: {} should be in the \
                proper directory: {}".format(file, dir))

            results.append(file)

        return results

    def merge_video(self, filename, task_dir, chunks):
        resources_dir, output_dir, work_dir, chunks = \
            self._prepare_merge_job(task_dir, chunks)

        extra_data = {
            'script_filepath': FFMPEG_BASE_SCRIPT,
            'command': Commands.MERGE.value[0],
            'output_stream': os.path.join(DockerJob.OUTPUT_DIR, filename),
            'chunks': chunks
        }

        logger.info('Merging video')
        logger.debug('Merge params: {}'.format(extra_data))

        dir_mapping = DockerTaskThread.specify_dir_mapping(output=output_dir,
                                                           temporary=work_dir,
                                                           resources=task_dir,
                                                           logs=output_dir,
                                                           work=work_dir)

        self._do_job_in_container(dir_mapping, extra_data)

        logger.info("Video merged successfully!")
        return os.path.join(output_dir, filename)

    @staticmethod
    def _do_job_in_container(dir_mapping, extra_data: dict,
                             env: Environment = None,
                             timeout: int = 120):

        if env:
            EnvironmentsManager().add_environment(env)

        dtt = DockerTaskThread(docker_images=[DockerImage(
            # repository=FFMPEG_DOCKER_IMAGE, tag=FFMPEG_DOCKER_TAG)],
            repository=FFMPEG_DOCKER_IMAGE, tag="latest")],
            extra_data=extra_data,
            dir_mapping=dir_mapping,
            timeout=timeout)

        dtt.run()
        if dtt.error:
            raise ffmpegException(dtt.error_msg)
        return dtt.result[0] if isinstance(dtt.result, tuple) else dtt.result

    @staticmethod
    def _get_dir_mapping(dir_manager: DirManager, task_id: str):
        tmp_task_dir = dir_manager.get_task_temporary_dir(task_id)
        resources_task_dir = dir_manager.get_task_resource_dir(task_id)
        task_output_dir = dir_manager.get_task_output_dir(task_id)

        return DockerTaskThread. \
            specify_dir_mapping(output=task_output_dir,
                                temporary=tmp_task_dir,
                                resources=resources_task_dir,
                                logs=tmp_task_dir,
                                work=tmp_task_dir)

    @staticmethod
    def _specify_dir_mapping(output, temporary, resources, logs, work):
        return DockerTaskThread.specify_dir_mapping(output=output,
                                                    temporary=temporary,
                                                    resources=resources,
                                                    logs=logs, work=work)

    def get_metadata(self, basename_file_list: List[str], task_dir):

        def _prepare_dir_mapping():
            try:
                _resources_dir = task_dir
                _output_dir = os.path.join(task_dir, 'metadata_output')
                # _work_dir = os.path.join(task_dir, 'merge')
                _work_dir = os.path.join(task_dir, 'metadata_work')
                # _work_dir = task_dir
                os.makedirs(_output_dir)
                os.makedirs(_work_dir)
            except OSError:
                pass
                # raise ffmpegException("Failed to prepare video \
                #            directory structure")
            return _resources_dir, _work_dir, _output_dir

        res_dir, work_dir, output_dir = _prepare_dir_mapping()

        metadata_requests = []

        for name in basename_file_list:
            metadata_requests.append(
                {'video': name,
                 'output': 'metadata-logs-'+os.path.splitext(name)[0]+'.json'}
                 # 'output': 'metadata-logs-'+os.path.splitext(name)[0]+'.txt'}
            )

        extra_data = {
            'script_filepath': FFMPEG_BASE_SCRIPT,
            'command': Commands.COMPUTE_METRICS.value[0],
            'metrics_params': {'metadata': metadata_requests},
        }

        dir_mapping = DockerTaskThread.specify_dir_mapping(output=output_dir,
                                                           temporary=work_dir,
                                                           resources=res_dir,
                                                           logs=work_dir,
                                                           work=work_dir)
        result = self._do_job_in_container(dir_mapping, extra_data)
        return result

