import enum
import json
import logging
import os
from pathlib import Path
from typing import Optional

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

# Suffix used to distinguish the temporary container that has no audio or data
# streams from a complete video
VIDEO_ONLY_CONTAINER_SUFFIX = '[video-only]'

logger = logging.getLogger(__name__)


class Commands(enum.Enum):
    EXTRACT_AND_SPLIT = ('extract_and_split', 'split-results.json')
    TRANSCODE = ('transcode', '')
    MERGE_AND_REPLACE = ('merge_and_replace', '')


def adjust_path(path: str, # pylint: disable=too-many-arguments
                dirname: Optional[str] = None,
                stem: Optional[str] = None,
                extension: Optional[str] = None,
                stem_prefix: str = '',
                stem_suffix: str = ''):
    """
    Splits specified path into components and reassembles it back,
    replacing some of those components with user-provided values and adding
    perfixes and suffixes.

    Path components
    ---------------

    # /golem/split/resources/video[num=10].reencoded.mp4
    # =======================^^^^^^^^^^^^^^^^^^^^^^^####
    #         dirname                  stem         extension
    """

    assert extension is None or extension == '' or extension.startswith('.'), \
        "Just like in splitext(), the dot must be included in the extension"

    (original_dirname, original_basename) = os.path.split(path)
    (original_stem, original_extension) = os.path.splitext(original_basename)

    new_dirname = original_dirname if dirname is None else dirname
    new_stem = original_stem if stem is None else stem
    new_extension = original_extension if extension is None else extension

    return os.path.join(
        f"{new_dirname}",
        f"{stem_prefix}{new_stem}{stem_suffix}{new_extension}")


class StreamOperator:
    @HandleError(ValueError, common.not_valid_json)
    def extract_video_streams_and_split(self,
                                        input_file_on_host: str,
                                        parts: int,
                                        dir_manager: DirManager,
                                        task_id: str):

        host_dirs = {
            'tmp': dir_manager.get_task_temporary_dir(task_id),
            'output': dir_manager.get_task_output_dir(task_id),
        }

        input_file_basename = os.path.basename(input_file_on_host)
        input_file_in_container = os.path.join(
            # FIXME: This is a path on the host but docker will create it in
            # the container. It's unlikely that there's anything there but
            # it's not guaranteed.
            host_dirs['tmp'],
            input_file_basename)

        # FIXME: The environment is stored globally. Changing it will affect
        # containers started by other functions that do not do it themselves.
        env = ffmpegEnvironment(binds=[DockerBind(
            Path(input_file_on_host),
            input_file_in_container,
            'ro')])

        extra_data = {
            'script_filepath': FFMPEG_BASE_SCRIPT,
            'command': Commands.EXTRACT_AND_SPLIT.value[0],
            'input_file': input_file_in_container,
            'parts': parts,
        }

        logger.debug(
            f'Running video stream extraction and splitting '
            f'[params = {extra_data}]')
        result = self._do_job_in_container(
            self._get_dir_mapping(dir_manager, task_id),
            extra_data,
            env)

        split_result_file = os.path.join(host_dirs['output'],
                                         Commands.EXTRACT_AND_SPLIT.value[1])
        output_files = result.get('data', [])
        if split_result_file not in output_files:
            raise ffmpegException('Result file {} does not exist'.
                                  format(split_result_file))
        logger.debug('Split result file is = {} [parts = {}]'.
                     format(split_result_file, parts))
        with open(split_result_file) as f:
            params = json.load(f)  # FIXME: check status of splitting
            if params.get('status', 'Success') != 'Success':
                raise ffmpegException('Splitting video failed')
            streams_list = list(map(
                lambda x: x.get('video_segment'),
                params.get('segments', [])))
            logger.info(
                f"Stream {input_file_on_host} has successfully passed the "
                f"extract+split operation. Segments: {streams_list}")
            return streams_list

    def _prepare_merge_job(self, task_dir, chunks_on_host):
        host_dirs = {
            'resources': task_dir,
            'temporary': os.path.join(task_dir, 'merge', 'work'),
            'work': os.path.join(task_dir, 'merge', 'work'),
            'output': os.path.join(task_dir, 'merge', 'output'),
            'logs': os.path.join(task_dir, 'merge', 'output'),
        }

        try:
            os.makedirs(host_dirs['output'])
            os.makedirs(host_dirs['work'])
        except OSError:
            raise ffmpegException(
                "Failed to prepare video merge directory structure")
        files = self._collect_files(host_dirs['resources'], chunks_on_host)
        chunks_in_container = list(map(
            lambda chunk: chunk.replace(
                host_dirs['resources'],
                DockerJob.RESOURCES_DIR),
            files))

        return (host_dirs, chunks_in_container)

    @staticmethod
    def _collect_files(dir, files):
        # each chunk must be in the same directory
        results = list()
        for file in files:
            if not os.path.isfile(file):
                raise ffmpegException("Missing result file: {}".format(file))
            if os.path.dirname(file) != dir:
                raise ffmpegException("Result file: {} should be in the \
                proper directory: {}".format(file, dir))

            results.append(file)

        return results

    def merge_and_replace_video_streams(self,
                                        input_file_on_host,
                                        chunks_on_host,
                                        output_file_basename,
                                        task_dir):

        assert os.path.isdir(task_dir), \
            "Caller is responsible for ensuring that task dir exists."
        assert os.path.isfile(input_file_on_host), \
            "Caller is responsible for ensuring that input file exists."

        (host_dirs, chunks_in_container) = self._prepare_merge_job(
            task_dir,
            chunks_on_host)

        container_files = {
            # FIXME: /golem/tmp should not be hard-coded.
            'in': os.path.join(
                '/golem/tmp',
                os.path.basename(input_file_on_host)),
            'out': os.path.join(DockerJob.OUTPUT_DIR, output_file_basename),
        }
        extra_data = {
            'script_filepath': FFMPEG_BASE_SCRIPT,
            'command': Commands.MERGE_AND_REPLACE.value[0],
            'input_file': container_files['in'],
            'chunks': chunks_in_container,
            'output_file': container_files['out'],
        }

        logger.info(
            'Merging video and '
            'replacing original video streams with merged ones')
        logger.debug(f'Merge and replace params: {extra_data}')

        # FIXME: The environment is stored globally. Changing it will affect
        # containers started by other functions that do not do it themselves.
        env = ffmpegEnvironment(binds=[DockerBind(
            Path(input_file_on_host),
            container_files['in'],
            'ro')])

        self._do_job_in_container(
            DockerTaskThread.specify_dir_mapping(**host_dirs),
            extra_data,
            env)

        logger.info("Video merged and streams replaced successfully!")

        return os.path.join(host_dirs['output'], output_file_basename)

    @staticmethod
    def _do_job_in_container(dir_mapping, extra_data: dict,
                             env: Environment = None,
                             timeout: int = 120):

        if env:
            EnvironmentsManager().add_environment(env)

        dtt = DockerTaskThread(
            docker_images=[
                DockerImage(
                    repository=FFMPEG_DOCKER_IMAGE,
                    tag=FFMPEG_DOCKER_TAG
                )
            ],
            extra_data=extra_data,
            dir_mapping=dir_mapping,
            timeout=timeout
        )

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
