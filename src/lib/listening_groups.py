import csv

from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata
import time

from src.lib import PipelineConfiguration

log = Logger(__name__)


class ListeningGroups(object):
    @classmethod
    def tag_listening_groups_participants(cls, user, data, pipeline_configuration, raw_data_dir):
        """
        This tags uids who participated in mothers or health practitioners listening groups sessions.
        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to tag listening group participation to.
        :type data: iterable of TracedData
        :param raw_data_dir: Directory containing de-identified listening groups contacts CSVs containing
                                    listening groups data stored as `Name` and `avf-phone-uuid` columns.
        :type user: str
        :param pipeline_configuration: Pipeline configuration.
        :type pipeline_configuration: PipelineConfiguration
        """

        listening_group_participants = {
            "health_practitioners": set(),
            "mothers":set()
        }

        # Read listening group participants CSVs and add their uids to the respective group
        for k in listening_group_participants.keys():
            for listening_group_csv_url in pipeline_configuration.listening_group_csv_urls[k]:
                listening_group_csv = listening_group_csv_url.split("/")[-1]
                with open(f'{raw_data_dir}/{listening_group_csv}', "r", encoding='utf-8-sig') as f:
                    listening_group_data = list(csv.DictReader(f))
                    for row in listening_group_data:
                        listening_group_participants[k].add(row['avf-phone-uuid'])

            log.info(f'Loaded {len(listening_group_participants[k])} {k} listening group participants')

        # Tag a participant based on the listening group type they belong to
        for td in data:
            listening_group_participation = dict() # of uid health_practitioner or mother lg participation data
            listening_group_participation['health_practitioners_listening_group_participant'] = \
                td['uid'] in listening_group_participants['health_practitioners']
            listening_group_participation['mothers_listening_group_participant'] = \
                td['uid'] in listening_group_participants['mothers']

            td.append_data(listening_group_participation, Metadata(user, Metadata.get_call_location(), time.time()))
