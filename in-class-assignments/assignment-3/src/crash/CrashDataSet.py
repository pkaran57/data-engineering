import logging
import os
import pandas as pd
from datetime import datetime

from crash.CrashRecordType import CrashRecordType
from definitions import DATA_DIR

COLLISION_TYPE = 'Collision Type'
CRASH_TYPE = 'Crash Type'
CRASH_ID = 'Crash ID'
AGE = 'Age'
SCHOOL_ZONE_INDICATOR = 'School Zone Indicator'

CRASH_YEAR = 'Crash Year'
CRASH_MONTH = 'Crash Month'
CRASH_DAY = 'Crash Day'

RECORD_TYPE = 'Record Type'

INJURY_COUNT_FIELDS = {
    'Total Suspected Serious Injury (A) Count',
    'Total Suspected Minor Injury (B) Count',
    'Total Possible Injury (C) Count',
    'Total Non-Fatal Injury Count',
    'Total Pedestrian Non-Fatal Injury Count',
    'Total Pedalcyclist Non-Fatal Injury Count',
    'Total Unknown Non-Motorist Injury Count'
}

FATALITY_COUNT_FIELDS = {
    'Total Fatality Count',
    'Total Pedestrian Fatality Count',
    'Total Pedalcyclist Fatality Count',
    'Total Unknown Non-Motorist Fatality Count'
}


class CrashDataSet:
    _logger = logging.getLogger('CrashDataSet')

    def __init__(self):
        self._df = self._load_data_as_df()
        self._group_by_crash = self._df.groupby(CRASH_ID)

    @staticmethod
    def _load_data_as_df():
        df_dtype = {CRASH_YEAR: pd.Int16Dtype(), CRASH_MONTH: pd.Int16Dtype(), CRASH_DAY: pd.Int16Dtype(), COLLISION_TYPE: pd.Int16Dtype()}

        for count_field in INJURY_COUNT_FIELDS.union(FATALITY_COUNT_FIELDS):
            df_dtype[count_field] = pd.Int16Dtype()

        return pd.read_csv(os.path.join(DATA_DIR, 'OR-Hwy-26-crashes-2019.csv'), dtype=df_dtype)

    def validate_crash_data(self):
        self._logger.info("Validating crash data ...")

        assert not self._df.empty, 'Crash Dataframe is empty'

        valid_record_types = list(map(int, CrashRecordType))
        for record_type in self._df[RECORD_TYPE].unique():
            assert record_type in valid_record_types, 'Unable to find {} record type in {}'.format(record_type, CrashRecordType.__class__.name)

        # validate crash dates
        self._validate_and_get_crash_dates()

        # every crash has at least 1 vehicle associated with it
        for crash_id, crash_group in self._group_by_crash:
            assert not crash_group[crash_group[RECORD_TYPE] == CrashRecordType.VEHICLE].empty, 'Found no vehicle for crash id {}'.format(crash_id)

        # crash participants age is not negative
        participants_with_age = self._df[self._df[RECORD_TYPE] == CrashRecordType.PARTICIPANT] \
            .dropna(subset=[AGE])
        assert all(participants_with_age[AGE] >= 0), 'Age of some participant(s) is negative'

        # there is at least 1 fatality or injury for a given crash
        crashes = self._df[self._df[RECORD_TYPE] == CrashRecordType.CRASH]
        assert all(crashes.apply(self._get_total_fatality_and_injury_count, axis=1) > 1), 'At least 1 crash has no fatality and/or injury'

        # every crash has a unique id
        assert len(list(self._group_by_crash.groups)) == len(set(self._group_by_crash.groups)), 'Some Crash ID(s) is/are not unique'

        # most crashes happen outside of school zones
        crashes_in_school_zone_counts = (crashes[SCHOOL_ZONE_INDICATOR] == 1).value_counts()
        assert crashes_in_school_zone_counts[True] < crashes_in_school_zone_counts[False], 'More crashes took place in school zones than outside, this was not expected'

        # every crash participant has a Crash ID of a known crash
        participants = self._df[self._df[RECORD_TYPE] == CrashRecordType.PARTICIPANT]
        assert set(participants[CRASH_ID]) == set(crashes[CRASH_ID]), "Expected all participants to have crash IDs of known crashes"

        # every vehicle has a crash id of known crash
        vehicles = self._df[self._df[RECORD_TYPE] == CrashRecordType.VEHICLE]
        assert set(vehicles[CRASH_ID]) == set(crashes[CRASH_ID]), "Expected all vehicles to have crash IDs of known crashes"

        # Most crashes involve at-least two vehicles
        crashes_with_at_least_2_vehicles_count = 0
        for crash_id, crash_group in self._group_by_crash:
            if crash_group[crash_group[RECORD_TYPE] == CrashRecordType.VEHICLE].shape[0] >= 2:
                crashes_with_at_least_2_vehicles_count += 1
        assert crashes_with_at_least_2_vehicles_count > (self._group_by_crash.ngroups / 2), "expected most crashes involve at-least two vehicles"

        # Most collisions happen at an angle
        crash_types_value_counts = crashes[COLLISION_TYPE].value_counts()
        # Collision Type code of 1 = collision occurred at an angle
        assert crash_types_value_counts[1] > (len(crashes) / 2), 'Expected most collisions happen at an angle'

        self._logger.info("Validation of crash data completed successfully!")

    @staticmethod
    def _get_total_fatality_and_injury_count(crash_record):
        count = 0
        for fatality_type_field in FATALITY_COUNT_FIELDS.union(INJURY_COUNT_FIELDS):
            count += crash_record[fatality_type_field]
        return count

    def _validate_and_get_crash_dates(self):
        """
        Validate that each crash record has a valid crash date. Crash dates should be in the past.

        :raises RuntimeError: when an error validating crash date(s) is encountered
        """
        try:
            crashes = self._df[self._df[RECORD_TYPE] == CrashRecordType.CRASH]

            # crash month, day and year for each crash level record is a valid date
            crash_dates = pd.to_datetime(crashes[[CRASH_DAY, CRASH_MONTH, CRASH_YEAR]]
                                         .astype(str)
                                         .apply(func=' '.join, axis=1)
                                         , format='%d %m %Y'
                                         , errors='raise')

            # every crash has a date associated with it
            assert crashes.shape[0] == len(crash_dates), 'Expected number of crash dates [{}] to be equal to number of crashes [{}]'.format(crashes.shape[0], len(crash_dates))

            # crash date is in the past, not future
            assert all(crash_dates < pd.to_datetime(datetime.now())), 'Expected all crash dates to be in the past, found 1 or more in the future.'

            return crash_dates
        except Exception as ex:
            raise RuntimeError("Encountered an error while validating crash dates", ex)

    def describe(self):
        self._logger.info('Found {} crashes'.format(self._group_by_crash.ngroups))
        self._logger.debug('Following are all crash IDs: {}'.format(list(self._group_by_crash.groups)))
