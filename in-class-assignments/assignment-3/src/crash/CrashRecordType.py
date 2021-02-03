from enum import IntEnum


class CrashRecordType(IntEnum):

    # records containing information that is common to a given crash, such as the hour the
    # crash occurred, its location, collision type, crash classification, weather conditions, investigation, etc
    CRASH = 1

    # records containing information specific to each vehicle involved in the crash, such as
    # vehicle type, direction of travel, action, errors, causes, events, etc.
    VEHICLE = 2

    # records containing information specific to persons involved in the crash, such
    # as participant type, sex, age, injury severity, etc
    PARTICIPANT = 3
