import logging

from crash.CrashDataSet import CrashDataSet

logging.basicConfig(format="'%(asctime)s' %(name)s : %(message)s'", level=logging.DEBUG)
logger = logging.getLogger('main')

if __name__ == '__main__':
    crash_data_set = CrashDataSet()
    crash_data_set.describe()
    crash_data_set.validate_crash_data()
