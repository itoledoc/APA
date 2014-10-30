__author__ = 'itoledo'

from lxml import objectify


prj = '{Alma/ObsPrep/ObsProject}'
val = '{Alma/ValueTypes}'
sbl = '{Alma/ObsPrep/SchedBlock}'


class ObsProposal(object):

    def __init__(self, xml_file, path='./'):
        """

        :param xml_file:
        :param path:
        """
        io_file = open(path + xml_file)
        tree = objectify.parse(io_file)
        io_file.close()
        self.data = tree.getroot()


class ObsProject(object):

    def __init__(self, xml_file, path='./'):
        """
        Notes
        -----

        ObsProject level:
        - isDDT may or may not be present, but we assume is unique.

        ScienceGoal level:
        - We only look for ScienceGoals. If a ScienceGoal has an ObsUnitSetRef
          then it might has SBs, but we assume there is only one OUSRef.
        - For the estimatedTotalTime, we assume it is the sum of the OT
          calculations, incluing ExtT, CompT, ACAT, TPT
        - Cycle 1 and 2 have only one band as requirement, supposely, but we
          check for more just in case.

        PerformanceParameters level:
        - Only one representativeFrequency assumed

        :param xml_file:
        :param path:
        """
        io_file = open(path + xml_file)
        tree = objectify.parse(io_file)
        io_file.close()
        data = tree.getroot()
        self.status = data.attrib['status']
        for key in data.__dict__:
            self.__setattr__(key, data.__dict__[key])


class SchedBlock(object):

    def __init__(self, xml_file, path='./'):
        """

        :param xml_file:
        :param path:
        """
        io_file = open(path + xml_file)
        tree = objectify.parse(io_file)
        io_file.close()
        self.data = tree.getroot()
