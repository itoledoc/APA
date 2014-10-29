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


def distribute_time(time_e, twelve, seven, single):

    if single and twelve:
        time_u = time_e / (1 + 0.5 + 2 + 4)
        return time_u, 0.5 * time_u, 2 * time_u, 4 * time_u
    elif single and not twelve:
        time_u = time_e / (1 + 2 + 4.)
        return time_u, 0., 2 * time_u, 4 * time_u
    elif seven and twelve:
        time_u = time_e / (1 + 0.5 + 2.)
        return time_u, 0.5 * time_u, 2 * time_u, 0.
    elif seven and not twelve:
        time_u = time_e / (1 + 2.)
        return time_u, 0., 2 * time_u, 0.
    elif twelve:
        time_u = time_e / 1.5
        return time_u, 0.5 * time_u, 0., 0.
    elif not twelve:
        return time_e, 0., 0., 0.
    else:
        print("couldn't distribute time...")
        return None


def needs2(ar, las):

    if (0.57 > ar >= 0.41) and las >= 9.1:
        return True
    elif (0.75 > ar >= 0.57) and las >= 9.1:
        return True
    elif (1.11 > ar >= 0.75) and las >= 14.4:
        return True
    elif (1.40 > ar >= 1.11) and las >= 18.0:
        return True
    else:
        return False
