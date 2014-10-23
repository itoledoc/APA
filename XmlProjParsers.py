__author__ = 'itoledo'

from lxml import objectify

from converter import *


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


def extract_sciencegoal_info(sg, ordinal):

    sg_id = 'sg%003d' % (ordinal + 1)
    sg_name = sg.name.pyval
    valid = True

    try:
        ous_id = sg.ObsUnitSetRef.attrib['partId']
        has_sb = True
        phase_ii = True

    except AttributeError:
        ous_id = None
        has_sb = False
        phase_ii = False
        valid = False

    band_tags = sg.findall(prj + 'requiredReceiverBands')
    bands = band_tags[0].pyval

    if len(band_tags) > 1:
        for b in band_tags[1:]:
            bands += ',' + b.pyval

    estimated_time = convert_tsec(
        sg.estimatedTotalTime.pyval, sg.estimatedTotalTime.attrib['unit'])

    performance = sg.PerformanceParameters
    rep_freq = convert_ghz(
        performance.representativeFrequency.pyval,
        performance.representativeFrequency.attrib['unit'])
    ar = convert_sec(
        performance.desiredAngularResolution.pyval,
        performance.desiredAngularResolution.attrib['unit'])
    las = convert_sec(
        performance.desiredLargestScale.pyval,
        performance.desiredLargestScale.attrib['unit'])
    sensitivity = convert_jy(
        performance.desiredSensitivity.pyval,
        performance.desiredSensitivity.attrib['unit'])
    use_aca = performance.useACA.pyval
    use_tp = performance.useTP.pyval

    is_point_source = performance.isPointSource.pyval

    try:
        is_time_constrained = performance.isTimeConstrained.pyval
    except AttributeError:
        is_time_constrained = None

    spectral = sg.SpectralSetupParameters
    polarization = spectral.attrib['polarisation']
    type_pol = spectral.attrib['type']

    ar_cor = ar * rep_freq / 100.
    las_cor = las * rep_freq / 100.

    two_12m = needs2(ar_cor, las_cor)

    extended_time, compact_time, seven_time, tp_time = distribute_time(
        estimated_time, two_12m, use_aca, use_tp
    )

    return [sg_id, sg_name, valid, has_sb, phase_ii,
            ous_id, bands, estimated_time, extended_time, compact_time,
            seven_time, tp_time, rep_freq, ar, ar_cor, las, las_cor,
            sensitivity, use_aca, use_tp, is_point_source,
            is_time_constrained, polarization, type_pol, two_12m]


def extract_targets(obsproject_uid, sg, ordinal):

    sg_id = 'sg%003d' % (ordinal + 1)
    targets = []
    list_targets = sg.findall(prj + 'TargetParameters')
    c = 1
    for target in list_targets:
        target_id = 'tg%003d' % c

        try:
            solar_system = target.attrib['solarSystemObject']
        except KeyError:
            solar_system = None
        try:
            typetar = target.attrib['type']
        except KeyError:
            typetar = None

        source_name = target.sourceName.pyval
        coord = target.sourceCoordinates
        ra = convert_deg(coord.findall(val + 'longitude')[0].pyval,
                         coord.findall(val + 'longitude')[0].attrib['unit'])
        dec = convert_deg(coord.findall(val + 'latitude')[0].pyval,
                          coord.findall(val + 'latitude')[0].attrib['unit'])
        try:
            is_mosaic = target.isMosaic.pyval
        except AttributeError:
            is_mosaic = None

        targets.append(
            [obsproject_uid, sg_id, target_id, typetar, solar_system,
             source_name, ra, dec, is_mosaic]
        )
        c += 1

    return targets


def extract_spw(obsproject_uid, sg, ordinal):

    pass