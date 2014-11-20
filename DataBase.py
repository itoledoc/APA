__author__ = 'itoledo'
__metaclass__ = type

import os
import pandas as pd
import ephem
import cx_Oracle
import arrayResolution2p as ARes

from subprocess import call
from XmlProjParsers import *
from converter import *

prj = '{Alma/ObsPrep/ObsProject}'
val = '{Alma/ValueTypes}'
sbl = '{Alma/ObsPrep/SchedBlock}'

pd.options.display.width = 200
pd.options.display.max_columns = 55

confDf = pd.DataFrame(
    [('C34-1', 3.73, 2.49, 1.62, 1.08, 0.81, 0.57)],
    columns=['Conf', 'ALMA_RB_03', 'ALMA_RB_04', 'ALMA_RB_06', 'ALMA_RB_07',
             'ALMA_RB_08', 'ALMA_RB_09'],
    index=['C34-1'])
confDf.ix['C34-2'] = ('C34-2', 2.04, 1.36, 0.89, 0.59, 0.44, 0.31)
confDf.ix['C34-3'] = ('C34-3', 1.4, 0.93, 0.61, 0.4, 0.3, 0.21)
confDf.ix['C34-4'] = ('C34-4', 1.11, 0.74, 0.48, 0.32, 0.24, 0.17)
confDf.ix['C34-5'] = ('C34-5', 0.75, 0.50, 0.33, 0.22, 0.16, 0.12)
confDf.ix['C34-6'] = ('C34-6', 0.57, 0.38, 0.25, 0.16, 0.12, 0.09)
confDf.ix['C34-7'] = ('C34-7', 0.41, 0.27, 0.18, 0.12, None, None)


# noinspection PyPep8Naming,PyAttributeOutsideInit,PyUnresolvedReferences
class Database(object):

    """
    Database is the class that stores the Projects and SB information in
    dataframes, and it also has the methods to connect and query the OSF
    archive for this info.

    A default instance will use the directory $HOME/.wto as a cache, and by
    default find the approved Cycle 2 projects and carried-over Cycle 1
    projects. If a file name or list are given as 'source' parameter, only the
    information of the projects in that list or filename will be ingested.

    Setting *forcenew* to True will force the cleaning of the cache dir, and
    all information will be processed again.

    :param path: Path for data cache.
    :type path: str, default '$HOME/.wto'
    :param forcenew: Force cache cleaning and reload from archive.
    :type forcenew: boolean, default False
    """

    def __init__(self, path='/.apa/', forcenew=False):
        """


        """
        self.new = forcenew
        # Default Paths and Preferences
        if path[-1] != '/':
            path += '/'
        self.path = os.environ['HOME'] + path
        self.apa_path = os.environ['APA']
        self.phase1_data = os.environ['PHASEONE']
        self.sbxml = self.path + 'sbxml/'
        self.obsxml = self.path + 'obsxml/'
        self.propxml = self.path + 'propxml/'
        self.preferences = pd.Series(
            ['project.pandas', 'sciencegoals.pandas',
             'scheduling.pandas', 'special.list', 'pwvdata.pandas',
             'executive.pandas', 'sbxml_table.pandas', 'sbinfo.pandas',
             'newar.pandas', 'fieldsource.pandas', 'target.pandas',
             'spectralconf.pandas'],
            index=['project_table', 'sciencegoals_table',
                   'scheduling_table', 'special', 'pwv_data',
                   'executive_table', 'sbxml_table', 'sbinfo_table',
                   'newar_table', 'fieldsource_table', 'target_table',
                   'spectralconf_table'])
        self.status = ["Canceled", "Rejected"]

        self.grades = pd.read_table(self.apa_path + 'conf/c2grade.csv', sep=',')
        self.sb_sg_p1 = pd.read_pickle(self.apa_path + 'conf/sb_sg_p1.pandas')

        # Global SQL search expressions
        # Search Project's PT information and match with PT Status
        self.sql1 = str(
            "SELECT PRJ_ARCHIVE_UID as OBSPROJECT_UID,PI,PRJ_NAME,"
            "CODE,PRJ_SCIENTIFIC_RANK,PRJ_VERSION,"
            "PRJ_LETTER_GRADE,DOMAIN_ENTITY_STATE as PRJ_STATUS,"
            "ARCHIVE_UID as OBSPROPOSAL_UID "
            "FROM ALMA.BMMV_OBSPROJECT obs1, ALMA.OBS_PROJECT_STATUS obs2,"
            " ALMA.BMMV_OBSPROPOSAL obs3 "
            "WHERE regexp_like (CODE, '^201[23].*\.[AST]') "
            "AND (PRJ_LETTER_GRADE='A' OR PRJ_LETTER_GRADE='B' "
            "OR PRJ_LETTER_GRADE='C') AND PRJ_SCIENTIFIC_RANK < 9999 "
            "AND obs2.OBS_PROJECT_ID = obs1.PRJ_ARCHIVE_UID AND "
            "obs1.PRJ_ARCHIVE_UID = obs3.PROJECTUID")

        # Initialize with saved data and update, Default behavior.
        if not self.new:
            try:
                self.projects = pd.read_pickle(
                    self.path + 'projects.pandas')
                self.sb_sg_p2 = pd.read_pickle(
                    self.path + 'sb_sg_p2.pandas')
                self.sciencegoals = pd.read_pickle(
                    self.path + 'sciencegoals.pandas')
                self.aqua_execblock = pd.read_pickle(
                    self.path + 'aqua_execblock.pandas')
                self.executive = pd.read_pickle(
                    self.path + 'executive.pandas')
                self.obsprojects = pd.read_pickle(
                    self.path + 'obsprojects.pandas')
                self.obsproposals = pd.read_pickle(
                    self.path + 'obsproposals.pandas')
                self.saos_obsproject = pd.read_pickle(
                    self.path + 'saos_obsproject.pands')
                self.saos_schedblock = pd.read_pickle(
                    self.path + 'saos_schedblock.pandas')
                self.sg_targets = pd.read_pickle(
                    self.path + 'sg_targets')
            except IOError, e:
                print e
                self.new = True

        # Create main dataframes
        if self.new:
            call(['rm', '-rf', self.path])
            print(self.path + ": creating preferences dir")
            os.mkdir(self.path)
            os.mkdir(self.sbxml)
            os.mkdir(self.obsxml)
            os.mkdir(self.propxml)
            # Global Oracle Connection
            conx_string = os.environ['CON_STR']
            self.connection = cx_Oracle.connect(conx_string)
            self.cursor = self.connection.cursor()

            # Populate different dataframes related to projects and SBs statuses
            # self.scheduling_proj: data frame with projects at SCHEDULING_AOS
            # Query Projects currently on SCHEDULING_AOS
            self.sqlsched_proj = str(
                "SELECT CODE,OBSPROJECT_UID as OBSPROJECT_UID,"
                "VERSION as PRJ_SAOS_VERSION, STATUS as PRJ_SAOS_STATUS "
                "FROM SCHEDULING_AOS.OBSPROJECT "
                "WHERE regexp_like (CODE, '^201[23].*\.[AST]')")
            self.cursor.execute(self.sqlsched_proj)
            self.saos_obsproject = pd.DataFrame(
                self.cursor.fetchall(),
                columns=[rec[0] for rec in self.cursor.description]
            ).set_index('CODE', drop=False)

            # self.scheduling_sb: SBs at SCHEDULING_AOS
            # Query SBs in the SCHEDULING_AOS tables
            self.sqlsched_sb = str(
                "SELECT ou.OBSUNIT_UID as OUS_ID, sb.NAME as SB_NAME,"
                "sb.SCHEDBLOCK_CTRL_EXEC_COUNT,"
                "sb.SCHEDBLOCK_CTRL_STATE as SB_SAOS_STATUS,"
                "ou.OBSUNIT_PROJECT_UID as OBSPROJECT_UID "
                "FROM SCHEDULING_AOS.SCHEDBLOCK sb, SCHEDULING_AOS.OBSUNIT ou "
                "WHERE sb.SCHEDBLOCKID = ou.OBSUNITID AND sb.CSV = 0")
            self.cursor.execute(self.sqlsched_sb)
            self.saos_schedblock = pd.DataFrame(
                self.cursor.fetchall(),
                columns=[rec[0] for rec in self.cursor.description]
            ).set_index('OUS_ID', drop=False)

            # self.sbstates: SBs status (PT?)
            # Query SBs status
            self.sqlstates = str(
                "SELECT DOMAIN_ENTITY_STATE as SB_STATE,"
                "DOMAIN_ENTITY_ID as SB_UID,OBS_PROJECT_ID as OBSPROJECT_UID "
                "FROM ALMA.SCHED_BLOCK_STATUS")
            self.cursor.execute(self.sqlstates)
            self.sb_status = pd.DataFrame(
                self.cursor.fetchall(),
                columns=[rec[0] for rec in self.cursor.description]
            ).set_index('SB_UID', drop=False)

            # self.qa0: QAO flags for observed SBs
            # Query QA0 flags from AQUA tables
            self.sqlqa0 = str(
                "SELECT SCHEDBLOCKUID as SB_UID,QA0STATUS "
                "FROM ALMA.AQUA_EXECBLOCK "
                "WHERE regexp_like (OBSPROJECTCODE, '^201[23].*\.[AST]')")

            self.cursor.execute(self.sqlqa0)
            self.aqua_execblock = pd.DataFrame(
                self.cursor.fetchall(),
                columns=[rec[0] for rec in self.cursor.description]
            ).set_index('SB_UID', drop=False)

            # Query for Executives
            sql2 = str(
                "SELECT PROJECTUID as OBSPROJECT_UID, ASSOCIATEDEXEC "
                "FROM ALMA.BMMV_OBSPROPOSAL "
                "WHERE regexp_like (CYCLE, '^201[23].[1A]')")
            self.cursor.execute(sql2)
            self.executive = pd.DataFrame(
                self.cursor.fetchall(), columns=['OBSPROJECT_UID', 'EXEC'])

            self.start_wto()

    def start_wto(self):

        """
        Initializes the wtoDatabase dataframes.

        The function queries the archive to look for cycle 1 and cycle 2
        projects, disregarding any projects with status "Approved",
        "Phase1Submitted", "Broken", "Canceled" or "Rejected".

        The archive tables used are ALMA.BMMV_OBSPROPOSAL,
        ALMA.OBS_PROJECT_STATUS, ALMA.BMMV_OBSPROJECT and
        ALMA.XML_OBSPROJECT_ENTITIES.

        :return: None
        """
        # noinspection PyUnusedLocal
        status = self.status

        # Query for Projects, from BMMV.
        self.cursor.execute(self.sql1)
        df1 = pd.DataFrame(
            self.cursor.fetchall(),
            columns=[rec[0] for rec in self.cursor.description])
        print(len(df1.query('PRJ_STATUS not in @status')))
        self.projects = pd.merge(
            df1.query('PRJ_STATUS not in @status'), self.executive,
            on='OBSPROJECT_UID'
        ).set_index('CODE', drop=False)

        timestamp = pd.Series(
            np.zeros(len(self.projects), dtype=object),
            index=self.projects.index)
        self.projects['timestamp'] = timestamp
        self.projects['xmlfile'] = pd.Series(
            np.zeros(len(self.projects), dtype=object),
            index=self.projects.index)
        self.filter_c1()

        # Download and read obsprojects and obsprosal
        number = self.projects.__len__()
        c = 1
        for r in self.projects.iterrows():
            xmlfilename, obsproj = self.get_projectxml(
                r[1].CODE, r[1].PRJ_STATUS, number, c)
            c += 1
            if obsproj:
                self.read_obsproject(xmlfilename)
            else:
                self.read_obsproposal(xmlfilename, r[1].CODE)

        self.projects['isCycle2'] = self.projects.apply(
            lambda r1: True if r1['CODE'].startswith('2013') else False,
            axis=1)
        self.projects.to_pickle(
            self.path + 'projects.pandas')
        self.sb_sg_p2.to_pickle(
            self.path + 'sb_sg_p2.pandas')
        self.sciencegoals.to_pickle(
            self.path + 'sciencegoals.pandas')
        self.aqua_execblock.to_pickle(
            self.path + 'aqua_execblock.pandas')
        self.executive.to_pickle(
            self.path + 'executive.pandas')
        self.obsprojects.to_pickle(
            self.path + 'obsprojects.pandas')
        self.obsproposals.to_pickle(
            self.path + 'obsproposals.pandas')
        self.saos_obsproject.to_pickle(
            self.path + 'saos_obsproject.pands')
        self.saos_schedblock.to_pickle(
            self.path + 'saos_schedblock.pandas')
        self.sg_targets.to_pickle(
            self.path + 'sg_targets')

    def get_phaseone_sb(self):
        sbp1 = os.listdir(self.phase1_data + 'SchedBlock/')
        for x in sbp1:
            xml = SchedBlock(x, self.phase1_data + 'SchedBlock/')
            obs_uid = xml.data.findall(
                './/' + prj + 'ObsProjectRef')[0].attrib['entityId']
            if obs_uid not in self.obsproposals.OBSPROJECT_UID.values:
                continue
            print('Procesing Phase I SBs of %s' % obs_uid)
            sb_uid = xml.data.SchedBlockEntity.attrib['entityId']
            self.read_schedblocks_p1(sb_uid, obs_uid, xml)

    # noinspection PyAttributeOutsideInit
    def process_sbs(self, forcenew=False):
        try:
            if not forcenew:
                self.schedblocks_p2 = pd.read_pickle(
                    self.path + 'schedblocks_p2.pandas')
                self.schedblocks_p1 = pd.read_pickle(
                    self.path + 'schedblocks_p1.pandas')
                self.fieldsource = pd.read_pickle(
                    self.path + 'fieldsource.pandas')
                self.target = pd.read_pickle(
                    self.path + 'target.pandas')
                self.spectralconf = pd.read_pickle(
                    self.path + 'spectralconf.pandas')
            else:
                # noinspection PyUnusedLocal
                damnsolution = pd.read_pickle(
                    self.path + 'thisfileonlytoforcerenewalIOError')

        except IOError:
            new = True
            for sg_sb in self.sb_sg_p2.iterrows():
                self.read_schedblocks_p2(
                    sg_sb[1].SB_UID, sg_sb[1].OBSPROJECT_UID, sg_sb[1].OUS_ID,
                    new=new)
                new = False
            self.schedblocks_p2.to_pickle(self.path + 'schedblocks_p2.pandas')
            self.get_phaseone_sb()
            self.schedblocks_p1.loc[:, 'SG_ID'] = self.schedblocks_p1.apply(
                lambda r: self.sb_sg_p1[
                    self.sb_sg_p1.SB_UID == r['SB_UID']].SG_ID_y.values[0],
                axis=1)
            self.schedblocks_p1.to_pickle(self.path + 'schedblocks_p1.pandas')
            self.fieldsource.to_pickle(self.path + 'fieldsource.pandas')
            self.target.to_pickle(self.path + 'target.pandas')
            self.spectralconf.to_pickle(self.path + 'spectralconf.pandas')

        # noinspection PyUnusedLocal
        not2t = self.schedblocks_p1[
            self.schedblocks_p1.duplicated(
                ['SG_ID', 'sbName', 'repfreq', 'array', 'minAR_ot'])
        ].SB_UID.values
        sg_p1_2TWELVE = self.schedblocks_p1[
            self.schedblocks_p1.duplicated(
                ['SG_ID', 'sbName', 'repfreq', 'array'])
        ].query('SB_UID not in @not2t').SG_ID.values

        for i in sg_p1_2TWELVE:
            self.sciencegoals.loc[i, 'two_12m'] = True
        sg_p2_2TWELVE = self.schedblocks_p2[
            self.schedblocks_p2.sbName.str.endswith('_TC')].SG_ID.values

        for i in sg_p2_2TWELVE:
            self.sciencegoals.loc[i, 'two_12m'] = True

        columns = ['eExt12Time', 'eComp12Time', 'eACATime', 'eTPTime']
        self.sciencegoals.loc[:, columns] = self.sciencegoals.apply(
            lambda r: distribute_time(
                r['estimatedTime'], r['two_12m'], r['useACA'], r['useTP']),
            axis=1)

        sb_comp_p1 = self.schedblocks_p1.query(
            'SG_ID in @sg_p1_2TWELVE'
        ).groupby('SG_ID').minAR_ot.idxmax().values

        for i in sb_comp_p1:
            self.schedblocks_p1.loc[i, 'array12mType'] = 'Comp'

        self.newAR_p2_input = pd.merge(
            self.schedblocks_p2.query('array == "TWELVE-M"'),
            self.sciencegoals, on='SG_ID')[
                ['SB_UID', 'sbName', 'AR', 'LAS', 'repfreq', 'repFreq',
                 'useACA', 'two_12m', 'array12mType', 'minAR_ot',
                 'maxAR_ot']].set_index('SB_UID', drop=False)

        self.newAR_p1_input = pd.merge(
            self.schedblocks_p1.query('array == "TWELVE-M"'),
            self.sciencegoals, on='SG_ID')[
                ['SB_UID', 'sbName', 'AR', 'LAS', 'repfreq', 'repFreq',
                 'useACA', 'two_12m', 'array12mType', 'minAR_ot',
                 'maxAR_ot']].set_index('SB_UID', drop=False)

        ars2 = self.newAR_p2_input.apply(
            lambda r: new_array_ar(
                self.apa_path, r['AR'], r['LAS'], r['repfreq'], r['useACA'],
                r['two_12m'], r['array12mType']),
            axis=1)

        ars1 = self.newAR_p1_input.apply(
            lambda r: new_array_ar(
                self.apa_path, r['AR'], r['LAS'], r['repfreq'], r['useACA'],
                r['two_12m'], r['array12mType']),
            axis=1)

        self.newAR_p2_input = pd.merge(
            self.newAR_p2_input, ars2, left_index=True, right_index=True)
        self.newAR_p1_input = pd.merge(
            self.newAR_p1_input, ars1, left_index=True, right_index=True)

    def get_projectxml(self, code, state, n, c):
        """

        :param code:
        """

        if state not in ['Approved', 'PhaseISubmitted']:
            print("Downloading Project %s obsproject.xml, status %s. (%s/%s)" %
                  (code, self.projects.ix[code, 'PRJ_STATUS'], c, n))
            self.cursor.execute(
                "SELECT TIMESTAMP, XMLTYPE.getClobVal(xml) "
                "FROM ALMA.XML_OBSPROJECT_ENTITIES "
                "WHERE ARCHIVE_UID = '%s'" %
                self.projects.ix[code, 'OBSPROJECT_UID'])
            obsproj = True
            try:
                data = self.cursor.fetchall()[0]
                xml_content = data[1].read()
                xmlfilename = code + '.xml'
                self.projects.loc[code, 'timestamp'] = data[0]
                filename = self.obsxml + xmlfilename
                io_file = open(filename, 'w')
                io_file.write(xml_content)
                io_file.close()
                self.projects.loc[code, 'xmlfile'] = xmlfilename
                return xmlfilename, obsproj
            except IndexError:
                print("Project %s not found on archive?" %
                      self.projects.ix[code])
                return 0
        else:
            print("Copying Project %s obsproposal.xml, status %s. (%s/%s)" %
                  (code, self.projects.ix[code, 'PRJ_STATUS'], c, n))
            xmlfilename = self.projects.ix[code, 'OBSPROJECT_UID'].replace(
                '://', '___').replace('/', '_')
            xmlfilename += '.xml'
            path_ori = self.phase1_data + 'ObsProject/' + xmlfilename
            path_dest = self.obsxml + '.'
            call('cp %s %s' % (path_ori, path_dest), shell=True)
            obsproj = False
            self.projects.loc[code, 'xmlfile'] = xmlfilename
            return xmlfilename, obsproj

    def read_obsproject(self, xml):

        try:
            obsparse = ObsProject(xml, self.obsxml)
        except KeyError:
            print("Something went wrong while trying to parse %s" % xml)
            return 0

        code = obsparse.code.pyval
        prj_version = obsparse.version.pyval
        staff_note = obsparse.staffProjectNote.pyval
        is_calibration = obsparse.isCalibration.pyval
        obsproject_uid = obsparse.ObsProjectEntity.attrib['entityId']
        try:
            is_ddt = obsparse.isDDT.pyval
        except AttributeError:
            is_ddt = False

        try:
            self.obsprojects.ix[code] = (
                code, obsproject_uid, prj_version, staff_note, is_ddt,
                is_calibration
            )
        except AttributeError:
            self.obsprojects = pd.DataFrame(
                [(code, obsproject_uid, prj_version, staff_note, is_ddt,
                  is_calibration)],
                columns=['CODE', 'OBSPROJECT_UID', 'PRJ_VERSION', 'staffNote',
                         'isDDT', 'isCalibration'],
                index=[code]
            )

        obsprog = obsparse.ObsProgram
        sg_list = obsprog.findall(prj + 'ScienceGoal')
        c = 0
        for sg in sg_list:
            self.read_sciencegoals(sg, obsproject_uid, c + 1, True, obsprog)
            c += 1

    def read_obsproposal(self, xml, code):

        try:
            obsparse = ObsProject(xml, self.obsxml)
        except KeyError:
            print("Something went wrong while trying to parse %s" % xml)
            return 0

        prj_version = None
        staff_note = None
        is_calibration = None
        obsproject_uid = obsparse.ObsProjectEntity.attrib['entityId']
        obsproposal_uid = obsparse.ObsProposalRef.attrib['entityId']
        is_ddt = None

        try:
            self.obsproposals.ix[code] = (
                code, obsproject_uid, obsproposal_uid, prj_version, staff_note,
                is_ddt, is_calibration
            )
        except AttributeError:
            self.obsproposals = pd.DataFrame(
                [(code, obsproject_uid, obsproposal_uid, prj_version,
                  staff_note, is_ddt, is_calibration)],
                columns=['CODE', 'OBSPROJECT_UID', 'OBSPROPOSAL_UID',
                         'PRJ_VERSION', 'staffNote', 'isDDT', 'isCalibration'],
                index=[code]
            )

        xmlfilename = obsproposal_uid.replace(
            '://', '___').replace('/', '_')
        xmlfilename += '.xml'
        path_ori = self.phase1_data + 'ObsProposal/' + xmlfilename
        path_dest = self.propxml + '.'
        call('cp %s %s' % (path_ori, path_dest), shell=True)
        obsparse = ObsProposal(xmlfilename, path=self.propxml)

        sg_list = obsparse.data.findall(prj + 'ScienceGoal')
        c = 0
        for sg in sg_list:
            self.read_sciencegoals(sg, obsproject_uid, c + 1, False, None)
            c += 1

    def read_sciencegoals(self, sg, obsproject_uid, idnum, isObsproj, obsprog):

        sg_id = obsproject_uid + '_' + str(idnum)
        try:
            ous_id = sg.ObsUnitSetRef.attrib['partId']
            hasSB = True
        except AttributeError:
            ous_id = None
            hasSB = False
        sg_name = sg.name.pyval
        bands = sg.findall(prj + 'requiredReceiverBands')[0].pyval
        estimatedTime = convert_tsec(
            sg.estimatedTotalTime.pyval,
            sg.estimatedTotalTime.attrib['unit']) / 3600.

        performance = sg.PerformanceParameters
        AR = convert_sec(
            performance.desiredAngularResolution.pyval,
            performance.desiredAngularResolution.attrib['unit'])
        LAS = convert_sec(
            performance.desiredLargestScale.pyval,
            performance.desiredLargestScale.attrib['unit'])
        sensitivity = convert_jy(
            performance.desiredSensitivity.pyval,
            performance.desiredSensitivity.attrib['unit'])
        useACA = performance.useACA.pyval
        useTP = performance.useTP.pyval
        isPointSource = performance.isPointSource.pyval
        try:
            isTimeConstrained = performance.isTimeConstrained.pyval
        except AttributeError:
            isTimeConstrained = None
        spectral = sg.SpectralSetupParameters
        repFreq = convert_ghz(
            performance.representativeFrequency.pyval,
            performance.representativeFrequency.attrib['unit'])
        polarization = spectral.attrib['polarisation']
        type_pol = spectral.attrib['type']
        ARcor = AR * repFreq / 100.
        LAScor = LAS * repFreq / 100.

        two_12m = False
        targets = sg.findall(prj + 'TargetParameters')
        num_targets = len(targets)
        c = 1
        for t in targets:
            self.read_pro_targets(t, sg_id, obsproject_uid, c)
            c += 1

        extendedTime, compactTime, sevenTime, TPTime = distribute_time(
            0., 0., 0., 0.)

        try:
            self.sciencegoals.ix[sg_id] = (
                sg_id, obsproject_uid, ous_id, sg_name, bands, estimatedTime,
                extendedTime, compactTime, sevenTime, TPTime, AR, LAS, ARcor,
                LAScor, sensitivity, useACA, useTP, isTimeConstrained, repFreq,
                isPointSource, polarization, type_pol, hasSB, two_12m,
                num_targets, isObsproj)
        except AttributeError:
            self.sciencegoals = pd.DataFrame(
                [(sg_id, obsproject_uid, ous_id, sg_name, bands, estimatedTime,
                  extendedTime, compactTime, sevenTime, TPTime, AR, LAS, ARcor,
                  LAScor, sensitivity, useACA, useTP, isTimeConstrained,
                  repFreq, isPointSource, polarization, type_pol, hasSB,
                  two_12m, num_targets, isObsproj)],
                columns=['SG_ID', 'OBSPROJECT_UID', 'OUS_ID', 'sg_name', 'band',
                         'estimatedTime', 'eExt12Time', 'eComp12Time',
                         'eACATime', 'eTPTime',
                         'AR', 'LAS', 'ARcor', 'LAScor', 'sensitivity',
                         'useACA', 'useTP', 'isTimeConstrained', 'repFreq',
                         'isPointSource', 'polarization', 'type', 'hasSB',
                         'two_12m', 'num_targets', 'isPhaseII'],
                index=[sg_id]
            )

        if isObsproj:
            oussg_list = obsprog.ObsPlan.findall(prj + 'ObsUnitSet')
            for oussg in oussg_list:
                groupous_list = oussg.findall(prj + 'ObsUnitSet')
                OUS_ID = oussg.attrib['entityPartId']
                if OUS_ID != ous_id:
                    continue
                ous_name = oussg.name.pyval
                OBSPROJECT_UID = oussg.ObsProjectRef.attrib['entityId']
                for groupous in groupous_list:
                    gous_id = groupous.attrib['entityPartId']
                    mous_list = groupous.findall(prj + 'ObsUnitSet')
                    gous_name = groupous.name.pyval
                    for mous in mous_list:
                        mous_id = mous.attrib['entityPartId']
                        mous_name = mous.name.pyval
                        try:
                            SB_UID = mous.SchedBlockRef.attrib['entityId']
                        except AttributeError:
                            continue
                        oucontrol = mous.ObsUnitControl
                        execount = oucontrol.aggregatedExecutionCount.pyval
                        array = mous.ObsUnitControl.attrib['arrayRequested']
                        sql = str(
                            "SELECT TIMESTAMP, XMLTYPE.getClobVal(xml) "
                            "FROM ALMA.xml_schedblock_entities "
                            "WHERE archive_uid = '%s'" % SB_UID)
                        self.cursor.execute(sql)
                        data = self.cursor.fetchall()
                        xml_content = data[0][1].read()
                        filename = SB_UID.replace(':', '_').replace('/', '_') +\
                            '.xml'
                        io_file = open(self.sbxml + filename, 'w')
                        io_file.write(xml_content)
                        io_file.close()
                        xml = filename

                        if array == 'ACA':
                            array = 'SEVEN-M'
                        try:
                            self.sb_sg_p2.ix[SB_UID] = (
                                SB_UID, OBSPROJECT_UID, sg_id,
                                ous_id, ous_name, gous_id,
                                gous_name, mous_id, mous_name,
                                array, execount, xml)
                        except AttributeError:
                            self.sb_sg_p2 = pd.DataFrame(
                                [(SB_UID, OBSPROJECT_UID, sg_id,
                                  ous_id, ous_name, gous_id,
                                  gous_name, mous_id, mous_name,
                                  array, execount, xml)],
                                columns=[
                                    'SB_UID', 'OBSPROJECT_UID', 'SG_ID',
                                    'OUS_ID', 'ous_name', 'GOUS_ID',
                                    'gous_name', 'MOUS_ID', 'mous_name',
                                    'array', 'execount', 'xmlfile'],
                                index=[SB_UID]
                            )

    def read_pro_targets(self, target, sgid, obsp_uid, c):

        tid = sgid + '_' + str(c)
        try:
            solarSystem = target.attrib['solarSystemObject']
        except KeyError:
            solarSystem = None

        typetar = target.attrib['type']
        sourceName = target.sourceName.pyval
        coord = target.sourceCoordinates
        coord_type = coord.attrib['system']
        if coord_type == 'J2000':
            ra = convert_deg(coord.findall(val + 'longitude')[0].pyval,
                             coord.findall(val + 'longitude')[0].attrib['unit'])
            dec = convert_deg(coord.findall(val + 'latitude')[0].pyval,
                              coord.findall(val + 'latitude')[0].attrib['unit'])
        elif coord_type == 'galactic':
            lon = convert_deg(
                coord.findall(val + 'longitude')[0].pyval,
                coord.findall(val + 'longitude')[0].attrib['unit'])
            lat = convert_deg(
                coord.findall(val + 'latitude')[0].pyval,
                coord.findall(val + 'latitude')[0].attrib['unit'])
            eph = ephem.Galactic(pd.np.radians(lon), pd.np.radians(lat))
            ra = pd.np.degrees(eph.to_radec()[0])
            dec = pd.np.degrees(eph.to_radec()[1])
        else:
            print "coord type is %s, deal with it" % coord_type
            ra = 0
            dec = 0
        try:
            isMosaic = target.isMosaic.pyval
        except AttributeError:
            isMosaic = None

        try:
            self.sg_targets.ix[tid] = (
                tid, obsp_uid, sgid, typetar, solarSystem, sourceName, ra, dec,
                isMosaic)
        except AttributeError:
            self.sg_targets = pd.DataFrame(
                [(tid, obsp_uid, sgid, typetar, solarSystem, sourceName, ra,
                  dec, isMosaic)],
                columns=['TARG_ID', 'OBSPROJECT_UID', 'SG_ID', 'tarType',
                         'solarSystem', 'sourceName', 'RA', 'DEC', 'isMosaic'],
                index=[tid]
            )

    def filter_c1(self):
        """


        """
        c1c2 = pd.read_csv(
            self.apa_path + 'conf/c1c2.csv', sep=',', header=0,
            usecols=range(5))
        c1c2.columns = pd.Index([u'CODE', u'Region', u'ARC', u'C2', u'P2G'],
                                dtype='object')
        toc2 = c1c2[c1c2.fillna('no').C2.str.startswith('Yes')]
        check_c1 = pd.merge(
            self.projects[self.projects.CODE.str.startswith('2012')],
            toc2, on='CODE', how='right').set_index(
                'CODE', drop=False)[['CODE']]
        check_c2 = self.projects[
            self.projects.CODE.str.startswith('2013')][['CODE']]
        grades = self.grades[
            (self.grades.aprcflag == 'A') | (self.grades.aprcflag == 'B') |
            (self.grades.aprcflag == 'C')]
        check_c2_g = pd.merge(
            grades, check_c2, on='CODE', how='left').set_index(
                'CODE', drop=False)[['CODE']]
        checked = pd.concat([check_c1, check_c2_g])
        temp = pd.merge(
            self.projects, checked, on='CODE',
            copy=False, how='inner').set_index('CODE', drop=False)
        self.projects = temp

    def read_schedblocks_p2(self, sb_uid, obs_uid, ous_id, new=False):

        # Open SB with SB parser class
        """

        :param sb_uid:
        :param new:
        """
        print("Procesing Phase II SB %s" % sb_uid)
        sb = self.sb_sg_p2.ix[sb_uid]
        sg_id = sb.SG_ID
        xml = SchedBlock(sb.xmlfile, self.sbxml)
        new_orig = new
        # Extract root level data
        array = xml.data.findall(
            './/' + prj + 'ObsUnitControl')[0].attrib['arrayRequested']
        name = xml.data.findall('.//' + prj + 'name')[0].pyval
        type12m = 'None'
        if name.rfind('TC') != -1:
            type12m = 'Comp'
        elif array == 'TWELVE-M':
            type12m = 'Ext'
        status = xml.data.attrib['status']

        schedconstr = xml.data.SchedulingConstraints
        schedcontrol = xml.data.SchedBlockControl
        preconditions = xml.data.Preconditions
        weather = preconditions.findall('.//' + prj + 'WeatherConstraints')[0]

        try:
            # noinspection PyUnusedLocal
            polarparam = xml.data.PolarizationCalParameters
            ispolarization = True
        except AttributeError:
            ispolarization = False

        repfreq = schedconstr.representativeFrequency.pyval
        ra = schedconstr.representativeCoordinates.findall(
            val + 'longitude')[0].pyval
        dec = schedconstr.representativeCoordinates.findall(
            val + 'latitude')[0].pyval
        minar_old = schedconstr.minAcceptableAngResolution.pyval
        maxar_old = schedconstr.maxAcceptableAngResolution.pyval
        band = schedconstr.attrib['representativeReceiverBand']

        execount = schedcontrol.executionCount.pyval
        maxpwv = weather.maxPWVC.pyval

        n_fs = len(xml.data.FieldSource)
        n_tg = len(xml.data.Target)
        n_ss = len(xml.data.SpectralSpec)

        for n in range(n_fs):
            if new:
                self.read_fieldsource(xml.data.FieldSource[n], sb_uid, array,
                                      new=new)
                new = False
            else:
                self.read_fieldsource(xml.data.FieldSource[n], sb_uid, array)

        new = new_orig
        for n in range(n_tg):
            if new:
                self.read_target(xml.data.Target[n], sb_uid, new=new)
                new = False
            else:
                self.read_target(xml.data.Target[n], sb_uid)

        new = new_orig
        for n in range(n_ss):
            if new:
                self.read_spectralconf(xml.data.SpectralSpec[n], sb_uid,
                                       new=new)
                new = False
            else:
                self.read_spectralconf(xml.data.SpectralSpec[n], sb_uid)

        try:
            self.schedblocks_p2.ix[sb_uid] = (
                sb_uid, obs_uid, sg_id, ous_id,
                name, status, repfreq, band, array,
                ra, dec, minar_old, maxar_old, execount,
                ispolarization, maxpwv, type12m)
        except AttributeError:
            self.schedblocks_p2 = pd.DataFrame(
                [(sb_uid, obs_uid, sg_id, ous_id,
                  name, status, repfreq, band, array,
                  ra, dec, minar_old, maxar_old, execount,
                  ispolarization, maxpwv, type12m)],
                columns=['SB_UID', 'OBSPROJECT_UID', 'SG_ID', 'OUS_ID',
                         'sbName', 'sbStatusXml', 'repfreq', 'band', 'array',
                         'RA', 'DEC', 'minAR_ot', 'maxAR_ot', 'execount',
                         'isPolarization', 'maxPWVC', 'array12mType'],
                index=[sb_uid])

    def read_schedblocks_p1(self, sb_uid, obs_uid, xml):

        # Open SB with SB parser class
        """

        :param sb_uid:
        """
        sg_id = None
        ous_id = None
        # Extract root level data
        array = xml.data.findall(
            './/' + prj + 'ObsUnitControl')[0].attrib['arrayRequested']
        name = xml.data.findall('.//' + prj + 'name')[0].pyval
        type12m = 'None'
        if name.rfind('TC') != -1:
            type12m = 'Comp'
        elif array == 'TWELVE-M':
            type12m = 'Ext'
        status = xml.data.attrib['status']

        schedconstr = xml.data.SchedulingConstraints
        schedcontrol = xml.data.SchedBlockControl
        preconditions = xml.data.Preconditions
        weather = preconditions.findall('.//' + prj + 'WeatherConstraints')[0]

        try:
            # noinspection PyUnusedLocal
            polarparam = xml.data.PolarizationCalParameters
            ispolarization = True
        except AttributeError:
            ispolarization = False

        repfreq = schedconstr.representativeFrequency.pyval
        ra = schedconstr.representativeCoordinates.findall(
            val + 'longitude')[0].pyval
        dec = schedconstr.representativeCoordinates.findall(
            val + 'latitude')[0].pyval
        minar_old = schedconstr.minAcceptableAngResolution.pyval
        maxar_old = schedconstr.maxAcceptableAngResolution.pyval
        band = schedconstr.attrib['representativeReceiverBand']

        execount = schedcontrol.executionCount.pyval
        maxpwv = weather.maxPWVC.pyval

        n_fs = len(xml.data.FieldSource)
        n_tg = len(xml.data.Target)
        n_ss = len(xml.data.SpectralSpec)

        for n in range(n_fs):
            self.read_fieldsource(xml.data.FieldSource[n], sb_uid, array)

        for n in range(n_tg):
            self.read_target(xml.data.Target[n], sb_uid)

        for n in range(n_ss):
            self.read_spectralconf(xml.data.SpectralSpec[n], sb_uid)

        try:
            self.schedblocks_p1.ix[sb_uid] = (
                sb_uid, obs_uid, sg_id, ous_id,
                name, status, repfreq, band, array,
                ra, dec, minar_old, maxar_old, execount,
                ispolarization, maxpwv, type12m)
        except AttributeError:
            self.schedblocks_p1 = pd.DataFrame(
                [(sb_uid, obs_uid, sg_id, ous_id,
                  name, status, repfreq, band, array,
                  ra, dec, minar_old, maxar_old, execount,
                  ispolarization, maxpwv, type12m)],
                columns=['SB_UID', 'OBSPROJECT_UID', 'SG_ID', 'OUS_ID',
                         'sbName', 'sbStatusXml', 'repfreq', 'band', 'array',
                         'RA', 'DEC', 'minAR_ot', 'maxAR_ot', 'execount',
                         'isPolarization', 'maxPWVC', 'array12mType'],
                index=[sb_uid])

    def read_fieldsource(self, fs, sbuid, array, new=False):
        """

        :param fs:
        :param sbuid:
        :param new:
        """
        partid = fs.attrib['entityPartId']
        coord = fs.sourceCoordinates
        solarsystem = fs.attrib['solarSystemObject']
        sourcename = fs.sourceName.pyval
        name = fs.name.pyval
        isquery = fs.isQuery.pyval
        pointings = len(fs.findall(sbl + 'PointingPattern/' + sbl +
                                   'phaseCenterCoordinates'))
        try:
            ismosaic = fs.PointingPattern.isMosaic.pyval
        except AttributeError:
            ismosaic = False
        if isquery:
            querysource = fs.QuerySource
            qc_intendeduse = querysource.attrib['intendedUse']
            qcenter = querysource.queryCenter
            qc_ra = qcenter.findall(val + 'longitude')[0].pyval
            qc_dec = qcenter.findall(val + 'latitude')[0].pyval
            qc_use = querysource.use.pyval
            qc_radius = querysource.searchRadius.pyval
            qc_radius_unit = querysource.searchRadius.attrib['unit']
        else:
            qc_intendeduse, qc_ra, qc_dec, qc_use, qc_radius, qc_radius_unit = (
                None, None, None, None, None, None
            )
        coord_type = coord.attrib['system']
        if coord_type == 'J2000':
            ra = convert_deg(coord.findall(val + 'longitude')[0].pyval,
                             coord.findall(val + 'longitude')[0].attrib['unit'])
            dec = convert_deg(coord.findall(val + 'latitude')[0].pyval,
                              coord.findall(val + 'latitude')[0].attrib['unit'])
        elif coord_type == 'galactic':
            lon = convert_deg(
                coord.findall(val + 'longitude')[0].pyval,
                coord.findall(val + 'longitude')[0].attrib['unit'])
            lat = convert_deg(
                coord.findall(val + 'latitude')[0].pyval,
                coord.findall(val + 'latitude')[0].attrib['unit'])
            eph = ephem.Galactic(pd.np.radians(lon), pd.np.radians(lat))
            ra = pd.np.degrees(eph.to_radec()[0])
            dec = pd.np.degrees(eph.to_radec()[1])
        else:
            print "coord type is %s, deal with it" % coord_type
            ra = 0
            dec = 0
        if solarsystem == 'Ephemeris':
            ephemeris = fs.sourceEphemeris.pyval
        else:
            ephemeris = None
        if new:
            self.fieldsource = pd.DataFrame(
                [(partid, sbuid, solarsystem, sourcename, name, ra, dec,
                  isquery, qc_intendeduse, qc_ra, qc_dec, qc_use, qc_radius,
                  qc_radius_unit, ephemeris, pointings, ismosaic, array)],
                columns=['fieldRef', 'SB_UID', 'solarSystem', 'sourcename',
                         'name', 'RA', 'DEC', 'isQuery', 'intendedUse', 'qRA',
                         'qDEC', 'use', 'search_radius', 'rad_unit',
                         'ephemeris', 'pointings', 'isMosaic', 'arraySB'],
                index=[partid]
            )
        self.fieldsource.ix[partid] = (
            partid, sbuid, solarsystem, sourcename, name, ra, dec, isquery,
            qc_intendeduse, qc_ra, qc_dec, qc_use, qc_radius, qc_radius_unit,
            ephemeris, pointings, ismosaic, array)

    def read_target(self, tg, sbuid, new=False):
        """

        :param tg:
        :param sbuid:
        :param new:
        """
        partid = tg.attrib['entityPartId']
        specref = tg.AbstractInstrumentSpecRef.attrib['partId']
        fieldref = tg.FieldSourceRef.attrib['partId']
        paramref = tg.ObservingParametersRef.attrib['partId']
        if new:
            self.target = pd.DataFrame(
                [(partid, sbuid, specref, fieldref, paramref)],
                columns=['targetId', 'SB_UID', 'specRef', 'fieldRef',
                         'paramRef'],
                index=[partid])
        else:
            self.target.ix[partid] = (partid, sbuid, specref, fieldref,
                                      paramref)

    def read_spectralconf(self, ss, sbuid, new=False):
        """

        :param ss:
        :param sbuid:
        :param new:
        """
        partid = ss.attrib['entityPartId']
        try:
            corrconf = ss.BLCorrelatorConfiguration
            nbb = len(corrconf.BLBaseBandConfig)
            nspw = 0
            for n in range(nbb):
                bbconf = corrconf.BLBaseBandConfig[n]
                nspw += len(bbconf.BLSpectralWindow)
        except AttributeError:
            corrconf = ss.ACACorrelatorConfiguration
            nbb = len(corrconf.ACABaseBandConfig)
            nspw = 0
            for n in range(nbb):
                bbconf = corrconf.ACABaseBandConfig[n]
                nspw += len(bbconf.ACASpectralWindow)
        if new:
            self.spectralconf = pd.DataFrame(
                [(partid, sbuid, nbb, nspw)],
                columns=['specRef', 'SB_UID', 'BaseBands', 'SPWs'],
                index=[partid])
        else:
            self.spectralconf.ix[partid] = (partid, sbuid, nbb, nspw)

    def do_summarize_sb(self):
        sum2 = pd.merge(
            self.schedblocks_p2,
            self.newAR_p2_input[
                ['SB_UID', 'AR', 'LAS', 'minArrayAR', 'maxArrayAR']],
            how='left'
        ).set_index('SB_UID', drop=False)

        for col in ['AR', 'LAS', 'minArrayAR', 'maxArrayAR']:
            sum2[col + '100GHz'] = sum2.apply(
                lambda r: correct_resolution(
                    r[col], r['repfreq'], r['RA'], r['DEC']), axis=1)
        for col in confDf.index.tolist():
            sum2[col.replace('-', '_')] = sum2.apply(
                lambda r: check_allowedconf(
                    r['minArrayAR100GHz'], r['maxArrayAR100GHz'], col),
                axis=1)

        sum2['allowed12m'] = sum2.iloc[
            :, [-7, -6, -5, -4, -3, -2, -1]].sum(axis=1)
        sum2.loc[:, ['minArrayAR100GHz', 'maxArrayAR100GHz']] = sum2.apply(
            lambda r: fix_allowedconf(r['minArrayAR100GHz'],
                                      r['maxArrayAR100GHz'],
                                      r['allowed12m'],
                                      r['array']),
            axis=1).values

        for col in confDf.index.tolist():
            sum2[col.replace('-', '_')] = sum2.apply(
                lambda r: check_allowedconf(
                    r['minArrayAR100GHz'], r['maxArrayAR100GHz'], col),
                axis=1)
        sum2['corr_allowed12m'] = sum2.iloc[
            :, [-8, -7, -6, -5, -4, -3, -2]].sum(axis=1)
        sum2['phase'] = 'II'

        sum1 = pd.merge(
            self.schedblocks_p1,
            self.newAR_p1_input[
                ['SB_UID', 'AR', 'LAS', 'minArrayAR', 'maxArrayAR']],
            how='left'
        ).set_index('SB_UID', drop=False)

        for col in ['AR', 'LAS', 'minArrayAR', 'maxArrayAR']:
            sum1[col + '100GHz'] = sum1.apply(
                lambda r: correct_resolution(
                    r[col], r['repfreq'], r['RA'], r['DEC']), axis=1)
        for col in confDf.index.tolist():
            sum1[col.replace('-', '_')] = sum1.apply(
                lambda r: check_allowedconf(
                    r['minArrayAR100GHz'], r['maxArrayAR100GHz'], col),
                axis=1)

        sum1['allowed12m'] = sum1.iloc[
            :, [-7, -6, -5, -4, -3, -2, -1]].sum(axis=1)
        sum1.loc[:, ['minArrayAR100GHz', 'maxArrayAR100GHz']] = sum1.apply(
            lambda r: fix_allowedconf(r['minArrayAR100GHz'],
                                      r['maxArrayAR100GHz'],
                                      r['allowed12m'],
                                      r['array']),
            axis=1).values

        for col in confDf.index.tolist():
            sum1[col.replace('-', '_')] = sum1.apply(
                lambda r: check_allowedconf(
                    r['minArrayAR100GHz'], r['maxArrayAR100GHz'], col),
                axis=1)
        sum1['corr_allowed12m'] = sum1.iloc[
            :, [-8, -7, -6, -5, -4, -3, -2]].sum(axis=1)
        sum1['phase'] = 'I'

        sum_schedblock = pd.concat([sum2, sum1])
        todrop = sum_schedblock[
            sum_schedblock.sbName.str.contains('Do ')].index.tolist()
        todrop.extend(
            sum_schedblock[
                sum_schedblock.sbName.str.contains('do ')].index.tolist())
        todrop.extend(
            sum_schedblock[
                sum_schedblock.sbName.str.contains('do_')].index.tolist())
        todrop.extend(
            sum_schedblock[
                sum_schedblock.sbName.str.contains('Do_')].index.tolist())
        todrop.extend(
            sum_schedblock[
                sum_schedblock.sbName.str.contains('DO_N')].index.tolist())
        todrop.extend(
            sum_schedblock[
                sum_schedblock.sbName.str.contains('DO ')].index.tolist())
        print len(sum_schedblock.query('SB_UID in @todrop').sbName)
        sum_schedblock = sum_schedblock.drop(todrop)
        self.summary_sb = sum_schedblock.copy(deep=True)

        self.summary_sb.loc[:, 'SB_ETC_exec'] = self.summary_sb.apply(
            lambda r: self.sb_eta(r['SG_ID'], r['array'], r['array12mType']),
            axis=1)
        self.qa0 = self.aqua_execblock.groupby(
            ['SB_UID', 'QA0STATUS']).QA0STATUS.count().unstack().fillna(0)
        self.qa0['observed'] = self.qa0.Pass + self.qa0.Unset
        self.summary_sb = pd.merge(
            self.summary_sb, self.qa0[['observed']],
            left_on='SB_UID', right_index=True, how='left')
        self.summary_sb.observed.fillna(0, inplace=True)
        self.summary_sb = pd.merge(
            self.projects[['OBSPROJECT_UID', 'CODE', 'PRJ_LETTER_GRADE',
                           'PRJ_STATUS', 'isCycle2']],
            self.summary_sb, on='OBSPROJECT_UID', how='right')

        self.summary_sb['SB_ETC_total'] = self.summary_sb['SB_ETC_exec'] * \
            self.summary_sb['execount']
        self.summary_sb['SB_ETC_remain'] = self.summary_sb['SB_ETC_total'] * (
            self.summary_sb['execount'] - self.summary_sb['observed']
        ) / self.summary_sb['execount']

        # noinspection PyUnresolvedReferences
        self.summary_sb['LST'] = (self.summary_sb.RA / 15.).astype(int)

    def sb_eta(self, sg_id, array, arrayt):
        if array == 'TWELVE-M' and arrayt == 'Ext':
            time_sg = self.sciencegoals.ix[sg_id].eExt12Time
            sb_sg = self.summary_sb.query(
                'SG_ID == @sg_id and array == @array and '
                'array12mType == @arrayt')
        elif array == 'TWELVE-M' and arrayt == 'Comp':
            time_sg = self.sciencegoals.ix[sg_id].eComp12Time
            sb_sg = self.summary_sb.query(
                'SG_ID == @sg_id and array == @array and '
                'array12mType == @arrayt')
        elif array == 'SEVEN-M':
            time_sg = self.sciencegoals.ix[sg_id].eACATime
            sb_sg = self.summary_sb.query(
                'SG_ID == @sg_id and array == @array')
        else:
            time_sg = self.sciencegoals.ix[sg_id].eTPTime
            sb_sg = self.summary_sb.query(
                'SG_ID == @sg_id and array == @array')

        return time_sg / sb_sg.execount.sum()



def distribute_time(tiempo, doce, siete, single):

    if single and doce:
        time_u = tiempo / (1 + 0.5 + 2 + 4)
        return pd.Series([time_u, 0.5 * time_u, 2 * time_u, 4 * time_u],
                         index=['eExt12Time', 'eComp12Time', 'eACATime',
                                'eTPTime'])
    elif single and not doce:
        time_u = tiempo / (1 + 2 + 4.)
        return pd.Series([time_u, 0., 2 * time_u, 4 * time_u],
                         index=['eExt12Time', 'eComp12Time', 'eACATime',
                                'eTPTime'])
    elif siete and doce:
        time_u = tiempo / (1 + 0.5 + 2.)
        return pd.Series([time_u, 0.5 * time_u, 2 * time_u, 0.],
                         index=['eExt12Time', 'eComp12Time', 'eACATime',
                                'eTPTime'])
    elif siete and not doce:
        time_u = tiempo / (1 + 2.)
        return pd.Series([time_u, 0., 2 * time_u, 0.],
                         index=['eExt12Time', 'eComp12Time', 'eACATime',
                                'eTPTime'])
    elif doce:
        time_u = tiempo / 1.5
        return pd.Series([time_u, 0.5 * time_u, 0., 0.],
                         index=['eExt12Time', 'eComp12Time', 'eACATime',
                                'eTPTime'])
    elif not doce:
        return pd.Series([tiempo, 0., 0., 0.],
                         index=['eExt12Time', 'eComp12Time', 'eACATime',
                                'eTPTime'])
    else:
        print("couldn't distribute time...")
        return pd.Series([None, None, None, None],
                         index=['eExt12Time', 'eComp12Time', 'eACATime',
                                'eTPTime'])


def new_array_ar(path, ar, las, repfreq, useaca, sbnum, type12):

    if sbnum:
        sbnum = 2
    else:
        sbnum = 1

    new_ar = ARes.arrayRes(
        [path, ar, las, repfreq, useaca, sbnum])
    new_ar.silentRun()
    minar_e, maxar_e, minar_c, maxar_c = new_ar.run()

    if type12 == 'Ext':
        return pd.Series([minar_e, maxar_e], index=['minArrayAR',
                                                    'maxArrayAR'])
    else:
        return pd.Series([minar_c, maxar_c], index=['minArrayAR',
                                                    'maxArrayAR'])


# noinspection PyUnresolvedReferences
def correct_resolution(res, repfreq, ra, dec):
    if ra == 0. and dec == 0:
        dec = -23.0262015
    c_bmax = 0.4001 / pd.np.cos(pd.np.radians(-23.0262015) -
                                pd.np.radians(dec)) + 0.6103
    c_freq = repfreq / 100.
    corr = c_freq / c_bmax
    return corr * res


def check_allowedconf(min_ar, max_ar, conf, confdf=confDf):
    conf_res = confdf.ix[conf].ALMA_RB_03
    if min_ar <= conf_res <= max_ar:
        return 1
    else:
        return 0


def fix_allowedconf(min_ar, max_ar, allowed, array):
    if allowed > 0 or array != 'TWELVE-M':
        return pd.Series([min_ar, max_ar])
    else:
        return pd.Series([0.9 * min_ar, 1.1 * max_ar])

