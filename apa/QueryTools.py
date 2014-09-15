__author__ = 'itoledo'

import pandas as pd
import cx_Oracle


conx_string = 'almasu/alma4dba@ALMA_ONLINE.OSF.CL'


class QueryArchive(object):

    def __init__(self, conx=conx_string):

        self.connection = cx_Oracle.connect(conx)
        self.cursor = self.connection.cursor()

    def close_cursor(self):

        self.cursor.close()
        self.connection.close()

    def query_projects_pt(self, filter_file=None):

        sql_string_proj = str(
            "SELECT PRJ_ARCHIVE_UID as OBSPROJECT_UID,PI,PRJ_NAME,"
            "CODE,PRJ_SCIENTIFIC_RANK,PRJ_VERSION,"
            "PRJ_LETTER_GRADE,DOMAIN_ENTITY_STATE as PRJ_STATUS,"
            "ARCHIVE_UID as OBSPROPOSAL_UID "
            "FROM ALMA.BMMV_OBSPROJECT obs1, ALMA.OBS_PROJECT_STATUS obs2,"
            " ALMA.BMMV_OBSPROPOSAL obs3 "
            "WHERE regexp_like (CODE, '^201[23].*\.[AST]') "
            "AND (PRJ_LETTER_GRADE='A' OR PRJ_LETTER_GRADE='B' "
            "     OR PRJ_LETTER_GRADE='C') "
            "AND obs2.OBS_PROJECT_ID = obs1.PRJ_ARCHIVE_UID AND "
            "obs1.PRJ_ARCHIVE_UID = obs3.PROJECTUID"
        )

        sql_string_executives = str(
            "SELECT PROJECTUID as OBSPROJECT_UID, ASSOCIATEDEXEC "
            "FROM ALMA.BMMV_OBSPROPOSAL "
            "WHERE regexp_like (CYCLE, '^201[23].[1A]')"
        )

        # noinspection PyUnusedLocal
        status = ["Canceled", "Rejected"]

        self.cursor.execute(sql_string_proj)
        df_proj = pd.DataFrame(
            self.cursor.fetchall(),
            columns=[rec[0] for rec in self.cursor.description]
        )

        self.cursor.execute(sql_string_executives)
        df_exe = pd.DataFrame(
            self.cursor.fetchall(), columns=['OBSPROJECT_UID', 'EXECUTIVE']
        )

        df_proj_exe = pd.merge(
            df_proj.query('PRJ_STATUS not in @status'), df_exe,
            on='OBSPROJECT_UID'
        )

        if filter_file is not None:
            projects_pt = filter_c1(df_proj_exe, filter_file)

        else:
            projects_pt = df_proj_exe

        timestamp = pd.Series(
            pd.np.zeros(len(projects_pt), dtype=object),
            index=projects_pt.index)
        projects_pt['timestamp'] = timestamp
        projects_pt['xmlfile'] = pd.Series(
            pd.np.zeros(len(projects_pt), dtype=object),
            index=projects_pt.index)

        return projects_pt

    def query_aqua(self):

        sql_string = str(
            "SELECT SCHEDBLOCKUID as SB_UID,QA0STATUS "
            "FROM ALMA.AQUA_EXECBLOCK "
            "WHERE regexp_like (OBSPROJECTCODE, '^201[23].*\.[AST]')"
        )

        self.cursor.execute(sql_string)
        aqua_execblock = pd.DataFrame(
            self.cursor.fetchall(),
            columns=[rec[0] for rec in self.cursor.description]
        )

        return aqua_execblock

    def query_projects_saos(self):

        sql_string = str(
            "SELECT CODE,OBSPROJECT_UID as OBSPROJECT_UID,"
            "VERSION as PRJ_SAOS_VERSION, STATUS as PRJ_SAOS_STATUS "
            "FROM SCHEDULING_AOS.OBSPROJECT "
            "WHERE regexp_like (CODE, '^201[23].*\.[AST]')")

        self.cursor.execute(sql_string)
        projects_saos = pd.DataFrame(
            self.cursor.fetchall(),
            columns=[rec[0] for rec in self.cursor.description]
        )

        return projects_saos

    def query_schedblocks_saos(self):

        sql_string = str(
            "SELECT ou.OBSUNIT_UID as OUS_ID, sb.NAME as SB_NAME,"
            "sb.SCHEDBLOCK_CTRL_EXEC_COUNT,"
            "sb.SCHEDBLOCK_CTRL_STATE as SB_SAOS_STATUS,"
            "ou.OBSUNIT_PROJECT_UID as OBSPROJECT_UID "
            "FROM SCHEDULING_AOS.SCHEDBLOCK sb, SCHEDULING_AOS.OBSUNIT ou "
            "WHERE sb.SCHEDBLOCKID = ou.OBSUNITID AND sb.CSV = 0"
        )

        self.cursor.execute(sql_string)

        schedblocks_saos = pd.DataFrame(
            self.cursor.fetchall(),
            columns=[rec[0] for rec in self.cursor.description]
        )

        return schedblocks_saos

    def query_schedblocks_s_c(self):

        sql_string = """
        with t1 as (SELECT PRJ_ARCHIVE_UID, CODE, PRJ_LETTER_GRADE
        FROM ALMA.BMMV_OBSPROJECT
        WHERE regexp_like (CODE, '^201[23].*\.[AST]')
        AND (PRJ_LETTER_GRADE='A' OR PRJ_LETTER_GRADE='B'
             OR PRJ_LETTER_GRADE='C')),
        t2 as (SELECT ARCHIVE_UID, EXECUTION_COUNT, STATUS,
               PRJ_REF FROM ALMA.BMMV_SCHEDBLOCK)

        SELECT t2.ARCHIVE_UID as SB_UID, t2.EXECUTION_COUNT,
         t2.STATUS as SB_STATUS
        FROM t1,t2 WHERE t1.PRJ_ARCHIVE_UID = t2.PRJ_REF
        """

        self.cursor.execute(sql_string)

        schedblocks_s_c = pd.DataFrame(
            self.cursor.fetchall(),
            columns=[rec[0] for rec in self.cursor.description]
        )

        return schedblocks_s_c


def filter_c1(projects, filter_file):

    c1c2 = pd.read_csv(
        filter_file, sep=',', header=0, usecols=range(5)
    )

    c1c2.columns = pd.Index(
        [u'CODE', u'Region', u'ARC', u'C2', u'P2G'],
        dtype='object')

    toc2 = c1c2[c1c2.fillna('no').C2.str.startswith('Yes')]
    check_c1 = pd.merge(
        projects[projects.CODE.str.startswith('2012')], toc2, on='CODE',
        how='right')[['CODE']]
    check_c2 = projects[
        projects.CODE.str.startswith('2013')][['CODE']]
    checked = pd.concat([check_c1, check_c2])
    filtered = pd.merge(
        projects, checked, on='CODE', copy=False, how='inner')

    return filtered
