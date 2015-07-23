import DataBase as DaBa
import pandas as pd
import numpy as np
import ephem

import cx_Oracle
import os

import cycle_tools as ct

datas = DaBa.Database(verbose=False, forcenew=True, path='/.apa_tabl/')
datas.process_sbs()
datas.do_summarize_sb()

alma1 = ephem.Observer()
alma1.lat = '-23.0262015'
alma1.long = '-67.7551257'
alma1.elev = 5060

data_ar = ct.create_dates(ct.es_cycle2)

date_df = pd.DataFrame(
    np.array(data_ar),
    columns=['start', 'end', 'block', 'C34_1', 'C34_2', 'C34_3', 'C34_4',
             'C34_5', 'C34_6', 'C34_7']
)

lst_times = date_df.apply(
    lambda r: ct.day_night(r['start'], r['end'], alma1), axis=1)
date_df = pd.concat([date_df, lst_times], axis=1)
date_df['available_time'] = date_df.apply(
    lambda r: (r['end'] - r['start']).total_seconds() / 3600.,
    axis=1)

date_df = date_df.ix[49:].copy()
conx_string = os.environ['CON_STR']
connection = cx_Oracle.connect(conx_string)
cursor = connection.cursor()
cursor.execute(
    "SELECT ACCOUNT_ID, LASTNAME, FIRSTNAME, EMAIL FROM ALMA.ACCOUNT")
users = pd.DataFrame(
    cursor.fetchall(),
    columns=[rec[0] for rec in cursor.description])

sqlqa0 = str("SELECT SCHEDBLOCKUID AS SB_UID, QA0STATUS, STARTTIME, ENDTIME,"
             "EXECBLOCKUID, EXECFRACTION "
             "FROM ALMA.AQUA_EXECBLOCK "
             "WHERE regexp_like (OBSPROJECTCODE, '^201[23].*\.[AST]')")

sql = str(
    'SELECT SE_ID, SE_TYPE, SE_SUBJECT, SE_TIMESTAMP, SE_AUTHOR, SE_START, '
    'SE_PROJECT_CODE, SE_SB_CODE, SE_SB_ID, SE_EB_UID, SE_LOCATION, SE_STATUS, '
    'SE_CALIBRATION, SE_QA0FLAG, SE_ARCHIVING_STATUS, SE_TEST_ACTIVITY, '
    'SE_ISPOWERCUT, SE_PCRECOVERYEND, SE_PCRECOVERYSTART, SE_WRECOVERYEND12M, '
    'SE_ISPOWERCUT, SE_PCRECOVERYEND, SE_PCRECOVERYSTART, SE_WRECOVERYEND12M, '
    'SE_WRECOVERYEND7M, SE_WRECOVERYENDTP, SE_WRECOVERYSTART12M, '
    'SE_WRECOVERYSTART7M, SE_WRECOVERYSTARTTP, SE_ARRAYENTRY_ID, '
    'SE_ARRAYNAME, SE_ARRAYTYPE, SE_ARRAYFAMILY, SE_CORRELATORTYPE, '
    'SE_PHOTONICREFERENCENAME, SE_ALMABUILD, SE_DOWNTIMETYPE, SE_MAINACTIVITY, '
    'SE_BANDNAME, SE_EXECUTIVE, SE_OBSPROJECTNAME, SE_OBSPROJECTPI, '
    'SE_OBSPROJECTVERSION, SE_SHIFTACTIVITY, SE_PWV, SE_REPRFREQUENCY '
    'FROM ALMA.SHIFTLOG_ENTRIES WHERE '
    'SE_TIMESTAMP > '
    'to_date(\'2014-04-27 00:00:00\', \'YYYY-MM-DD HH24:MI:SS\') '
    'AND SE_LOCATION = \'OSF-AOS\'')

cursor.execute(sqlqa0)

aqua_execblock = pd.DataFrame(
    cursor.fetchall(), columns=[rec[0] for rec in cursor.description]
).set_index('SB_UID', drop=False)

cursor.execute(sql)

shiftlog = pd.DataFrame(
    cursor.fetchall(), columns=[rec[0] for rec in cursor.description]
)
shiftlog.to_csv('/users/aod/data/shiftlog.csv', index=False, encoding='utf-8')
# Stales
sept_obs = aqua_execblock.query(
    'QA0STATUS == "Pass" and STARTTIME < "2014-10-30"').groupby('SB_UID').agg(
    {'QA0STATUS': pd.np.count_nonzero}
).reset_index()
now_obs = aqua_execblock.query(
    'QA0STATUS == "Pass" and STARTTIME >= "2014-10-30"').groupby('SB_UID').agg(
    {'QA0STATUS': pd.np.count_nonzero}
).reset_index()
sbsobs_sept = sept_obs.SB_UID.unique()
sbsobs_now = now_obs.SB_UID.unique()
septnow_sb = datas.summary_sb.query('SB_UID in @sbsobs_sept')
stalesep_sbs = septnow_sb.query('observed < execount').SB_UID.unique()

aqua_execblock['delta'] = aqua_execblock.ENDTIME - aqua_execblock.STARTTIME
d1 = pd.merge(datas.target, datas.scienceparam, on=['SB_UID', 'paramRef'])
d1 = pd.merge(d1, datas.fieldsource, on=['SB_UID', 'fieldRef'])
remnam = ['calibrator_J1256', 'Amplitude', '3c454.3']
d1 = d1.query('intendedUse != "phase"')
inttimes = d1.groupby('SB_UID').intTime.sum().reset_index()
inttimes['intTime'] = inttimes.intTime / 3600.
summary = pd.merge(datas.summary_sb, inttimes, on='SB_UID')
summary['SB_ETC2_exec'] = summary.intTime * 1.0972 + 0.4712

aqua = aqua_execblock.query('QA0STATUS == "Pass"').copy()
aqua.ix[aqua.ENDTIME.isnull(),
        'ENDTIME'] = aqua[aqua.ENDTIME.isnull()]['STARTTIME'] + \
    pd.datetools.timedelta(1 / 24.)

aqua['totExecTime'] = aqua.apply(
    lambda r: (r['ENDTIME'] - r['STARTTIME']).total_seconds() / 3600., axis=1)
aqua_times = aqua.groupby('SB_UID').sum().reset_index(drop=False)

pro_df_or = summary.groupby('CODE')[['execount', 'observed']].sum().sort()
pro_df_12 = pd.merge(
    pro_df_or,
    pd.merge(
        summary.query('array == "TWELVE-M"')[['CODE', 'SB_UID']],
        aqua_times, on='SB_UID').groupby('CODE').sum(),
    left_index=True, right_index=True, how='outer').fillna(0)
pro_df_all = pd.merge(
    pro_df_or,
    pd.merge(
        summary[['CODE', 'SB_UID']],
        aqua_times, on='SB_UID').groupby('CODE').sum(),
    left_index=True, right_index=True, how='outer').fillna(0)
pro_df_all['proj_comp_per'] = pro_df_all.observed / pro_df_all.execount
pro_df_12['proj_comp_per_12m'] = pro_df_12.observed / pro_df_12.execount

sg_df_or = summary.groupby('SG_ID')[['execount', 'observed']].sum().sort()
sg_df_12 = pd.merge(
    sg_df_or,
    pd.merge(
        summary.query('array == "TWELVE-M"')[['SG_ID', 'SB_UID']],
        aqua_times, on='SB_UID').groupby('SG_ID').sum(),
    left_index=True, right_index=True, how='outer').fillna(0)
sg_df_all = pd.merge(
    sg_df_or,
    pd.merge(
        summary[['SG_ID', 'SB_UID']],
        aqua_times, on='SB_UID').groupby('SG_ID').sum(),
    left_index=True, right_index=True, how='outer').fillna(0)
sg_df_all['sg_comp_per'] = sg_df_all.observed / sg_df_all.execount
sg_df_12['sg_comp_per_12m'] = sg_df_12.observed / sg_df_12.execount

summary = pd.merge(
    summary, pro_df_all[['proj_comp_per']], left_on='CODE', right_index=True)
summary = pd.merge(
    summary, pro_df_12[['proj_comp_per_12m']], left_on='CODE', right_index=True)
summary = pd.merge(
    summary, sg_df_all[['sg_comp_per']], left_on='SG_ID', right_index=True)
summary = pd.merge(
    summary, sg_df_12[['sg_comp_per_12m']], left_on='SG_ID', right_index=True)

check_estimates = pd.merge(
    aqua.query('totExecTime < 2.1'),
    summary[['CODE', 'SB_UID', 'SB_ETC2_exec']], on='SB_UID', how='left')

check = check_estimates.groupby('SB_UID').agg(
    {'totExecTime': pd.np.mean, 'SB_ETC2_exec': pd.np.max}
).reset_index()

summary = pd.merge(
    summary,
    check[['SB_UID', 'totExecTime']], on='SB_UID', how='left')

summary['totExecTime'] = summary.totExecTime.fillna(0)
summary['SB_ETC2org_exec'] = summary.SB_ETC2_exec
summary['SB_ETC2_exec'] = summary.apply(
    lambda r: r['totExecTime'] if r['totExecTime'] > 0 else r['SB_ETC2_exec'],
    axis=1)
summary['SB_ETC2_total'] = summary.SB_ETC2_exec * summary.execount
summary['SB_ETC2_remain'] = summary.SB_ETC2_exec * (
    summary.execount - summary.observed
)
summary['SBremExec'] = summary.execount - summary.observed
summary['SBtimeNeedComp'] = summary.SB_ETC2_exec * summary.SBremExec

remaining_fullyobs_exe_codes = summary.query(
    'observed < execount and SB_STATE == "FullyObserved"'
).SB_UID.unique().tolist()

print(summary.query('SB_UID in @remaining_fullyobs_exe_codes')[[
    'CODE', 'SB_UID', 'sbName', 'execount', 'observed', 'SB_STATE']])

summary['execount'] = summary.apply(
    lambda r:
    r['observed'] if r['SB_STATE'] == "FullyObserved" else r['execount'],
    axis=1)

c1toc2 = datas.toc2.CODE.unique()
c2 = datas.summary_sb[
    datas.summary_sb.CODE.str.startswith('2013.')].CODE.unique()
codes = np.concatenate([c1toc2, c2])

remaining_all = summary.query(
    'array == "TWELVE-M" and phase == "II" and '
    'SB_STATE not in ["FullyObserved", "Canceled"] and '
    'PRJ_LETTER_GRADE in ["A", "B", "C"] and CODE in @codes')
remaining_all = pd.merge(
    remaining_all,
    datas.projects[['OBSPROJECT_UID', 'PI', 'EXEC']], on='OBSPROJECT_UID')
remaining_all = pd.merge(
    remaining_all,
    users, left_on='PI', right_on='ACCOUNT_ID')
remaining_all = pd.merge(
    remaining_all,
    datas.sciencegoals[['SG_ID', 'two_12m']], on='SG_ID', how='left')

p1 = pd.merge(datas.scienceparam, datas.target, on=['paramRef', 'SB_UID'])
p2 = pd.merge(p1, datas.fieldsource, on=['fieldRef', 'SB_UID'])
p3 = p2[
    ['fieldRef', 'SB_UID', 'solarSystem', 'name', 'intendedUse', 'arraySB']
].query(
    'arraySB == "TWELVE-M" and solarSystem != "Unspecified"').SB_UID.values
remaining_ephem_codes = remaining_all.query('SB_UID in @p3'
                                            ).SB_UID.unique().tolist()
remaining_problem_codes = remaining_all.query('observed >= execount'
                                              ).SB_UID.unique().tolist()

print(remaining_all.query('SB_UID in @remaining_ephem_codes')[[
    'CODE', 'SB_UID', 'sbName', 'execount', 'observed', 'bestconf',
    'SB_STATE']])
print(remaining_all.query('SB_UID in @remaining_problem_codes')[[
    'CODE', 'SB_UID', 'sbName', 'execount', 'observed', 'SB_STATE']])

outcod = []

outcod.extend(remaining_problem_codes)
remaining = remaining_all.query('SB_UID not in @outcod').copy()
sbinrem = remaining.SB_UID.unique()

sbnota = remaining_all.SB_UID.unique()
aqua_execblock.to_csv(
    '/users/aod/data/aquaexe.csv', index=False, encoding='utf-8')
i1 = remaining.query(
    'C34_1 == 0 and C34_2 == 0 and C34_3 == 0 and C34_4 == 0 and '
    'C34_5 == 0 and C34_6 == 0 and C34_7 == 0 and '
    'array == "TWELVE-M"').index.values[0]
remaining.ix[i1, 'C34_2'] = 1
i1 = summary.query(
    'C34_1 == 0 and C34_2 == 0 and C34_3 == 0 and C34_4 == 0 and '
    'C34_5 == 0 and C34_6 == 0 and C34_7 == 0 and '
    'array == "TWELVE-M"').index.values[0]
summary.ix[i1, 'C34_2'] = 1

alma1.horizon = ephem.degrees('20')
obs_param = remaining.apply(
    lambda r: ct.observable(r['RA'], r['DEC'], alma1, r['SB_UID']), axis=1)
remaining = pd.merge(remaining, obs_param, on='SB_UID').sort('CODE')

alma1.horizon = ephem.degrees('20')
obs_param = summary.apply(
    lambda r: ct.observable(r['RA'], r['DEC'], alma1, r['SB_UID']), axis=1)
summary = pd.merge(summary, obs_param, on='SB_UID').sort('CODE')

availability = remaining.apply(
    lambda r: ct.avail_calc(
        r['rise'], r['set'], r['C34_1'], r['C34_2'], r['C34_3'], r['C34_4'],
        r['C34_5'], r['C34_6'], r['C34_7'], r['up'], r['band'], date_df),
    axis=1)
remaining = pd.concat([remaining, availability], axis=1)

availability = summary.apply(
    lambda r: ct.avail_calc(
        r['rise'], r['set'], r['C34_1'], r['C34_2'], r['C34_3'], r['C34_4'],
        r['C34_5'], r['C34_6'], r['C34_7'], r['up'], r['band'], date_df),
    axis=1)
summary = pd.concat([summary, availability], axis=1)

remaining['stale'] = remaining.apply(
    lambda r: 'SB Stale' if r['SB_UID'] in stalesep_sbs else 'SB Ready', axis=1)
remaining['proj_started'] = remaining.apply(
    lambda r: 'Prj. Started' if r['proj_comp_per'] > 0 else 'Prj. Not Started',
    axis=1)
unlikely = remaining.query('available_hours == 0').copy()
critical = remaining.query('24 > available_hours > 0').copy()
ok = remaining.query('available_hours >= 24').copy()

unlikely['Problem'] = 'Null SBEL'
critical['Problem'] = 'Low SBEL'
ok['Problem'] = 'High SBEL'

codep2 = unlikely.SB_UID.values
codep1 = critical.SB_UID.values
codep0 = ok.SB_UID.values

remaining['Problem'] = 'High SBEL'
remaining['Problem'] = remaining.apply(
    lambda r: 'Null SBEL' if r['SB_UID'] in codep2 else r['Problem'], axis=1)
remaining['Problem'] = remaining.apply(
    lambda r: 'Low SBEL' if r['SB_UID'] in codep1 else r['Problem'], axis=1)

proj_unlikely = remaining.query('Problem == "Null SBEL"').CODE.unique()
proj_critical = remaining.query(
    'Problem == "Low SBEL" and CODE not in @proj_unlikely').CODE.unique()
proj_ok = remaining.query(
    'Problem == "High SBEL" and CODE not in @proj_unlikely and '
    'CODE not in @proj_critical').CODE.unique()

remaining['Prj. Problem'] = 'High PCL'
remaining['Prj. Problem'] = remaining.apply(
    lambda r: 'Null PCL' if r['CODE'] in proj_unlikely else r['Prj. Problem'],
    axis=1)
remaining['Prj. Problem'] = remaining.apply(
    lambda r: 'Low PCL' if r['CODE'] in proj_critical else r['Prj. Problem'],
    axis=1)

remaining['Cycle'] = remaining.apply(
    lambda r: 'Cycle 2' if r['isCycle2'] is True else 'Cycle 1', axis=1)

compact_high = remaining.query(
    'C34_3 == 0 and C34_4 == 0 and C34_5 == 0 and C34_6 == 0 and '
    'C34_7 == 0 and Problem == "Null SBEL" and '
    'band in ["ALMA_RB_08", "ALMA_RB_09"]'
).SB_UID.values

high_day = remaining.query(
    'Problem == "Null SBEL" and '
    'band in ["ALMA_RB_07", "ALMA_RB_08", "ALMA_RB_09"] and '
    'SB_UID not in @compact_high and '
    '(C34_5 == 1 or C34_6 == 1 or C34_7 == 1)'
).SB_UID.values

not_longer = remaining.query(
    'Problem == "Null SBEL" and SB_UID not in @compact_high and '
    'SB_UID not in @high_day').SB_UID.values

remaining['Null SBEL Description'] = ''
remaining['Null SBEL Description'] = remaining.apply(
    lambda r: 'C34-1/2 High Frequency' if r['SB_UID'] in compact_high else
    r['Null SBEL Description'],
    axis=1)

remaining['Null SBEL Description'] = remaining.apply(
    lambda r: 'Daytime High Frequency' if r['SB_UID'] in high_day else
    r['Null SBEL Description'],
    axis=1)

remaining['Null SBEL Description'] = remaining.apply(
    lambda r: 'No longer observable (compact conf.)' if
    r['SB_UID'] in not_longer else r['Null SBEL Description'],
    axis=1)

remaining.set_index('SB_UID', drop=False, inplace=True)
summary.RA /= 15.
summary['RepLST'] = summary.apply(
    lambda r: round(r['RA']) if round(r['RA']) != 24 else 0, axis=1)
remaining.RA /= 15.
remaining['RepLST'] = remaining.apply(
    lambda r: round(r['RA']) if round(r['RA']) != 24 else 0, axis=1)

sel_col = [
    'CODE', 'PI', 'LASTNAME', 'FIRSTNAME', 'EMAIL', 'PRJ_LETTER_GRADE', 'EXEC',
    'sbName', 'band', 'repfreq', 'RepLST', 'execount', 'observed',
    'SB_ETC2_exec', 'SB_ETC2_total', 'SB_ETC2_remain', 'Problem',
    'Null SBEL Description', 'stale', 'two_12m', 'bestconf', 'C34_1', 'C34_2',
    'C34_3', 'C34_4', 'C34_5', 'C34_6', 'C34_7', 'minArrayAR100GHz',
    'maxArrayAR100GHz', 'proj_comp_per', 'SG_ID']

sel_col_names = pd.Index(
    [u'Project Code', u'PI', u'PI Last Name', u'PI First Name', u'PI email',
     u'Grade', u'Executive', u'SB Name', u'Band', u'Rep. Frequency',
     u'Representative LST', u'Executions requested', u'Executions done',
     u'Obs. Time per SB Execution', u'Obs. Time for SB Completion',
     u'Obs. Time remaining for SB Completion', u'SB Execution Likelihood',
     u'Null SBEL Reason', u'SB Stale Status', u'SB Needs Two 12m Conf.',
     u'Best Configuration', u'C34-1', u'C34-2', u'C34-3', u'C34-4', u'C34-5',
     u'C34-6', u'C34-7', u'Min. Array AR (100GHz)', u'Max. Array AR (100GHz)',
     u'Proj. Completion Fract.', u'SG ID'], dtype='object')

# C34_1 and C34_2 not longer on C34_3
conf12_all_notobs = remaining.query(
    'C34_1 == 1 or C34_2 ==1').query('C34_3 == 0')
conf12_AB_notobs = remaining.query(
    'C34_1 == 1 or C34_2 ==1').query('C34_3 == 0 and PRJ_LETTER_GRADE != "C"')
c12 = conf12_AB_notobs.SB_UID.unique()
c12sg = summary.query('SB_UID in @c12').SG_ID.unique()
conf12_C_notobs = remaining.query(
    'C34_1 == 1 or C34_2 ==1').query('C34_3 == 0 and PRJ_LETTER_GRADE == "C"')
# C34_3 and 4
conf34_all_notobs = remaining.query(
    'C34_3 == 1 or C34_4 ==1').query('C34_5 == 0')
conf34_AB_notobs = remaining.query(
    'C34_3 == 1 or C34_4 ==1').query('C34_5 == 0 and PRJ_LETTER_GRADE != "C"')
c34 = conf34_AB_notobs.SB_UID.unique()
c34sg = summary.query('SB_UID in @c34').SG_ID.unique()
conf34_C_notobs = remaining.query(
    'C34_3 == 1 or C34_4 ==1').query('C34_5 == 0 and PRJ_LETTER_GRADE == "C"')

# C34_5 not longer on C34_6
conf5_all_notobs = remaining.query('C34_5 == 1').query('C34_6 == 0')
conf5_AB_notobs = remaining.query('C34_5 == 1').query(
    'C34_6 == 0 and PRJ_LETTER_GRADE != "C"')
c5 = conf5_AB_notobs.SB_UID.unique()
c5sg = summary.query('SB_UID in @c5').SG_ID.unique()
conf5_C_notobs = remaining.query('C34_5 == 1').query(
    'C34_6 == 0 and PRJ_LETTER_GRADE == "C"')
remaining_clean = remaining.query(
    'PRJ_LETTER_GRADE != "C" and SB_UID not in @c12 and SB_UID not in @c34 and '
    'SB_UID not in @c5 and SG_ID not in @c12sg and SG_ID not in @c34sg and '
    'SG_ID not in @c5sg')

not_obs_sg = remaining.query(
    'PRJ_LETTER_GRADE != "C" and SB_UID not in @c12 and SB_UID not in @c34 and '
    'SB_UID not in @c5 and '
    '(SG_ID in @c12sg or SG_ID in @c34sg or SG_ID in @c5sg)')
not_obs_sg_sb = not_obs_sg.SB_UID.unique()

not_obs_sg = not_obs_sg[sel_col]
not_obs_sg.columns = sel_col_names

sg_notobs = list()
sb_notobs = list()
sg_notobs.extend(c12sg.tolist())
sg_notobs.extend(c34sg.tolist())
sg_notobs.extend(c5sg)
sb_notobs.extend(c12)
sb_notobs.extend(c34)
sb_notobs.extend(c5)
sb_notobs.extend(not_obs_sg_sb)

res3, res4 = ct.runsim(
    date_df,
    remaining_clean.query('PRJ_LETTER_GRADE != "C"'), ct.alma1)
d = pd.DataFrame(
    np.array(res4),
    columns=['time', 'lst', 'day', 'bands', 'array', 'SB_UID', 'SBremExec',
             'RA', 'Grade', 'dur'])

d.to_csv('/users/aod/data/sim.csv', index=False, encoding='utf-8')
date_df.to_csv('/users/aod/data/dates.csv', index=False, encoding='utf-8')

simulres = d.groupby('SB_UID').agg(
    {'Grade': pd.np.count_nonzero, 'dur': sum}).reset_index()
simulres.columns = pd.Index(
    [u'SB_UID', u'SimExecutions', u'SimObserved'], dtype='object')

temp_c1 = aqua_execblock.query(
    'QA0STATUS == "Pass" and STARTTIME < "2014-04-28"'
).groupby('SB_UID').agg(
    {'QA0STATUS': pd.np.count_nonzero, 'delta': pd.np.sum}).reset_index()
temp_c2 = aqua_execblock.query(
    'QA0STATUS in ["Pass", "Unset"] and STARTTIME >= "2014-04-28"'
).groupby('SB_UID').agg(
    {'QA0STATUS': pd.np.count_nonzero, 'delta': pd.np.sum}).reset_index()
temp_c1.columns = pd.Index([u'SB_UID', u'obs_c1', u'time_c1'], dtype='object')
temp_c2.columns = pd.Index([u'SB_UID', u'obs_c2', u'time_c2'], dtype='object')

temp_c1.time_c1.fillna(0, inplace=True)
temp_c2.time_c2.fillna(0, inplace=True)
temp_c1['time_c1'] = temp_c1.apply(
    lambda x: x['time_c1'].total_seconds() / 3600., axis=1)
temp_c2['time_c2'] = temp_c2.apply(
    lambda x: x['time_c2'].total_seconds() / 3600., axis=1)

tc2 = aqua_execblock.query(
    'STARTTIME >= "2014-04-28"'
).groupby('SB_UID').agg(
    {'QA0STATUS': pd.np.count_nonzero, 'delta': pd.np.sum}).reset_index()
tc2.columns = pd.Index([u'SB_UID', u'obs_c2', u'time_c2'], dtype='object')
tc2.time_c2.fillna(0, inplace=True)
tc2['alltime_c2'] = tc2.apply(
    lambda x: x['time_c2'].total_seconds() / 3600., axis=1)

tc2p = aqua_execblock.query(
    'STARTTIME >= "2014-04-28"'
).groupby('SB_UID').agg(
    {'QA0STATUS': pd.np.count_nonzero, 'delta': pd.np.sum}).reset_index()
tc2p.columns = pd.Index([u'SB_UID', u'obs_c2', u'time_c2'], dtype='object')
tc2p.time_c2.fillna(0, inplace=True)
tc2p['passtime_c2'] = tc2p.apply(
    lambda x: x['time_c2'].total_seconds() / 3600., axis=1)

summary2 = pd.merge(summary, temp_c1, on='SB_UID', how='left').copy()
summary2 = pd.merge(summary2, temp_c2, on='SB_UID', how='left').copy()
summary2 = pd.merge(
    summary2,
    tc2[['SB_UID', 'alltime_c2']], on='SB_UID', how='left').copy()
summary2 = pd.merge(
    summary2,
    tc2p[['SB_UID', 'passtime_c2']], on='SB_UID', how='left').copy()
summary2['unfinishable_sg'] = summary2.apply(
    lambda x: True if x['SG_ID'] in sg_notobs else False, axis=1)
summary2['unfinishable'] = summary2.apply(
    lambda x: True if x['SB_UID'] in sb_notobs else False, axis=1)

summary2 = pd.merge(
    summary2,
    datas.projects[['OBSPROJECT_UID', 'PI', 'EXEC']], on='OBSPROJECT_UID')
summary2.obs_c1.fillna(0, inplace=True)
summary2.obs_c2.fillna(0, inplace=True)
summary2.time_c1.fillna(0, inplace=True)
summary2.time_c2.fillna(0, inplace=True)

summary2 = pd.merge(summary2, simulres, on='SB_UID', how='left')
summary2.SimExecutions.fillna(0, inplace=True)
summary2.SimObserved.fillna(0, inplace=True)
summary2 = pd.merge(
    summary2, remaining[['SB_UID', 'Problem', 'Null SBEL Description']],
    on='SB_UID', how='left')
summary2['selected_code'] = summary2.apply(
    lambda x: True if x['CODE'] in codes else False, axis=1)
summary2.to_csv(
    '/users/aod/data/summary_table.csv', index=False, encoding='utf-8')
