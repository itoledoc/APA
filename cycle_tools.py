__author__ = 'itoledo'

import pandas as pd
import datetime as dt
import ephem
import numpy as np

alma1 = ephem.Observer()
alma1.lat = '-23.0262015'
alma1.long = '-67.7551257'
alma1.elev = 5060
alma1.horizon = ephem.degrees(str('20'))


def create_dates(start, end, block, conf, data_arin=list()):

    data_ar = data_arin
    startd = dt.datetime.strptime(start, '%Y-%m-%d')
    endd = dt.datetime.strptime(end, '%Y-%m-%d')
    confs = [0, 0, 0, 0, 0, 0, 0]
    for c in conf:
        confs[c - 1] = 1
    while startd <= endd:
        if startd.isoweekday() not in [5, 6]:
            dr = [
                startd + dt.timedelta(hours=20),
                startd + dt.timedelta(hours=20 + 16),
                block]
            dr.extend(confs)
            data_ar.append(dr)
        else:
            dr = [
                startd + dt.timedelta(hours=20),
                startd + dt.timedelta(hours=20 + 24),
                block]
            dr.extend(confs)
            data_ar.append(dr)
        startd += dt.timedelta(1)

    return data_ar


def observable(ra1, dec1, alma, sbuid):

    datet = alma.date

    obj = ephem.FixedBody()
    obj._ra = pd.np.deg2rad(ra1)
    obj._dec = pd.np.deg2rad(dec1)
    obj.compute(alma)

    sets = alma.next_setting(obj)
    rise = alma.previous_rising(obj)
    alma.date = rise
    lstr = alma.sidereal_time()
    alma.date = sets
    lsts = alma.sidereal_time()
    alma.date = datet
    lstr = np.rad2deg(lstr) / 15.
    lsts = np.rad2deg(lsts) / 15. - 2.
    if lsts < 0:
        lsts += 24
    if lstr > lsts:
        up = 24. - lstr + lsts
    else:
        up = lsts - lstr
    return pd.Series([sbuid, lstr, lsts, up],
                     index=['SB_UID', 'rise', 'set', 'up'])


def day_night(sdate, edate, alma):

    datet = alma.date
    alma.horizon = ephem.degrees(str('-20'))
    obj = ephem.Sun()

    alma.date = sdate
    lst_start = alma.sidereal_time()
    sun_set = alma.next_setting(obj)
    alma.date = sun_set
    alma.horizon = ephem.degrees(str('0'))
    lst_dark = alma.sidereal_time()

    alma.horizon = ephem.degrees(str('-20'))
    alma.date = edate
    lst_end = alma.sidereal_time()
    sun_rise = alma.previous_rising(obj)
    alma.date = sun_rise
    alma.horizon = ephem.degrees(str('0'))
    lst_dawn = alma.sidereal_time()

    alma.date = datet

    lst_start = np.rad2deg(lst_start) / 15.
    lst_dark = np.rad2deg(lst_dark) / 15.
    lst_end = np.rad2deg(lst_end) / 15.
    lst_dawn = np.rad2deg(lst_dawn) / 15.

    return pd.Series([lst_start, lst_dark, lst_end, lst_dawn],
                     index=['lst_start', 'lst_dusk', 'lst_end', 'lst_dawn'])


def avail_calc(orise, oset, conf1, conf2, conf3, conf4, conf5, conf6, conf7, up,
               band, datedf):

    # First, is observable?
    confnames_df = ['C34_1', 'C34_2', 'C34_3', 'C34_4', 'C34_5', 'C34_6',
                    'C34_7']
    cf = np.array([conf1, conf2, conf3, conf4, conf5, conf6, conf7])
    hup = 0.
    safe = 0
    crit = 0
    wend = 0
    based = dt.datetime(2015, 1, 1)
    if orise < oset:
        b1 = based + dt.timedelta(hours=orise)
        b2 = based + dt.timedelta(hours=oset)
        b3 = based + dt.timedelta(days=1, hours=orise)
        b4 = based + dt.timedelta(days=1, hours=oset)
    else:
        b1 = based
        b2 = based + dt.timedelta(hours=oset)
        b3 = based + dt.timedelta(hours=orise)
        b4 = based + dt.timedelta(days=1, hours=oset)

    for di in datedf.index:
        l = datedf.ix[di]
        confs = l[confnames_df].values

        if 1 not in cf * confs:
            continue
        else:
            if ((l.start < dt.datetime(2015, 5, 1)) and
                    band in ['ALMA_RB_08', 'ALMA_RB_09']):
                continue

        if band in ['ALMA_RB_07', 'ALMA_RB_08', 'ALMA_RB_09']:
            dstart = l.lst_dusk
            dend = l.lst_dawn
            check_not24 = True
        else:
            dstart = l.lst_start
            dend = l.lst_end
            check_not24 = False
            if l.start.weekday() not in [4, 5]:
                dend = l.lst_end - 1.5
                if dend < 0:
                    dend += 24
                check_not24 = True

        if dstart < dend and check_not24:
            dstart = based + dt.timedelta(hours=dstart)
            dend = based + dt.timedelta(hours=dend)
        else:
            dstart = based + dt.timedelta(hours=dstart)
            dend = based + dt.timedelta(days=1, hours=dend)

        critt = 0
        wendt = 0
        safet = 0

        for r in [[b1, b2], [b3, b4]]:

            if (r[1] < dstart) or (r[0] > dend):
                hup += 0
            elif (r[0] < dstart) and (r[1] > dend):
                hup += (dend - dstart).total_seconds() / 3600.
                safet += 1
            elif (r[0] > dstart) and (r[1] < dend):
                hup += (r[1] - r[0]).total_seconds() / 3600.
                safet += 1
            else:
                tl = [r[1], r[0], dstart, dend]
                tl.sort()
                # print tl
                delta = ((tl[2] - tl[1]).total_seconds() / 3600.)
                if 0 < delta < 2.:
                    hup += delta
                    critt += 1
                else:
                    hup += delta
                    safe += 1

            if ((critt > 0) or (safet > 0)) and (l.start.weekday() in [4, 5]):
                wendt += 1
        if critt > 0:
            crit += 1
        if wendt > 0:
            wend += 1
        if safet > 0:
            safe += 1

    return pd.Series([hup, safe, crit, wend],
                     index=['available_hours', 'days', 'days_crit', 'weekend'])
