from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path

import streamlit as st

from path_analyze.csv_io import load_track_points
from path_analyze.models import DEFAULT_TZ, Visit
from path_analyze.timeutils import tzinfo_from_name
from path_analyze.visits import (
    GeofenceCircle,
    VisitParams,
    find_visits,
    iter_visits_from_csv,
    write_visits_csv,
)


def _hhmmss(seconds: float) -> str:
    s = int(round(max(0.0, seconds)))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


def _range_to_epoch_ms(start_d: date, end_d: date, tz_name: str) -> tuple[int, int]:
    """Convert date range to epoch-ms [start, end) in tz."""

    tz = tzinfo_from_name(tz_name)
    start_dt = datetime.combine(start_d, time.min).replace(tzinfo=tz)
    end_dt = datetime.combine(end_d + timedelta(days=1), time.min).replace(tzinfo=tz)
    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)


def _overlap_seconds(visit: Visit, start_ms: int, end_ms_exclusive: int) -> float:
    lo = max(visit.start_ms, start_ms)
    hi = min(visit.end_ms, end_ms_exclusive)
    return max(0.0, (hi - lo) / 1000.0)


def _day_ranges(start_d: date, end_d: date, tz_name: str) -> list[tuple[date, int, int]]:
    """Return list of (day, start_ms, end_ms) for each day in range in tz."""

    tz = tzinfo_from_name(tz_name)
    days: list[tuple[date, int, int]] = []
    cur = start_d
    while cur <= end_d:
        sdt = datetime.combine(cur, time.min).replace(tzinfo=tz)
        edt = datetime.combine(cur + timedelta(days=1), time.min).replace(tzinfo=tz)
        days.append((cur, int(sdt.timestamp() * 1000), int(edt.timestamp() * 1000)))
        cur = cur + timedelta(days=1)
    return days


@st.cache_data(show_spinner=False)
def _load_visits(visits_csv: str, tz_name: str, mtime: float) -> list[Visit]:
    _ = mtime  # part of cache key so updated files reload automatically
    return list(iter_visits_from_csv(visits_csv, tz_name))


def main() -> None:
    st.set_page_config(page_title="灵感足迹：时间范围停留统计", layout="wide")
    st.title("灵感足迹：按时间范围统计实验室停留时长")

    with st.sidebar:
        st.subheader("数据与时区")
        tz_name = st.text_input("时区（IANA）", value=DEFAULT_TZ)
        path_csv = st.text_input("Path.csv 路径", value="Path.csv")
        visits_csv = st.text_input("visits.csv 输出/读取路径", value="visits.csv")

        st.subheader("实验室围栏（点击按钮一键生成 visits.csv）")
        center_lat = st.number_input("中心纬度 center_lat", value=30.7456421, format="%.7f")
        center_lon = st.number_input("中心经度 center_lon", value=103.9284974, format="%.7f")
        radius_m = st.number_input("半径 radius_m（米）", value=80.0, step=5.0)

        with st.expander("高级参数（通常不用改）", expanded=False):
            max_gap_seconds = st.number_input("max_gap_seconds（默认 12h）", value=12 * 60 * 60.0, step=600.0)
            exit_grace_seconds = st.number_input("exit_grace_seconds（默认 5min）", value=5 * 60.0, step=60.0)
            transition_gap_seconds = st.number_input(
                "transition_gap_seconds（默认 10min）", value=10 * 60.0, step=60.0
            )
            min_dwell_seconds = st.number_input("min_dwell_seconds（默认 60s）", value=60.0, step=10.0)

        if st.button("一键重新生成 visits.csv", type="primary", use_container_width=True):
            pcsv = Path(path_csv)
            if not pcsv.exists():
                st.error(f"找不到文件：{path_csv!r}")
            else:
                with st.spinner("正在读取 Path.csv 并重新计算 visits.csv ..."):
                    points, _ = load_track_points(path_csv)
                    geofence = GeofenceCircle(
                        center_lat=float(center_lat),
                        center_lon=float(center_lon),
                        radius_m=float(radius_m),
                    )
                    params = VisitParams(
                        tz_name=tz_name,
                        max_gap_seconds=float(max_gap_seconds),
                        exit_grace_seconds=float(exit_grace_seconds),
                        transition_gap_seconds=float(transition_gap_seconds),
                        min_dwell_seconds=float(min_dwell_seconds),
                    )
                    visits_gen = find_visits(points, geofence, params)
                    write_visits_csv(visits_gen, visits_csv)
                st.success(f"已生成：{visits_csv}（段数={len(visits_gen)}）")

        st.subheader("时间范围")
        today = datetime.now(tzinfo_from_name(tz_name)).date()
        default_start = today.replace(day=1)
        start_d = st.date_input("开始日期", value=default_start)
        end_d = st.date_input("结束日期", value=today)

    p = Path(visits_csv)
    if not p.exists():
        st.error(
            f"找不到文件：{visits_csv!r}。你可以点击左侧“一键重新生成 visits.csv”，或填写正确路径。"
        )
        return

    if start_d > end_d:
        st.error("开始日期不能晚于结束日期。")
        return

    try:
        visits = _load_visits(visits_csv, tz_name, p.stat().st_mtime)
    except Exception as exc:
        st.exception(exc)
        return

    start_ms, end_ms = _range_to_epoch_ms(start_d, end_d, tz_name)
    days = _day_ranges(start_d, end_d, tz_name)
    days_count = max(1, len(days))

    total_s = 0.0
    hit = 0
    rows: list[dict[str, object]] = []
    day_seconds: dict[date, float] = {d: 0.0 for d, _, _ in days}
    for v in visits:
        s = _overlap_seconds(v, start_ms, end_ms)
        if s <= 0:
            continue
        hit += 1
        total_s += s
        for d, d_start, d_end in days:
            ds = _overlap_seconds(v, d_start, d_end)
            if ds > 0:
                day_seconds[d] += ds
        rows.append(
            {
                "visit_id": v.visit_id,
                "start_time": v.start_dt.isoformat(sep=" "),
                "end_time": v.end_dt.isoformat(sep=" "),
                "overlap_hhmmss": _hhmmss(s),
                "overlap_seconds": round(s, 3),
                "points": v.points,
                "method": v.method,
            }
        )

    rows.sort(key=lambda r: float(r["overlap_seconds"]), reverse=True)

    st.subheader("汇总")
    c1, c2, c3 = st.columns(3)
    c1.metric("范围内总时长", _hhmmss(total_s))
    c2.metric("命中的停留段数", str(hit))
    c3.metric("visits.csv 总段数", str(len(visits)))

    st.subheader("平均每天停留时长")
    avg_s = total_s / float(days_count)
    st.metric("平均每天（按所选日期范围天数）", _hhmmss(avg_s))
    with st.expander("按天明细", expanded=False):
        day_rows = [
            {"date": d.isoformat(), "hhmmss": _hhmmss(sec), "seconds": round(sec, 3)}
            for d, sec in sorted(day_seconds.items(), key=lambda kv: kv[0])
        ]
        st.dataframe(day_rows, use_container_width=True, height=360)

    st.subheader("明细（按 overlap 时长排序）")
    st.dataframe(rows, use_container_width=True, height=520)

    st.caption(
        "说明：该界面读取 visits.csv 并做区间重叠裁剪统计；日期范围按本地时区计算，区间为 [开始日 00:00, 结束日+1 00:00)。"
    )


if __name__ == "__main__":
    main()


