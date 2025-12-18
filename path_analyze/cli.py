"""Command-line interface for path_analyze.

Run:
    python -m path_analyze inspect --csv Path.csv
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import asdict
from pathlib import Path
from time import perf_counter
from time import sleep, time

from path_analyze.csv_io import load_track_points
from path_analyze.inspect import export_readable_csv, inspect_points
from path_analyze.models import DEFAULT_TZ
from path_analyze.timeutils import dt_from_epoch_ms
from path_analyze.visits import (
    GeofenceCircle,
    VisitParams,
    iter_visits_from_csv,
    find_visits,
    sum_visits,
    write_visits_csv,
)


def _cmd_inspect(args: argparse.Namespace) -> int:
    points, summary = load_track_points(args.csv)
    res = inspect_points(points)

    print("### CSV字段")
    print(", ".join(summary.fieldnames))
    print()

    print("### 行数")
    print(f"total_rows={summary.rows_total}, parsed={summary.rows_parsed}, skipped={summary.rows_skipped}")
    print()

    if res.min_time_ms is not None and res.max_time_ms is not None:
        print("### 时间范围（本地时区）")
        start = dt_from_epoch_ms(res.min_time_ms, args.tz)
        end = dt_from_epoch_ms(res.max_time_ms, args.tz)
        print(f"start={start.isoformat(sep=' ')}, end={end.isoformat(sep=' ')}")
        print()

    if res.delta is not None:
        print("### 采样间隔（秒）")
        print(
            f"count={res.delta.count}, min={res.delta.min_s:.3f}, median={res.delta.median_s:.3f}, "
            f"p95={res.delta.p95_s:.3f}, max={res.delta.max_s:.3f}"
        )
        print()

    print("### 经纬度范围（粗略）")
    print(f"lat=[{res.min_lat}, {res.max_lat}], lon=[{res.min_lon}, {res.max_lon}]")
    print()

    print("### 重复时间戳（geoTime重复）")
    print(res.duplicates_geo_time)
    print()

    if args.json:
        # 方便你之后做二次处理
        import json

        payload = asdict(res) | {
            "rows_total": summary.rows_total,
            "rows_parsed": summary.rows_parsed,
            "rows_skipped": summary.rows_skipped,
            "fieldnames": list(summary.fieldnames),
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def _cmd_export_readable(args: argparse.Namespace) -> int:
    points, _ = load_track_points(args.csv)
    place_by_key: dict[str, str] | None = None
    if args.geocode:
        from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, ThreadPoolExecutor, wait

        from path_analyze.geocode import (
            JsonDiskCache,
            NominatimConfig,
            NominatimReverseGeocoder,
            coord_key,
            nominatim_reverse_raw,
        )

        cache = JsonDiskCache(args.geocode_cache)
        # 立刻创建缓存文件（避免长时间运行看不到任何文件产出）
        cache.ensure_persistent_files()
        cfg = NominatimConfig(
            accept_language=args.geocode_lang,
            min_interval_seconds=args.geocode_min_interval,
            user_agent=args.geocode_user_agent,
            zoom=args.geocode_zoom,
            timeout_seconds=args.geocode_timeout_seconds,
        )
        geocoder = NominatimReverseGeocoder(cfg, cache=cache)
        # 先扫描本地缓存（增量：优先用缓存填充，缺的再去请求）
        place_by_key = cache.place_name_map()

        # 可选：用更粗的坐标精度做“去抖动复用”（减少请求次数）
        dedup_precision: int | None = args.geocode_dedup_precision
        if dedup_precision is not None and dedup_precision >= args.geocode_precision:
            dedup_precision = None

        dedup_place_by_key: dict[str, str] = {}
        if dedup_precision is not None:
            for k, name in place_by_key.items():
                if not name:
                    continue
                try:
                    lat_s, lon_s = k.split(",", 1)
                    lat_v = float(lat_s)
                    lon_v = float(lon_s)
                except ValueError:
                    continue
                dk = coord_key(lat_v, lon_v, dedup_precision)
                dedup_place_by_key.setdefault(dk, name)

        budget = {"used": 0}
        max_requests: int | None
        if args.geocode_max_requests is None:
            max_requests = 2000
        elif int(args.geocode_max_requests) < 0:
            max_requests = None
        else:
            max_requests = int(args.geocode_max_requests)

        step = max(1, int(args.geocode_every_n))
        # 先统计“需要新请求”的唯一坐标（按 rounded key 去重）
        pending: dict[str, tuple[float, float]] = {}
        for pt in points[::step]:
            key = coord_key(pt.latitude, pt.longitude, args.geocode_precision)
            if key in place_by_key or key in pending:
                continue
            if dedup_precision is not None:
                dk = coord_key(pt.latitude, pt.longitude, dedup_precision)
                reused = dedup_place_by_key.get(dk, "")
                if reused:
                    # 直接复用“同一粗粒度格子”的已有地名，不再请求
                    place_by_key[key] = reused
                    continue
            pending[key] = (pt.latitude, pt.longitude)

        cached_n = len(place_by_key)
        pending_n = len(pending)
        if max_requests is None:
            target_n = pending_n
        else:
            target_n = max(0, min(max_requests, pending_n))

        print(
            f"逆地理编码：缓存命中={cached_n}，待请求(唯一坐标)={pending_n}，本次将请求={target_n}，"
            f"min_interval={args.geocode_min_interval}s，precision={args.geocode_precision}，"
            f"dedup_precision={dedup_precision}，every_n={step}",
            file=sys.stderr,
            flush=True,
        )
        if args.geocode_workers > 1:
            print(
                f"并发模式：workers={args.geocode_workers}, executor={args.geocode_executor}。"
                "注意：公共服务可能限速/封禁；建议保持 min-interval=1s 或更大。",
                file=sys.stderr,
                flush=True,
            )

        # 再跑一遍做 unique geocode（按 rounded key），并输出进度
        if target_n > 0 and args.geocode_workers <= 1:
            started = perf_counter()
            last_print = started
            completed = 0
            ok = 0
            failed = 0
            try:
                for key, (lat, lon) in pending.items():
                    if max_requests is not None and completed >= max_requests:
                        break

                    res = geocoder.reverse(
                        lat=lat,
                        lon=lon,
                        precision=args.geocode_precision,
                        max_new_requests=max_requests,
                        _budget_state=budget,
                    )
                    completed += 1
                    if res is None:
                        failed += 1
                        place_by_key.setdefault(key, "")
                    else:
                        ok += 1
                        place_by_key[key] = res.place_name
                        if dedup_precision is not None and res.place_name:
                            dk = coord_key(lat, lon, dedup_precision)
                            dedup_place_by_key.setdefault(dk, res.place_name)

                    now = perf_counter()
                    # 每 1 秒或每 10 次“完成”刷新一次进度
                    if (completed % 10 == 0) or (now - last_print >= 1.0) or (completed == target_n):
                        elapsed = now - started
                        rate = elapsed / max(1, completed)
                        eta = rate * max(0, target_n - completed)
                        pct = 100.0 * completed / max(1, target_n)
                        msg = (
                            f"\r逆地理编码进度：{completed}/{target_n} ({pct:5.1f}%) "
                            f"ok={ok} fail={failed} elapsed={elapsed:6.1f}s eta={eta:6.1f}s"
                        )
                        print(msg, end="", file=sys.stderr, flush=True)
                        last_print = now
            except KeyboardInterrupt:
                # 优雅中断：保留已请求到的缓存与 place_by_key，继续导出 readable.csv
                print("\n收到中断信号：停止继续请求，开始导出当前结果……", file=sys.stderr, flush=True)
            print(file=sys.stderr)  # 换行
        elif target_n > 0:
            # 并发请求模式：按 min_interval 控制“提交频率”，允许多个请求同时在途（对高延迟网络提速明显）
            workers = int(args.geocode_workers)
            if workers < 1:
                workers = 1

            def _submitter(executor, lat: float, lon: float) -> Future[dict[str, object] | None]:
                return executor.submit(nominatim_reverse_raw, lat, lon, cfg)

            if args.geocode_executor == "process":
                executor_cm = ProcessPoolExecutor(max_workers=workers)
            else:
                executor_cm = ThreadPoolExecutor(max_workers=workers)

            started = perf_counter()
            last_print = started
            completed = 0
            ok = 0
            failed = 0
            submitted = 0
            futures: dict[Future[dict[str, object] | None], tuple[str, float, float]] = {}

            # Prepare iterable of tasks limited to target_n
            items = []
            for key, (lat, lon) in pending.items():
                items.append((key, lat, lon))
                if len(items) >= target_n:
                    break
            it_idx = 0

            next_submit_at = time()

            try:
                with executor_cm as executor:
                    while True:
                        # Submit new tasks respecting global min_interval spacing
                        while (
                            it_idx < len(items)
                            and len(futures) < workers
                            and time() >= next_submit_at
                        ):
                            key, lat, lon = items[it_idx]
                            it_idx += 1
                            fut = _submitter(executor, lat, lon)
                            futures[fut] = (key, lat, lon)
                            submitted += 1
                            next_submit_at = max(next_submit_at + float(args.geocode_min_interval), time())

                        if not futures:
                            # No in-flight and no remaining tasks
                            if it_idx >= len(items):
                                break
                            # Wait until we can submit the next task
                            sleep(max(0.0, next_submit_at - time()))
                            continue

                        done_set, _ = wait(futures.keys(), timeout=0.2, return_when=FIRST_COMPLETED)
                        for fut in done_set:
                            key, lat, lon = futures.pop(fut)
                            raw = None
                            try:
                                raw = fut.result()
                            except Exception:
                                raw = None

                            completed += 1
                            if raw is None:
                                failed += 1
                                place_by_key.setdefault(key, "")
                            else:
                                place = str(raw.get("display_name", "") or "")
                                ok += 1
                                place_by_key[key] = place
                                # 关键：主进程立即落盘（journal）保存结果
                                cache.set(key, {"place_name": place, **raw})
                                if dedup_precision is not None and place:
                                    dk = coord_key(lat, lon, dedup_precision)
                                    dedup_place_by_key.setdefault(dk, place)

                            now = perf_counter()
                            if (completed % 10 == 0) or (now - last_print >= 1.0) or (completed == target_n):
                                elapsed = now - started
                                rate = elapsed / max(1, completed)
                                eta = rate * max(0, target_n - completed)
                                pct = 100.0 * completed / max(1, target_n)
                                msg = (
                                    f"\r逆地理编码进度：{completed}/{target_n} ({pct:5.1f}%) "
                                    f"inflight={len(futures)} ok={ok} fail={failed} "
                                    f"elapsed={elapsed:6.1f}s eta={eta:6.1f}s"
                                )
                                print(msg, end="", file=sys.stderr, flush=True)
                                last_print = now
            except KeyboardInterrupt:
                print("\n收到中断信号：停止继续请求，开始导出当前结果……", file=sys.stderr, flush=True)
            print(file=sys.stderr)

        # 注意：reverse() 里也有 budget 逻辑，这里再提示一次上限情况（体验更直观）
        if max_requests == 0:
            print("逆地理编码：max-requests=0，本次不发起请求，仅使用本地缓存。", file=sys.stderr, flush=True)

        cache.flush()

        if max_requests is not None and budget["used"] >= int(max_requests):
            print(
                f"注意：逆地理编码已达到 max-requests={max_requests} 上限，"
                "剩余未请求到的坐标将输出为空。你可以增大上限或提高 geocode-precision/加大 geocode-every-n。"
            )

    export_readable_csv(
        points,
        args.out,
        args.tz,
        place_name_by_key=place_by_key,
        coord_precision=args.geocode_precision,
    )
    print(f"已导出：{args.out}")
    return 0


def _cmd_find_visits(args: argparse.Namespace) -> int:
    points, _ = load_track_points(args.csv)
    if args.range_start is not None or args.range_end is not None:
        from path_analyze.timeutils import parse_dt

        start_ms = parse_dt(args.range_start, args.tz).timestamp() * 1000 if args.range_start else None
        end_ms = parse_dt(args.range_end, args.tz).timestamp() * 1000 if args.range_end else None
        if start_ms is not None:
            points = [p for p in points if p.geo_time_ms >= int(start_ms)]
        if end_ms is not None:
            points = [p for p in points if p.geo_time_ms <= int(end_ms)]

    geofence = GeofenceCircle(center_lat=args.center_lat, center_lon=args.center_lon, radius_m=args.radius_m)
    params = VisitParams(
        tz_name=args.tz,
        max_gap_seconds=args.max_gap_seconds,
        exit_grace_seconds=args.exit_grace_seconds,
        transition_gap_seconds=args.transition_gap_seconds,
        min_dwell_seconds=args.min_dwell_seconds,
    )
    visits = find_visits(points, geofence, params)
    write_visits_csv(visits, args.out)
    total = sum_visits(visits)
    print(f"识别到 visits={total.visits} 段，合计={total.total_hhmmss}（{total.total_seconds:.1f}s）")
    print(f"已导出：{args.out}（你可以手工修改 start_time/end_time 后再 sum-visits）")
    return 0


def _cmd_sum_visits(args: argparse.Namespace) -> int:
    visits = list(iter_visits_from_csv(args.visits, args.tz))

    if args.range_start is not None or args.range_end is not None:
        from path_analyze.timeutils import parse_dt

        start_ms = parse_dt(args.range_start, args.tz).timestamp() * 1000 if args.range_start else None
        end_ms = parse_dt(args.range_end, args.tz).timestamp() * 1000 if args.range_end else None

        def clipped_seconds(v_start: int, v_end: int) -> float:
            lo = v_start if start_ms is None else max(v_start, int(start_ms))
            hi = v_end if end_ms is None else min(v_end, int(end_ms))
            return max(0.0, (hi - lo) / 1000.0)

        total_s = sum(clipped_seconds(v.start_ms, v.end_ms) for v in visits)
        from path_analyze.visits import VisitsTotal

        total = VisitsTotal(visits=len(visits), total_seconds=total_s)
    else:
        total = sum_visits(visits)

    print(f"visits={total.visits}, total={total.total_hhmmss}（{total.total_seconds:.1f}s）")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build CLI parser."""

    p = argparse.ArgumentParser(prog="path_analyze")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_ins = sub.add_parser("inspect", help="分析 Path.csv 的结构/时间范围/采样间隔等")
    p_ins.add_argument("--csv", type=str, default="Path.csv", help="输入CSV路径")
    p_ins.add_argument("--tz", type=str, default=DEFAULT_TZ, help="时区（IANA），默认 Asia/Shanghai")
    p_ins.add_argument("--json", action="store_true", help="额外输出JSON（便于后处理）")
    p_ins.set_defaults(func=_cmd_inspect)

    p_exp = sub.add_parser("export-readable", help="导出可读时间的轨迹点CSV")
    p_exp.add_argument("--csv", type=str, default="Path.csv", help="输入CSV路径")
    p_exp.add_argument("--out", type=str, default="readable.csv", help="输出CSV路径")
    p_exp.add_argument("--tz", type=str, default=DEFAULT_TZ, help="时区（IANA）")
    p_exp.add_argument("--geocode", action="store_true", help="启用逆地理编码（lat/lon -> 地名）")
    p_exp.add_argument("--geocode-cache", type=str, default="geocode_cache.json", help="逆地理编码缓存文件")
    p_exp.add_argument("--geocode-lang", type=str, default="zh-CN", help="逆地理编码语言（如 zh-CN/en）")
    p_exp.add_argument("--geocode-zoom", type=int, default=18, help="逆地理编码缩放等级（越大越细）")
    p_exp.add_argument(
        "--geocode-precision",
        type=int,
        default=4,
        help="缓存用坐标小数位数（4约~11m纬度分辨率；越小请求越少但更粗）",
    )
    p_exp.add_argument(
        "--geocode-min-interval",
        type=float,
        default=0.01,
        help="请求最小间隔（秒），公共服务建议>=1.0",
    )
    p_exp.add_argument(
        "--geocode-timeout-seconds",
        type=float,
        default=20.0,
        help="单次请求超时（秒）",
    )
    p_exp.add_argument(
        "--geocode-max-requests",
        type=int,
        default=-1,
        help="最多发起多少次新的逆地理编码请求（防止30k行跑很久）。设为 0 表示不请求；设为 -1 表示不限制。",
    )
    p_exp.add_argument(
        "--geocode-workers",
        type=int,
        default=1,
        help="并发 worker 数（>1 启用并发；主进程会按 min-interval 控制提交频率）",
    )
    p_exp.add_argument(
        "--geocode-executor",
        type=str,
        default="thread",
        choices=["thread", "process"],
        help="并发执行器：thread(默认, I/O更合适) / process(多进程)",
    )
    p_exp.add_argument(
        "--geocode-every-n",
        type=int,
        default=1,
        help="每隔N个点采样一次去请求地名（仍会按 rounded key 缓存复用）",
    )
    p_exp.add_argument(
        "--geocode-user-agent",
        type=str,
        default="path-analyze/0.1.0 (reverse-geocode; set your own UA)",
        help="HTTP User-Agent（建议填你自己的标识，避免被服务方屏蔽）",
    )
    p_exp.add_argument(
        "--geocode-dedup-precision",
        type=int,
        default=None,
        help="更粗粒度的去重精度（例如 3）。如果该精度下已有地名，则细粒度坐标直接复用，不再请求（抵抗GPS抖动）",
    )
    p_exp.set_defaults(func=_cmd_export_readable)

    p_fv = sub.add_parser("find-visits", help="用地理围栏识别“在实验室”的停留区间并导出 visits.csv")
    p_fv.add_argument("--csv", type=str, default="Path.csv", help="输入CSV路径")
    p_fv.add_argument("--center-lat", type=float, required=True, help="实验室中心点纬度")
    p_fv.add_argument("--center-lon", type=float, required=True, help="实验室中心点经度")
    p_fv.add_argument("--radius-m", type=float, required=True, help="围栏半径（米）")
    p_fv.add_argument("--tz", type=str, default=DEFAULT_TZ, help="时区（IANA）")
    p_fv.add_argument(
        "--max-gap-seconds",
        type=float,
        default=12 * 60 * 60.0,
        help="同一次停留中，连续“在围栏内”的两次采样若间隔超过该值，将强制切分（默认12小时，适配稀疏采样）",
    )
    p_fv.add_argument(
        "--exit-grace-seconds",
        type=float,
        default=5 * 60.0,
        help="离开围栏的确认时长：连续在围栏外超过该秒数才结束停留（防GPS抖动）",
    )
    p_fv.add_argument(
        "--transition-gap-seconds",
        type=float,
        default=10 * 60.0,
        help="进入/离开边界用“中点估计”的最大间隔（超过则不用中点，直接取一侧）",
    )
    p_fv.add_argument("--min-dwell-seconds", type=float, default=60.0, help="过滤小于该驻留时长的visit")
    p_fv.add_argument(
        "--range-start",
        type=str,
        default=None,
        help="仅统计该时间之后的数据（例如 2025-12-01 00:00:00）",
    )
    p_fv.add_argument(
        "--range-end",
        type=str,
        default=None,
        help="仅统计该时间之前的数据（例如 2025-12-31 23:59:59）",
    )
    p_fv.add_argument("--out", type=str, default="visits.csv", help="输出 visits.csv 路径")
    p_fv.set_defaults(func=_cmd_find_visits)

    p_sv = sub.add_parser("sum-visits", help="汇总 visits.csv（支持你手工改过开始/结束时间）")
    p_sv.add_argument("--visits", type=str, default="visits.csv", help="visits.csv 路径")
    p_sv.add_argument("--tz", type=str, default=DEFAULT_TZ, help="时区（IANA）")
    p_sv.add_argument(
        "--range-start",
        type=str,
        default=None,
        help="只统计该时间之后的重叠部分（例如 2025-12-01 00:00:00）",
    )
    p_sv.add_argument(
        "--range-end",
        type=str,
        default=None,
        help="只统计该时间之前的重叠部分（例如 2025-12-31 23:59:59）",
    )
    p_sv.set_defaults(func=_cmd_sum_visits)

    return p


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""

    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())


