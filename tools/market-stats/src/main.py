"""
market-stats — статистический анализ исторических данных из БД.

Команды:
    python src/main.py prices    — статистика цен ETH
    python src/main.py fear      — анализ Fear & Greed индекса
    python src/main.py combined  — корреляция цены и настроения рынка
    python src/main.py all       — всё (по умолчанию)
"""
import os, sys, math
from datetime import date
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DB_HOST  = os.environ["DB_HOST"]
DB_PORT  = os.environ.get("DB_PORT", "5432")
DB_NAME  = os.environ["DB_NAME"]
DB_USER  = os.environ["DB_USER"]
DB_PASS  = os.environ["DB_PASSWORD"]
COIN     = os.environ.get("COIN", "eth")


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )


# ─── Утилиты ─────────────────────────────────────────────────────────────────

def mean(values):
    return sum(values) / len(values) if values else 0

def stdev(values):
    if len(values) < 2:
        return 0
    m = mean(values)
    return math.sqrt(sum((x - m) ** 2 for x in values) / len(values))

def median(values):
    s = sorted(values)
    n = len(s)
    return (s[n//2] + s[n//2-1]) / 2 if n % 2 == 0 else s[n//2]

def sep(char="─", width=60):
    print(char * width)

def header(title):
    print()
    sep("═")
    print(f"  {title}")
    sep("═")


# ─── Анализ цен ──────────────────────────────────────────────────────────────

def report_prices(conn):
    header(f"ЦЕНЫ {COIN.upper()} — исторические данные")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT date, open_price, high_price, low_price, close_price,
                   avg_price, volume,
                   EXTRACT(YEAR FROM date)::int AS year
            FROM collectors_history_coinmarketcap_daily_candles
            WHERE coin = %s
            ORDER BY date
        """, (COIN,))
        rows = [dict(r) for r in cur.fetchall()]

    if not rows:
        print(f"  Нет данных для {COIN}")
        return

    closes = [r["close_price"] for r in rows]
    highs  = [r["high_price"]  for r in rows]
    lows   = [r["low_price"]   for r in rows]
    amps   = [(r["high_price"] - r["low_price"]) / r["low_price"] * 100 for r in rows]

    print(f"\n  Всего дней: {len(rows)}")
    print(f"  Период:    {rows[0]['date']} → {rows[-1]['date']}")
    print(f"\n  Цена закрытия:")
    print(f"    min:    ${min(closes):>12,.2f}")
    print(f"    max:    ${max(closes):>12,.2f}")
    print(f"    mean:   ${mean(closes):>12,.2f}")
    print(f"    median: ${median(closes):>12,.2f}")
    print(f"    σ:      ${stdev(closes):>12,.2f}")

    print(f"\n  Амплитуда дня (high-low)/low:")
    print(f"    mean:   {mean(amps):>8.2f}%")
    print(f"    median: {median(amps):>8.2f}%")
    print(f"    σ:      {stdev(amps):>8.2f}%")
    print(f"    min:    {min(amps):>8.2f}%")
    print(f"    max:    {max(amps):>8.2f}%")
    norm_lo = mean(amps) - stdev(amps)
    norm_hi = mean(amps) + stdev(amps)
    norm_cnt = sum(1 for a in amps if norm_lo <= a <= norm_hi)
    print(f"    норма [{norm_lo:.1f}% — {norm_hi:.1f}%]: {norm_cnt} дней ({norm_cnt/len(amps)*100:.1f}%)")

    # По годам
    from itertools import groupby
    print(f"\n  По годам:")
    print(f"  {'Год':<6} {'Дней':>5} {'Close AVG':>12} {'Амп AVG':>9} {'🐂%':>7} {'🐻%':>7} {'Рост x':>8}")
    sep()
    years = {}
    for r in rows:
        y = r["year"]
        years.setdefault(y, []).append(r)

    for year, yr_rows in sorted(years.items()):
        yr_closes = [r["close_price"] for r in yr_rows]
        yr_amps   = [(r["high_price"] - r["low_price"]) / r["low_price"] * 100 for r in yr_rows]
        bull = sum(1 for r in yr_rows if r["close_price"] >= r["open_price"])
        bear = len(yr_rows) - bull
        growth = yr_rows[-1]["close_price"] / yr_rows[0]["open_price"] if yr_rows[0]["open_price"] else 0
        print(f"  {year:<6} {len(yr_rows):>5} "
              f"${mean(yr_closes):>11,.0f} "
              f"{mean(yr_amps):>8.1f}% "
              f"{bull/len(yr_rows)*100:>6.1f}% "
              f"{bear/len(yr_rows)*100:>6.1f}% "
              f"{growth:>7.2f}x")

    # Топ движений
    daily_moves = []
    for r in rows:
        if r["open_price"] > 0:
            pct = (r["close_price"] - r["open_price"]) / r["open_price"] * 100
            daily_moves.append((pct, r["date"], r["open_price"], r["close_price"]))

    print(f"\n  Топ-5 роста за день:")
    for pct, d, o, c in sorted(daily_moves, reverse=True)[:5]:
        print(f"    {d}  {pct:>+7.1f}%   ${o:,.0f} → ${c:,.0f}")

    print(f"\n  Топ-5 падения за день:")
    for pct, d, o, c in sorted(daily_moves)[:5]:
        print(f"    {d}  {pct:>+7.1f}%   ${o:,.0f} → ${c:,.0f}")

    # Цепочки роста/падения
    streaks_bull, streaks_bear = [], []
    cur_streak, cur_type = 1, None
    for i in range(1, len(rows)):
        is_bull = rows[i]["close_price"] >= rows[i]["open_price"]
        if cur_type is None:
            cur_type = is_bull
        if is_bull == cur_type:
            cur_streak += 1
        else:
            (streaks_bull if cur_type else streaks_bear).append(cur_streak)
            cur_streak, cur_type = 1, is_bull
    (streaks_bull if cur_type else streaks_bear).append(cur_streak)

    print(f"\n  Цепочки роста/падения:")
    print(f"    Рост:    avg={mean(streaks_bull):.1f} дней  max={max(streaks_bull)} дней")
    print(f"    Падение: avg={mean(streaks_bear):.1f} дней  max={max(streaks_bear)} дней")


# ─── Анализ Fear & Greed ─────────────────────────────────────────────────────

def report_fear_greed(conn):
    header("FEAR & GREED INDEX")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT date, value, classification
            FROM collectors_fear_greed_index
            ORDER BY date
        """)
        rows = [dict(r) for r in cur.fetchall()]

    if not rows:
        print("  Нет данных Fear & Greed")
        return

    values = [r["value"] for r in rows]
    print(f"\n  Всего записей: {len(rows)}")
    print(f"  Период: {rows[0]['date']} → {rows[-1]['date']}")
    print(f"\n  Индекс (0=Extreme Fear, 100=Extreme Greed):")
    print(f"    mean:   {mean(values):>6.1f}")
    print(f"    median: {median(values):>6.1f}")
    print(f"    σ:      {stdev(values):>6.1f}")
    print(f"    min:    {min(values):>6}")
    print(f"    max:    {max(values):>6}")

    print(f"\n  Распределение по классификациям:")
    classes = {}
    for r in rows:
        classes.setdefault(r["classification"], []).append(r["value"])

    order = ["Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"]
    for cls in order:
        if cls in classes:
            vals = classes[cls]
            pct  = len(vals) / len(rows) * 100
            bar  = "█" * int(pct / 2)
            print(f"    {cls:<14} {len(vals):>4} дней ({pct:>5.1f}%)  {bar}")


# ─── Корреляция цены и настроения ────────────────────────────────────────────

def report_combined(conn):
    header(f"КОРРЕЛЯЦИЯ: {COIN.upper()} цена + Fear & Greed")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                p.date,
                p.close_price,
                p.open_price,
                f.value        AS fg_value,
                f.classification AS fg_class,
                LEAD(p.close_price) OVER (ORDER BY p.date) AS next_close
            FROM collectors_history_coinmarketcap_daily_candles p
            JOIN collectors_fear_greed_index f ON f.date = p.date
            WHERE p.coin = %s
            ORDER BY p.date
        """, (COIN,))
        rows = [dict(r) for r in cur.fetchall()]

    if not rows:
        print("  Нет совпадающих данных")
        return

    rows = [r for r in rows if r["next_close"] is not None]
    print(f"\n  Совпадающих дней: {len(rows)}")

    # Средняя цена и движение следующего дня по классификации
    print(f"\n  Средняя цена и движение следующего дня по уровню настроения:")
    print(f"  {'Уровень':<14} {'Дней':>5} {'Avg цена':>12} {'След.день avg':>14} {'🐂%':>7}")
    sep()

    order = ["Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"]
    for cls in order:
        group = [r for r in rows if r["fg_class"] == cls]
        if not group:
            continue
        prices    = [r["close_price"] for r in group]
        next_moves = [(r["next_close"] - r["close_price"]) / r["close_price"] * 100
                      for r in group]
        bull = sum(1 for m in next_moves if m > 0)
        print(f"  {cls:<14} {len(group):>5} "
              f"${mean(prices):>11,.0f} "
              f"{mean(next_moves):>+12.2f}% "
              f"{bull/len(group)*100:>6.1f}%")

    # Корреляция числового значения FG с движением цены
    fg_vals   = [r["fg_value"] for r in rows]
    next_pcts = [(r["next_close"] - r["close_price"]) / r["close_price"] * 100
                 for r in rows]

    n  = len(fg_vals)
    mx = mean(fg_vals)
    my = mean(next_pcts)
    cov = sum((fg_vals[i] - mx) * (next_pcts[i] - my) for i in range(n)) / n
    sx  = stdev(fg_vals)
    sy  = stdev(next_pcts)
    corr = cov / (sx * sy) if sx * sy != 0 else 0

    print(f"\n  Корреляция FG value → движение цены следующего дня:")
    print(f"    Pearson r = {corr:+.4f}")
    if abs(corr) < 0.1:
        print(f"    Интерпретация: очень слабая связь")
    elif abs(corr) < 0.3:
        print(f"    Интерпретация: слабая связь")
    elif abs(corr) < 0.5:
        print(f"    Интерпретация: умеренная связь")
    else:
        print(f"    Интерпретация: сильная связь")

    if corr < 0:
        print(f"    Направление: чем выше жадность → тем хуже следующий день (mean reversion)")
    else:
        print(f"    Направление: чем выше жадность → тем лучше следующий день (momentum)")


# ─── Точка входа ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cmd  = sys.argv[1] if len(sys.argv) > 1 else "all"
    conn = get_conn()

    try:
        if cmd in ("prices", "all"):
            report_prices(conn)
        if cmd in ("fear", "all"):
            report_fear_greed(conn)
        if cmd in ("combined", "all"):
            report_combined(conn)
    finally:
        conn.close()
