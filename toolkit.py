"""
sql-analytics-toolkit
=====================
Colección de análisis SQL/Python sobre datos de e-commerce usando DuckDB.
Cada función es independiente y devuelve un DataFrame listo para visualizar.

Instalación:
    pip install duckdb pandas

Uso rápido:
    from toolkit import SQLAnalyticsToolkit
    tk = SQLAnalyticsToolkit("data/ecommerce.db")
    tk.seed_demo_data()
    print(tk.cohort_retention())
"""

import duckdb
import pandas as pd
from datetime import datetime, timedelta
import random


# ──────────────────────────────────────────────
# SETUP
# ──────────────────────────────────────────────

class SQLAnalyticsToolkit:
    """
    Toolkit de analytics sobre datos de e-commerce.
    Usa DuckDB como motor SQL en memoria o en archivo.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.con = duckdb.connect(db_path)
        self._tables_created = False

    # ──────────────────────────────────────────────
    # DATOS DE DEMO
    # ──────────────────────────────────────────────

    def seed_demo_data(self, n_users: int = 500, n_orders: int = 3000, seed: int = 42):
        """
        Genera datos sintéticos de e-commerce:
          - users: id, país, fecha de registro, plan
          - orders: id, user_id, fecha, monto, categoría, estado
          - events: id, user_id, evento, fecha (para funnel)
        """
        random.seed(seed)

        countries = ["AR", "MX", "CO", "CL", "BR", "PE"]
        plans = ["free", "basic", "pro"]
        categories = ["electrónica", "ropa", "hogar", "libros", "deportes", "belleza"]
        statuses = ["completed", "completed", "completed", "returned", "cancelled"]
        event_types = ["page_view", "add_to_cart", "checkout_start", "payment_info", "purchase"]

        base_date = datetime(2023, 1, 1)

        # --- users ---
        users = []
        for i in range(1, n_users + 1):
            reg_date = base_date + timedelta(days=random.randint(0, 364))
            users.append({
                "user_id": i,
                "country": random.choice(countries),
                "plan": random.choices(plans, weights=[60, 25, 15])[0],
                "registered_at": reg_date.strftime("%Y-%m-%d"),
                "age": random.randint(18, 65),
            })
        users_df = pd.DataFrame(users)

        # --- orders ---
        orders = []
        for i in range(1, n_orders + 1):
            uid = random.randint(1, n_users)
            reg_date = datetime.strptime(users[uid - 1]["registered_at"], "%Y-%m-%d")
            order_date = reg_date + timedelta(days=random.randint(0, 180))
            if order_date > datetime(2023, 12, 31):
                order_date = datetime(2023, 12, 31)
            orders.append({
                "order_id": i,
                "user_id": uid,
                "order_date": order_date.strftime("%Y-%m-%d"),
                "amount": round(random.uniform(5, 800), 2),
                "category": random.choice(categories),
                "status": random.choice(statuses),
            })
        orders_df = pd.DataFrame(orders)

        # --- events (funnel) ---
        events = []
        eid = 1
        for uid in range(1, n_users + 1):
            session_date = base_date + timedelta(days=random.randint(0, 300))
            # cada usuario llega hasta un paso del funnel (drop-off simulado)
            max_step = random.choices(range(len(event_types)), weights=[5, 25, 30, 20, 20])[0]
            for step in range(max_step + 1):
                events.append({
                    "event_id": eid,
                    "user_id": uid,
                    "event_type": event_types[step],
                    "event_date": (session_date + timedelta(minutes=step * 3)).strftime("%Y-%m-%d %H:%M:%S"),
                })
                eid += 1
        events_df = pd.DataFrame(events)

        # Registrar tablas en DuckDB
        self.con.register("users", users_df)
        self.con.register("orders", orders_df)
        self.con.register("events", events_df)
        self._tables_created = True
        print(f"✓ Datos generados: {len(users_df)} usuarios · {len(orders_df)} órdenes · {len(events_df)} eventos")

    def _check(self):
        if not self._tables_created:
            raise RuntimeError("Primero ejecutá seed_demo_data() para generar los datos de demo.")

    # ──────────────────────────────────────────────
    # 1. ANÁLISIS DE COHORTES
    # ──────────────────────────────────────────────

    def cohort_retention(self) -> pd.DataFrame:
        """
        Retención mensual por cohorte de registro.
        Muestra qué % de usuarios de cada cohorte volvió a comprar en meses posteriores.
        """
        self._check()
        return self.con.execute("""
            WITH cohorts AS (
                SELECT
                    user_id,
                    DATE_TRUNC('month', CAST(registered_at AS DATE)) AS cohort_month
                FROM users
            ),
            orders_month AS (
                SELECT
                    o.user_id,
                    DATE_TRUNC('month', CAST(o.order_date AS DATE)) AS order_month
                FROM orders o
                WHERE o.status = 'completed'
            ),
            joined AS (
                SELECT
                    c.cohort_month,
                    o.order_month,
                    DATEDIFF('month', c.cohort_month, o.order_month) AS month_number,
                    COUNT(DISTINCT o.user_id) AS active_users
                FROM cohorts c
                JOIN orders_month o ON c.user_id = o.user_id
                GROUP BY 1, 2, 3
            ),
            cohort_size AS (
                SELECT
                    DATE_TRUNC('month', CAST(registered_at AS DATE)) AS cohort_month,
                    COUNT(*) AS total_users
                FROM users
                GROUP BY 1
            )
            SELECT
                j.cohort_month,
                j.month_number,
                j.active_users,
                cs.total_users,
                ROUND(100.0 * j.active_users / cs.total_users, 1) AS retention_pct
            FROM joined j
            JOIN cohort_size cs ON j.cohort_month = cs.cohort_month
            WHERE j.month_number BETWEEN 0 AND 5
            ORDER BY j.cohort_month, j.month_number
        """).df()

    # ──────────────────────────────────────────────
    # 2. ANÁLISIS RFM
    # ──────────────────────────────────────────────

    def rfm_segmentation(self) -> pd.DataFrame:
        """
        Segmentación RFM (Recency, Frequency, Monetary).
        Clasifica clientes en Champions, Loyal, At Risk, Lost, etc.
        """
        self._check()
        return self.con.execute("""
            WITH rfm_raw AS (
                SELECT
                    user_id,
                    DATEDIFF('day',
                        MAX(CAST(order_date AS DATE)),
                        DATE '2023-12-31'
                    ) AS recency,
                    COUNT(*) AS frequency,
                    ROUND(SUM(amount), 2) AS monetary
                FROM orders
                WHERE status = 'completed'
                GROUP BY user_id
            ),
            rfm_scored AS (
                SELECT *,
                    NTILE(5) OVER (ORDER BY recency DESC)   AS r_score,
                    NTILE(5) OVER (ORDER BY frequency ASC)  AS f_score,
                    NTILE(5) OVER (ORDER BY monetary ASC)   AS m_score
                FROM rfm_raw
            )
            SELECT
                user_id,
                recency,
                frequency,
                monetary,
                r_score,
                f_score,
                m_score,
                ROUND((r_score + f_score + m_score) / 3.0, 1) AS rfm_avg,
                CASE
                    WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
                    WHEN r_score >= 3 AND f_score >= 3                  THEN 'Loyal Customers'
                    WHEN r_score >= 4 AND f_score <= 2                  THEN 'New Customers'
                    WHEN r_score <= 2 AND f_score >= 3                  THEN 'At Risk'
                    WHEN r_score <= 2 AND f_score <= 2                  THEN 'Lost'
                    ELSE 'Potential Loyalists'
                END AS segment
            FROM rfm_scored
            ORDER BY rfm_avg DESC
        """).df()

    # ──────────────────────────────────────────────
    # 3. FUNNEL DE CONVERSIÓN
    # ──────────────────────────────────────────────

    def conversion_funnel(self) -> pd.DataFrame:
        """
        Funnel de conversión paso a paso.
        Muestra cuántos usuarios llegan a cada etapa y el drop-off entre etapas.
        """
        self._check()
        return self.con.execute("""
            WITH steps AS (
                SELECT event_type, COUNT(DISTINCT user_id) AS users
                FROM events
                GROUP BY event_type
            ),
            ordered AS (
                SELECT
                    event_type,
                    users,
                    CASE event_type
                        WHEN 'page_view'       THEN 1
                        WHEN 'add_to_cart'     THEN 2
                        WHEN 'checkout_start'  THEN 3
                        WHEN 'payment_info'    THEN 4
                        WHEN 'purchase'        THEN 5
                    END AS step_order
                FROM steps
            )
            SELECT
                step_order,
                event_type,
                users,
                LAG(users) OVER (ORDER BY step_order) AS prev_users,
                ROUND(100.0 * users / FIRST_VALUE(users) OVER (ORDER BY step_order), 1) AS pct_of_top,
                ROUND(100.0 * users / NULLIF(LAG(users) OVER (ORDER BY step_order), 0), 1) AS pct_prev_step
            FROM ordered
            ORDER BY step_order
        """).df()

    # ──────────────────────────────────────────────
    # 4. MÉTRICAS DAU / WAU / MAU
    # ──────────────────────────────────────────────

    def dau_wau_mau(self) -> pd.DataFrame:
        """
        Usuarios activos por día, semana y mes.
        Define 'activo' como haber realizado al menos una compra completada.
        """
        self._check()
        return self.con.execute("""
            WITH daily AS (
                SELECT
                    CAST(order_date AS DATE) AS day,
                    COUNT(DISTINCT user_id) AS dau
                FROM orders
                WHERE status = 'completed'
                GROUP BY 1
            ),
            weekly AS (
                SELECT
                    DATE_TRUNC('week', CAST(order_date AS DATE)) AS week,
                    COUNT(DISTINCT user_id) AS wau
                FROM orders
                WHERE status = 'completed'
                GROUP BY 1
            ),
            monthly AS (
                SELECT
                    DATE_TRUNC('month', CAST(order_date AS DATE)) AS month,
                    COUNT(DISTINCT user_id) AS mau
                FROM orders
                WHERE status = 'completed'
                GROUP BY 1
            )
            SELECT
                d.day,
                d.dau,
                w.wau,
                m.mau,
                ROUND(100.0 * d.dau / NULLIF(w.wau, 0), 1) AS dau_over_wau
            FROM daily d
            LEFT JOIN weekly w ON DATE_TRUNC('week', d.day) = w.week
            LEFT JOIN monthly m ON DATE_TRUNC('month', d.day) = m.month
            ORDER BY d.day
        """).df()

    # ──────────────────────────────────────────────
    # 5. REVENUE POR CATEGORÍA Y PERÍODO
    # ──────────────────────────────────────────────

    def revenue_breakdown(self) -> pd.DataFrame:
        """
        Revenue mensual por categoría de producto.
        Incluye MoM growth y % del total mensual.
        """
        self._check()
        return self.con.execute("""
            WITH monthly_cat AS (
                SELECT
                    DATE_TRUNC('month', CAST(order_date AS DATE)) AS month,
                    category,
                    ROUND(SUM(amount), 2) AS revenue,
                    COUNT(*) AS orders_count
                FROM orders
                WHERE status = 'completed'
                GROUP BY 1, 2
            ),
            with_pct AS (
                SELECT
                    month,
                    category,
                    revenue,
                    orders_count,
                    ROUND(100.0 * revenue / SUM(revenue) OVER (PARTITION BY month), 1) AS pct_of_month,
                    LAG(revenue) OVER (PARTITION BY category ORDER BY month) AS prev_revenue
                FROM monthly_cat
            )
            SELECT
                month,
                category,
                revenue,
                orders_count,
                pct_of_month,
                ROUND(100.0 * (revenue - prev_revenue) / NULLIF(prev_revenue, 0), 1) AS mom_growth_pct
            FROM with_pct
            ORDER BY month, revenue DESC
        """).df()

    # ──────────────────────────────────────────────
    # 6. WINDOW FUNCTIONS SHOWCASE
    # ──────────────────────────────────────────────

    def window_functions_demo(self) -> pd.DataFrame:
        """
        Demostración de window functions avanzadas:
        ranking, running totals, moving averages, percentiles.
        """
        self._check()
        return self.con.execute("""
            WITH user_stats AS (
                SELECT
                    u.user_id,
                    u.country,
                    u.plan,
                    COUNT(o.order_id)       AS total_orders,
                    ROUND(SUM(o.amount), 2) AS total_spent,
                    ROUND(AVG(o.amount), 2) AS avg_order_value
                FROM users u
                LEFT JOIN orders o ON u.user_id = o.user_id AND o.status = 'completed'
                GROUP BY u.user_id, u.country, u.plan
            )
            SELECT
                user_id,
                country,
                plan,
                total_orders,
                total_spent,
                avg_order_value,

                -- Ranking global y por país
                RANK() OVER (ORDER BY total_spent DESC)                              AS global_rank,
                RANK() OVER (PARTITION BY country ORDER BY total_spent DESC)         AS country_rank,

                -- Percentil
                ROUND(PERCENT_RANK() OVER (ORDER BY total_spent) * 100, 1)           AS percentile,

                -- Running total de revenue
                ROUND(SUM(total_spent) OVER (ORDER BY total_spent DESC
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2)          AS running_revenue,

                -- Lag/Lead para comparar con el anterior/siguiente
                LAG(total_spent)  OVER (PARTITION BY country ORDER BY total_spent DESC) AS prev_user_spent,
                LEAD(total_spent) OVER (PARTITION BY country ORDER BY total_spent DESC) AS next_user_spent,

                -- Ntile para segmentar en cuartiles
                NTILE(4) OVER (ORDER BY total_spent DESC)                            AS quartile
            FROM user_stats
            WHERE total_orders > 0
            ORDER BY global_rank
            LIMIT 100
        """).df()

    # ──────────────────────────────────────────────
    # 7. RESUMEN EJECUTIVO
    # ──────────────────────────────────────────────

    def executive_summary(self) -> dict:
        """
        KPIs principales del negocio en un solo dict.
        Ideal para un widget de métricas o un reporte rápido.
        """
        self._check()
        row = self.con.execute("""
            SELECT
                COUNT(DISTINCT user_id)                                          AS total_users,
                COUNT(DISTINCT CASE WHEN status='completed' THEN order_id END)  AS completed_orders,
                ROUND(SUM(CASE WHEN status='completed' THEN amount END), 2)     AS total_revenue,
                ROUND(AVG(CASE WHEN status='completed' THEN amount END), 2)     AS avg_order_value,
                ROUND(100.0 * COUNT(CASE WHEN status='returned'  THEN 1 END) / COUNT(*), 1) AS return_rate_pct,
                ROUND(100.0 * COUNT(CASE WHEN status='cancelled' THEN 1 END) / COUNT(*), 1) AS cancel_rate_pct
            FROM orders
        """).fetchone()

        keys = ["total_users", "completed_orders", "total_revenue",
                "avg_order_value", "return_rate_pct", "cancel_rate_pct"]
        return dict(zip(keys, row))


# ──────────────────────────────────────────────
# 8. SIMULADOR DE IMPACTO EN CONVERSIÓN
# ──────────────────────────────────────────────

def simulate_funnel_impact(
    visitors: int = 500,
    avg_order_value: float = 397.48,
    # Tasas de conversión actuales (paso a paso)
    current_cart_rate: float = 0.948,
    current_checkout_rate: float = 0.736,
    current_payment_rate: float = 0.576,
    current_purchase_rate: float = 0.478,
    # Tasas de conversión mejoradas
    improved_cart_rate: float = None,
    improved_checkout_rate: float = None,
    improved_payment_rate: float = None,
    improved_purchase_rate: float = None,
) -> pd.DataFrame:
    """
    Simulador de impacto financiero de mejoras en el funnel de conversión.

    Calcula cuántas compras y cuánto revenue adicional generaría
    mejorar uno o varios pasos del funnel, manteniendo el mismo tráfico.

    Parámetros:
        visitors            : visitantes totales (tráfico de entrada)
        avg_order_value     : ticket promedio en $
        current_*_rate      : tasa de conversión actual por etapa (0 a 1)
        improved_*_rate     : tasa mejorada por etapa (None = sin cambio)

    Retorna:
        DataFrame con comparativa actual vs mejorado por etapa,
        más el impacto en compras y revenue.
    """
    # Si no se especifica mejora, usar la tasa actual
    imp_cart     = improved_cart_rate     or current_cart_rate
    imp_checkout = improved_checkout_rate or current_checkout_rate
    imp_payment  = improved_payment_rate  or current_payment_rate
    imp_purchase = improved_purchase_rate or current_purchase_rate

    stages = ["page_view", "add_to_cart", "checkout_start", "payment_info", "purchase"]

    # Usuarios actuales por etapa (acumulado)
    curr_rates = [1.0, current_cart_rate, current_checkout_rate,
                  current_payment_rate, current_purchase_rate]
    curr_users = [visitors]
    for r in curr_rates[1:]:
        curr_users.append(round(curr_users[-1] * r))

    # Usuarios mejorados por etapa
    imp_rates = [1.0, imp_cart, imp_checkout, imp_payment, imp_purchase]
    imp_users = [visitors]
    for r in imp_rates[1:]:
        imp_users.append(round(imp_users[-1] * r))

    # Conversión acumulada %
    curr_pct = [round(u / visitors * 100, 1) for u in curr_users]
    imp_pct  = [round(u / visitors * 100, 1) for u in imp_users]

    df = pd.DataFrame({
        "etapa":               stages,
        "usuarios_actual":     curr_users,
        "conv_acum_actual_%":  curr_pct,
        "usuarios_mejorado":   imp_users,
        "conv_acum_mejorado_%":imp_pct,
        "usuarios_extra":      [i - c for i, c in zip(imp_users, curr_users)],
    })

    # Revenue
    compras_actual   = curr_users[-1]
    compras_mejorado = imp_users[-1]
    rev_actual       = round(compras_actual   * avg_order_value, 2)
    rev_mejorado     = round(compras_mejorado * avg_order_value, 2)
    rev_incremental  = round(rev_mejorado - rev_actual, 2)
    uplift_pct       = round((compras_mejorado - compras_actual) / compras_actual * 100, 1)

    print("=" * 55)
    print("  SIMULADOR DE IMPACTO — FUNNEL DE CONVERSIÓN")
    print("=" * 55)
    print(f"  Visitantes:          {visitors:,}")
    print(f"  Ticket promedio:     ${avg_order_value:,.2f}")
    print("-" * 55)
    print(f"  Compras actuales:    {compras_actual:,}  →  Revenue: ${rev_actual:,.2f}")
    print(f"  Compras mejoradas:   {compras_mejorado:,}  →  Revenue: ${rev_mejorado:,.2f}")
    print(f"  Revenue incremental: ${rev_incremental:,.2f}  (+{uplift_pct}%)")
    print("=" * 55)

    return df, {
        "compras_actual":    compras_actual,
        "compras_mejorado":  compras_mejorado,
        "revenue_actual":    rev_actual,
        "revenue_mejorado":  rev_mejorado,
        "revenue_extra":     rev_incremental,
        "uplift_pct":        uplift_pct,
    }


# ──────────────────────────────────────────────
# MAIN — demo rápida en terminal
# ──────────────────────────────────────────────

if __name__ == "__main__":
    tk = SQLAnalyticsToolkit()
    tk.seed_demo_data()

    print("\n── Executive Summary ──")
    for k, v in tk.executive_summary().items():
        print(f"  {k}: {v}")

    print("\n── RFM Segmentation (top 10) ──")
    print(tk.rfm_segmentation().head(10).to_string(index=False))

    print("\n── Conversion Funnel ──")
    print(tk.conversion_funnel().to_string(index=False))

    print("\n── Cohort Retention (primeras filas) ──")
    print(tk.cohort_retention().head(12).to_string(index=False))
