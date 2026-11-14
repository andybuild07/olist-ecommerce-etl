import os
import pytest
import psycopg2

pytestmark = pytest.mark.skipif(
    not os.getenv("TEST_DATABASE_URL"),
    reason="Set TEST_DATABASE_URL to run Postgres idempotency tests",
)


@pytest.fixture
def pg_conn():
    conn = psycopg2.connect(os.environ["TEST_DATABASE_URL"])
    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()


def _load_sample_slice(cur, target_ds: str):
    cur.execute(
        """
        INSERT INTO public.dim_customers (
            customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO UPDATE SET
            customer_city = EXCLUDED.customer_city,
            customer_state = EXCLUDED.customer_state;
        """,
        ("cust_1", "ucust_1", "01000", "sao paulo", "SP"),
    )

    cur.execute(
        """
        INSERT INTO public.dim_products (
            product_id, product_category_name, product_name_lenght, product_description_lenght,
            product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO UPDATE SET
            product_category_name = EXCLUDED.product_category_name;
        """,
        ("prod_1", "beleza_saude", 40, 200, 2, 500, 20, 10, 15),
    )

    cur.execute(
        """
        INSERT INTO public.dim_sellers (
            seller_id, seller_zip_code_prefix, seller_city, seller_state
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (seller_id) DO UPDATE SET
            seller_city = EXCLUDED.seller_city,
            seller_state = EXCLUDED.seller_state;
        """,
        ("sell_1", "02000", "curitiba", "PR"),
    )

    cur.execute("DELETE FROM public.fact_order_items WHERE ds = %s;", (target_ds,))
    cur.execute(
        """
        INSERT INTO public.fact_order_items (
            order_id, order_item_id, customer_id, product_id, seller_id,
            payment_type, payment_sequential, payment_installments,
            price, freight_value, payment_value,
            shipping_limit_date, order_purchase_timestamp, order_approved_at,
            order_delivered_carrier_date, order_delivered_customer_date,
            order_estimated_delivery_date, order_status, ds
        )
        VALUES
            ('ord_1', 1, 'cust_1', 'prod_1', 'sell_1', 'credit_card', 1, 2, 100.00, 15.00, 230.00,
             '2018-01-10 00:00:00', '2018-01-05 12:00:00', '2018-01-05 12:30:00',
             '2018-01-06 09:00:00', '2018-01-08 14:00:00', '2018-01-15 00:00:00', 'delivered', %s),
            ('ord_1', 2, 'cust_1', 'prod_1', 'sell_1', 'credit_card', 1, 2, 130.00, 15.00, 230.00,
             '2018-01-10 00:00:00', '2018-01-05 12:00:00', '2018-01-05 12:30:00',
             '2018-01-06 09:00:00', '2018-01-08 14:00:00', '2018-01-15 00:00:00', 'delivered', %s);
        """,
        (target_ds, target_ds),
    )


def test_fact_load_is_idempotent_for_same_ds(pg_conn):
    target_ds = "2018-01-05"
    with pg_conn.cursor() as cur:
        # First run
        _load_sample_slice(cur, target_ds)
        cur.execute("SELECT COUNT(*), SUM(price) FROM public.fact_order_items WHERE ds = %s;", (target_ds,))
        count_1, rev_1 = cur.fetchone()

        # Second run (re-processing the exact same slice)
        _load_sample_slice(cur, target_ds)
        cur.execute("SELECT COUNT(*), SUM(price) FROM public.fact_order_items WHERE ds = %s;", (target_ds,))
        count_2, rev_2 = cur.fetchone()

    assert count_1 == 2
    assert count_2 == count_1
    assert float(rev_1) == pytest.approx(230.00)
    assert float(rev_2) == pytest.approx(float(rev_1))