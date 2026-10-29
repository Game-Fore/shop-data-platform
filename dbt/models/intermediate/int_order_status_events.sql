-- Переходы статусов заказа: одна строка = заказ впервые получил новый статус.
-- В журнале CDC есть события, которые статус не меняют: UPDATE total сразу после вставки
-- позиций (живой генератор) и delete-событие с образом строки до удаления. Их отбрасываем,
-- сравнивая статус с предыдущим событием того же заказа.
select
    order_id,
    status,
    event_at as status_at,
    lsn
from (
    select
        order_id,
        status,
        event_at,
        lsn,
        -- lagInFrame учитывает рамку окна, поэтому она задана явно; для первого события
        -- вернётся дефолт '' и оно останется (это переход "ничего -> new")
        lagInFrame(status) over (
            partition by order_id order by lsn
            rows between unbounded preceding and current row
        ) as prev_status
    from {{ ref('stg_order_events') }}
    where not is_delete
)
where status != prev_status
