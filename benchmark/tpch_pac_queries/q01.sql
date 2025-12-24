SELECT
    l_returnflag,
    l_linestatus,

    pac_sum(
            hash(l_orderkey ^ l_linenumber),
            l_quantity
    ) AS sum_qty,

    pac_sum(
            hash(l_orderkey ^ l_linenumber),
            l_extendedprice
    ) AS sum_base_price,

    pac_sum(
            hash(l_orderkey ^ l_linenumber),
            l_extendedprice * (1 - l_discount)
    ) AS sum_disc_price,

    pac_sum(
            hash(l_orderkey ^ l_linenumber),
            l_extendedprice * (1 - l_discount) * (1 + l_tax)
    ) AS sum_charge,

    pac_sum(
            hash(l_orderkey ^ l_linenumber),
            l_quantity
    ) / pac_count(
            hash(l_orderkey ^ l_linenumber)
        ) AS avg_qty,

    pac_sum(
            hash(l_orderkey ^ l_linenumber),
            l_extendedprice
    ) / pac_count(
            hash(l_orderkey ^ l_linenumber)
        ) AS avg_price,

    pac_sum(
            hash(l_orderkey ^ l_linenumber),
            l_discount
    ) / pac_count(
            hash(l_orderkey ^ l_linenumber)
        ) AS avg_disc,

    pac_count(
            hash(l_orderkey ^ l_linenumber)
    ) AS count_order

FROM
    lineitem
WHERE
    l_shipdate <= DATE '1998-09-02'
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
