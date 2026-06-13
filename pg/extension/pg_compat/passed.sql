-- id: aggregates_11_select_avg_b_numeric_10_3_as_avg_107_943_from_aggtest_eeb75f37
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:30
-- compare: multiset
SELECT avg(b)::numeric(10,3) AS avg_107_943 FROM aggtest;

-- id: aggregates_13_select_sum_four_as_sum_1500_from_onek_9440d3f5
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:37
-- compare: multiset
SELECT sum(four) AS sum_1500 FROM onek;

-- id: aggregates_14_select_sum_a_as_sum_198_from_aggtest_dc90beaa
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:40
-- compare: multiset
SELECT sum(a) AS sum_198 FROM aggtest;

-- id: aggregates_15_select_sum_b_as_avg_431_773_from_aggtest_65bb080b
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:41
-- compare: multiset
SELECT sum(b) AS avg_431_773 FROM aggtest;

-- id: aggregates_17_select_max_four_as_max_3_from_onek_57e17af7
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:43
-- compare: multiset
SELECT max(four) AS max_3 FROM onek;

-- id: aggregates_18_select_max_a_as_max_100_from_aggtest_87706287
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:45
-- compare: multiset
SELECT max(a) AS max_100 FROM aggtest;

-- id: aggregates_19_select_max_aggtest_b_as_max_324_78_from_aggtest_3d1a1fc2
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:46
-- compare: multiset
SELECT max(aggtest.b) AS max_324_78 FROM aggtest;

-- id: aggregates_21_select_stddev_pop_b_from_aggtest_8d318cc2
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:48
-- compare: multiset
SELECT stddev_pop(b) FROM aggtest;

-- id: aggregates_22_select_stddev_samp_b_from_aggtest_b1f790b8
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:50
-- compare: multiset
SELECT stddev_samp(b) FROM aggtest;

-- id: aggregates_23_select_var_pop_b_from_aggtest_bb8d765b
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:51
-- compare: multiset
SELECT var_pop(b) FROM aggtest;

-- id: aggregates_24_select_var_samp_b_from_aggtest_7cbefc7a
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:52
-- compare: multiset
SELECT var_samp(b) FROM aggtest;

-- id: aggregates_29_select_var_pop_1_0_float8_var_samp_2_0_float8_7fe3f273
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:58
-- compare: multiset
SELECT var_pop(1.0::float8), var_samp(2.0::float8);

-- id: aggregates_30_select_stddev_pop_3_0_float8_stddev_samp_4_0_float8_2ab456e6
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:62
-- compare: multiset
SELECT stddev_pop(3.0::float8), stddev_samp(4.0::float8);

-- id: aggregates_35_select_var_pop_1_0_float4_var_samp_2_0_float4_bf1846d5
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:67
-- compare: multiset
SELECT var_pop(1.0::float4), var_samp(2.0::float4);

-- id: aggregates_36_select_stddev_pop_3_0_float4_stddev_samp_4_0_float4_e5a26808
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:68
-- compare: multiset
SELECT stddev_pop(3.0::float4), stddev_samp(4.0::float4);

-- id: aggregates_41_select_var_pop_1_0_numeric_var_samp_2_0_numeric_2c3efe0e
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:73
-- compare: multiset
SELECT var_pop(1.0::numeric), var_samp(2.0::numeric);

-- id: aggregates_42_select_stddev_pop_3_0_numeric_stddev_samp_4_0_numeric_c520ae86
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:74
-- compare: multiset
SELECT stddev_pop(3.0::numeric), stddev_samp(4.0::numeric);

-- id: aggregates_57_select_sum_x_float8_avg_x_float8_var_pop_x_float8_from_values_1_infinity_320c507f
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:91
-- compare: multiset
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
FROM (VALUES ('1'), ('infinity')) v(x);

-- id: aggregates_58_select_sum_x_float8_avg_x_float8_var_pop_x_float8_from_values_infinity_1_edb2309f
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:95
-- compare: multiset
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
FROM (VALUES ('infinity'), ('1')) v(x);

-- id: aggregates_59_select_sum_x_float8_avg_x_float8_var_pop_x_float8_from_values_infinity_i_ca8e35ff
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:97
-- compare: multiset
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
FROM (VALUES ('infinity'), ('infinity')) v(x);

-- id: aggregates_60_select_sum_x_float8_avg_x_float8_var_pop_x_float8_from_values_infinity_i_89fee83b
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:99
-- compare: multiset
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
FROM (VALUES ('-infinity'), ('infinity')) v(x);

-- id: aggregates_61_select_sum_x_float8_avg_x_float8_var_pop_x_float8_from_values_infinity_i_5df8a8dd
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:101
-- compare: multiset
SELECT sum(x::float8), avg(x::float8), var_pop(x::float8)
FROM (VALUES ('-infinity'), ('-infinity')) v(x);

-- id: aggregates_68_select_avg_x_float8_var_pop_x_float8_from_values_7000000000005_700000000_18e7271d
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:117
-- compare: multiset
SELECT avg(x::float8), var_pop(x::float8)
FROM (VALUES (7000000000005), (7000000000007)) v(x);

-- id: aggregates_70_select_regr_sxx_b_a_from_aggtest_55ec9342
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:122
-- compare: multiset
SELECT regr_sxx(b, a) FROM aggtest;

-- id: aggregates_71_select_regr_syy_b_a_from_aggtest_0a0437cb
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:123
-- compare: multiset
SELECT regr_syy(b, a) FROM aggtest;

-- id: aggregates_72_select_regr_sxy_b_a_from_aggtest_8930d9a0
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:124
-- compare: multiset
SELECT regr_sxy(b, a) FROM aggtest;

-- id: aggregates_73_select_regr_avgx_b_a_regr_avgy_b_a_from_aggtest_ee0a6b62
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:125
-- compare: multiset
SELECT regr_avgx(b, a), regr_avgy(b, a) FROM aggtest;

-- id: aggregates_74_select_regr_r2_b_a_from_aggtest_c7716ff5
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:126
-- compare: multiset
SELECT regr_r2(b, a) FROM aggtest;

-- id: aggregates_75_select_regr_slope_b_a_regr_intercept_b_a_from_aggtest_26e0a6f5
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:127
-- compare: multiset
SELECT regr_slope(b, a), regr_intercept(b, a) FROM aggtest;

-- id: aggregates_76_select_covar_pop_b_a_covar_samp_b_a_from_aggtest_ed64dba8
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:128
-- compare: multiset
SELECT covar_pop(b, a), covar_samp(b, a) FROM aggtest;

-- id: aggregates_77_select_corr_b_a_from_aggtest_8557c168
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:129
-- compare: multiset
SELECT corr(b, a) FROM aggtest;

-- id: aggregates_78_select_covar_pop_1_float8_2_float8_covar_samp_3_float8_4_float8_b5e39b74
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:130
-- compare: multiset
SELECT covar_pop(1::float8,2::float8), covar_samp(3::float8,4::float8);

-- id: aggregates_79_select_covar_pop_1_float8_inf_float8_covar_samp_3_float8_inf_float8_5045161d
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:133
-- compare: multiset
SELECT covar_pop(1::float8,'inf'::float8), covar_samp(3::float8,'inf'::float8);

-- id: aggregates_80_select_covar_pop_1_float8_nan_float8_covar_samp_3_float8_nan_float8_a347d273
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:134
-- compare: multiset
SELECT covar_pop(1::float8,'nan'::float8), covar_samp(3::float8,'nan'::float8);

-- id: aggregates_96_select_count_four_as_cnt_1000_from_onek_a47ea9fe
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:159
-- compare: multiset
SELECT count(four) AS cnt_1000 FROM onek;

-- id: aggregates_97_select_count_distinct_four_as_cnt_4_from_onek_641615ea
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:162
-- compare: multiset
SELECT count(DISTINCT four) AS cnt_4 FROM onek;

-- id: aggregates_98_select_ten_count_sum_four_from_onek_group_by_ten_order_by_ten_9aae0237
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:163
-- compare: ordered
select ten, count(*), sum(four) from onek
group by ten order by ten;

-- id: aggregates_99_select_ten_count_four_sum_distinct_four_from_onek_group_by_ten_order_by__97c932e9
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:166
-- compare: ordered
select ten, count(four), sum(DISTINCT four) from onek
group by ten order by ten;

-- id: aggregates_126_select_min_unique1_from_tenk1_aa69d0fe
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:353
-- compare: multiset
select min(unique1) from tenk1;

-- id: aggregates_128_select_max_unique1_from_tenk1_3fa1cfa2
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:356
-- compare: multiset
select max(unique1) from tenk1;

-- id: aggregates_130_select_max_unique1_from_tenk1_where_unique1_42_bf1b96ff
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:359
-- compare: multiset
select max(unique1) from tenk1 where unique1 < 42;

-- id: aggregates_132_select_max_unique1_from_tenk1_where_unique1_42_f2943106
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:362
-- compare: multiset
select max(unique1) from tenk1 where unique1 > 42;

-- id: aggregates_136_select_max_unique1_from_tenk1_where_unique1_42000_116734fc
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:372
-- compare: multiset
select max(unique1) from tenk1 where unique1 > 42000;

-- id: aggregates_139_select_max_tenthous_from_tenk1_where_thousand_33_272efee4
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:378
-- compare: multiset
select max(tenthous) from tenk1 where thousand = 33;

-- id: aggregates_141_select_min_tenthous_from_tenk1_where_thousand_33_d44a6c73
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:381
-- compare: multiset
select min(tenthous) from tenk1 where thousand = 33;

-- id: aggregates_145_select_distinct_max_unique2_from_tenk1_35d59c5b
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:393
-- compare: multiset
select distinct max(unique2) from tenk1;

-- id: aggregates_147_select_max_unique2_from_tenk1_order_by_1_d6db44aa
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:396
-- compare: ordered
select max(unique2) from tenk1 order by 1;

-- id: aggregates_149_select_max_unique2_from_tenk1_order_by_max_unique2_8dcfffaa
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:399
-- compare: ordered
select max(unique2) from tenk1 order by max(unique2);

-- id: aggregates_151_select_max_unique2_from_tenk1_order_by_max_unique2_1_65a056bc
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:402
-- compare: ordered
select max(unique2) from tenk1 order by max(unique2)+1;

-- id: aggregates_176_select_f1_select_distinct_min_t1_f1_from_int4_tbl_t1_where_t1_f1_t0_f1_f_aeef22a4
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:444
-- compare: multiset
select f1, (select distinct min(t1.f1) from int4_tbl t1 where t1.f1 = t0.f1)
from int4_tbl t0;

-- id: aggregates_314_select_select_count_from_values_1_t0_inner_c_from_values_2_3_t1_outer_c_2aae5f8c
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:864
-- compare: multiset
select (select count(*)
        from (values (1)) t0(inner_c))
from (values (2),(3)) t1(outer_c);

-- id: aggregates_467_select_min_x_order_by_y_from_values_1_null_as_d_x_y_98161c4a
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:1444
-- compare: ordered
SELECT min(x ORDER BY y) FROM (VALUES(1, NULL)) AS d(x,y);

-- id: aggregates_468_select_min_x_order_by_y_from_values_1_2_as_d_x_y_c0554c03
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:1450
-- compare: ordered
SELECT min(x ORDER BY y) FROM (VALUES(1, 2)) AS d(x,y);

-- id: aggregates_476_select_unique1_count_sum_twothousand_from_tenk1_group_by_unique1_having__f519d8be
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:1475
-- compare: ordered
select unique1, count(*), sum(twothousand) from tenk1
group by unique1
having sum(fivethous) > 4975
order by sum(twothousand);

-- id: alter_table_140_select_unique1_from_tenk1_where_unique1_5_6f2736f1
-- origin: postgres REL_17_STABLE src/test/regress/sql/alter_table.sql:271
-- compare: multiset
SELECT unique1 FROM tenk1 WHERE unique1 < 5;

-- id: alter_table_758_select_from_foo_408b0583
-- origin: postgres REL_17_STABLE src/test/regress/sql/alter_table.sql:1320
-- compare: multiset
select * from foo;

-- id: boolean_1_select_1_as_one_7805bbb1
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:1
-- compare: multiset
SELECT 1 AS one;

-- id: boolean_2_select_true_as_true_a64339a3
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:8
-- compare: multiset
SELECT true AS true;

-- id: boolean_3_select_false_as_false_a5251792
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:15
-- compare: multiset
SELECT false AS false;

-- id: boolean_4_select_bool_t_as_true_cab2245c
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:17
-- compare: multiset
SELECT bool 't' AS true;

-- id: boolean_5_select_bool_f_as_false_db83bd2a
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:19
-- compare: multiset
SELECT bool '   f           ' AS false;

-- id: boolean_6_select_bool_true_as_true_865d9754
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:21
-- compare: multiset
SELECT bool 'true' AS true;

-- id: boolean_8_select_bool_false_as_false_480ddeb9
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:25
-- compare: multiset
SELECT bool 'false' AS false;

-- id: boolean_10_select_bool_y_as_true_5acccc2b
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:29
-- compare: multiset
SELECT bool 'y' AS true;

-- id: boolean_11_select_bool_yes_as_true_23c332b9
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:31
-- compare: multiset
SELECT bool 'yes' AS true;

-- id: boolean_13_select_bool_n_as_false_19df4cad
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:35
-- compare: multiset
SELECT bool 'n' AS false;

-- id: boolean_14_select_bool_no_as_false_cc684866
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:37
-- compare: multiset
SELECT bool 'no' AS false;

-- id: boolean_16_select_bool_on_as_true_d41dc2a4
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:41
-- compare: multiset
SELECT bool 'on' AS true;

-- id: boolean_17_select_bool_off_as_false_2f087e3a
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:43
-- compare: multiset
SELECT bool 'off' AS false;

-- id: boolean_18_select_bool_of_as_false_532b4922
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:45
-- compare: multiset
SELECT bool 'of' AS false;

-- id: boolean_22_select_bool_1_as_true_8907277b
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:53
-- compare: multiset
SELECT bool '1' AS true;

-- id: boolean_24_select_bool_0_as_false_ad6679a9
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:57
-- compare: multiset
SELECT bool '0' AS false;

-- id: boolean_30_select_bool_t_or_bool_f_as_true_c74b0018
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:68
-- compare: multiset
SELECT bool 't' or bool 'f' AS true;

-- id: boolean_31_select_bool_t_and_bool_f_as_false_98fbe290
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:72
-- compare: multiset
SELECT bool 't' and bool 'f' AS false;

-- id: boolean_32_select_not_bool_f_as_true_5a28a76b
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:74
-- compare: multiset
SELECT not bool 'f' AS true;

-- id: boolean_33_select_bool_t_bool_f_as_false_4c78b83f
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:76
-- compare: multiset
SELECT bool 't' = bool 'f' AS false;

-- id: boolean_34_select_bool_t_bool_f_as_true_0af362c5
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:78
-- compare: multiset
SELECT bool 't' <> bool 'f' AS true;

-- id: boolean_35_select_bool_t_bool_f_as_true_014b59e1
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:80
-- compare: multiset
SELECT bool 't' > bool 'f' AS true;

-- id: boolean_36_select_bool_t_bool_f_as_true_e1db0bf1
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:82
-- compare: multiset
SELECT bool 't' >= bool 'f' AS true;

-- id: boolean_37_select_bool_f_bool_t_as_true_fbc690dc
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:84
-- compare: multiset
SELECT bool 'f' < bool 't' AS true;

-- id: boolean_38_select_bool_f_bool_t_as_true_bf877b1d
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:86
-- compare: multiset
SELECT bool 'f' <= bool 't' AS true;

-- id: boolean_39_select_true_text_boolean_as_true_false_text_boolean_as_false_9a5ef86e
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:88
-- compare: multiset
SELECT 'TrUe'::text::boolean AS true, 'fAlse'::text::boolean AS false;

-- id: boolean_40_select_true_text_boolean_as_true_false_text_boolean_as_false_9d2c0fab
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:91
-- compare: multiset
SELECT '    true   '::text::boolean AS true,
       '     FALSE'::text::boolean AS false;

-- id: boolean_41_select_true_boolean_text_as_true_false_boolean_text_as_false_54049412
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:93
-- compare: multiset
SELECT true::boolean::text AS true, false::boolean::text AS false;

-- id: boolean_48_select_booltbl1_from_booltbl1_ef8c5f30
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:105
-- compare: multiset
SELECT BOOLTBL1.* FROM BOOLTBL1;

-- id: boolean_49_select_booltbl1_from_booltbl1_where_f1_bool_true_991bacba
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:109
-- compare: multiset
SELECT BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 = bool 'true';

-- id: boolean_50_select_booltbl1_from_booltbl1_where_f1_bool_false_3961abf0
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:114
-- compare: multiset
SELECT BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 <> bool 'false';

-- id: boolean_53_select_booltbl1_from_booltbl1_where_f1_bool_false_246788b4
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:125
-- compare: multiset
SELECT BOOLTBL1.*
   FROM BOOLTBL1
   WHERE f1 = bool 'false';

-- id: boolean_60_select_booltbl2_from_booltbl2_1a74dcee
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:145
-- compare: multiset
SELECT BOOLTBL2.* FROM BOOLTBL2;

-- id: boolean_61_select_booltbl1_booltbl2_from_booltbl1_booltbl2_where_booltbl2_f1_booltb_a5d3f81c
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:148
-- compare: multiset
SELECT BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 <> BOOLTBL1.f1;

-- id: boolean_63_select_booltbl1_booltbl2_from_booltbl1_booltbl2_where_booltbl2_f1_booltb_95bdab8b
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:158
-- compare: multiset
SELECT BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 = BOOLTBL1.f1 and BOOLTBL1.f1 = bool 'false';

-- id: boolean_64_select_booltbl1_booltbl2_from_booltbl1_booltbl2_where_booltbl2_f1_booltb_bda7d0d3
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:163
-- compare: ordered
SELECT BOOLTBL1.*, BOOLTBL2.*
   FROM BOOLTBL1, BOOLTBL2
   WHERE BOOLTBL2.f1 = BOOLTBL1.f1 or BOOLTBL1.f1 = bool 'true'
   ORDER BY BOOLTBL1.f1, BOOLTBL2.f1;

-- id: boolean_65_select_f1_from_booltbl1_where_f1_is_true_c3586987
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:169
-- compare: multiset
SELECT f1
   FROM BOOLTBL1
   WHERE f1 IS TRUE;

-- id: boolean_66_select_f1_from_booltbl1_where_f1_is_not_false_fedb4fad
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:179
-- compare: multiset
SELECT f1
   FROM BOOLTBL1
   WHERE f1 IS NOT FALSE;

-- id: boolean_67_select_f1_from_booltbl1_where_f1_is_false_c602ef1d
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:183
-- compare: multiset
SELECT f1
   FROM BOOLTBL1
   WHERE f1 IS FALSE;

-- id: boolean_68_select_f1_from_booltbl1_where_f1_is_not_true_9b833b94
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:187
-- compare: multiset
SELECT f1
   FROM BOOLTBL1
   WHERE f1 IS NOT TRUE;

-- id: boolean_69_select_f1_from_booltbl2_where_f1_is_true_ab2f12fc
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:191
-- compare: multiset
SELECT f1
   FROM BOOLTBL2
   WHERE f1 IS TRUE;

-- id: boolean_70_select_f1_from_booltbl2_where_f1_is_not_false_ccd49b05
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:195
-- compare: multiset
SELECT f1
   FROM BOOLTBL2
   WHERE f1 IS NOT FALSE;

-- id: boolean_71_select_f1_from_booltbl2_where_f1_is_false_9aacbdaa
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:199
-- compare: multiset
SELECT f1
   FROM BOOLTBL2
   WHERE f1 IS FALSE;

-- id: boolean_72_select_f1_from_booltbl2_where_f1_is_not_true_0b3884ac
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:203
-- compare: multiset
SELECT f1
   FROM BOOLTBL2
   WHERE f1 IS NOT TRUE;

-- id: boolean_92_select_0_boolean_c118776d
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:251
-- compare: multiset
SELECT 0::boolean;

-- id: boolean_93_select_1_boolean_f26ba46e
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:254
-- compare: multiset
SELECT 1::boolean;

-- id: boolean_94_select_2_boolean_e3a99ca6
-- origin: postgres REL_17_STABLE src/test/regress/sql/boolean.sql:255
-- compare: multiset
SELECT 2::boolean;

-- id: btree_index_33_select_hundred_twenty_from_tenk1_where_hundred_48_order_by_hundred_desc__c23625dd
-- origin: postgres REL_17_STABLE src/test/regress/sql/btree_index.sql:127
-- compare: ordered
select hundred, twenty from tenk1 where hundred < 48 order by hundred desc limit 1;

-- id: btree_index_35_select_hundred_twenty_from_tenk1_where_hundred_48_order_by_hundred_desc__6edb5cfc
-- origin: postgres REL_17_STABLE src/test/regress/sql/btree_index.sql:134
-- compare: ordered
select hundred, twenty from tenk1 where hundred <= 48 order by hundred desc limit 1;

-- id: btree_index_37_select_distinct_hundred_from_tenk1_where_hundred_in_47_48_72_82_81e20d09
-- origin: postgres REL_17_STABLE src/test/regress/sql/btree_index.sql:141
-- compare: multiset
select distinct hundred from tenk1 where hundred in (47, 48, 72, 82);

-- id: btree_index_39_select_distinct_hundred_from_tenk1_where_hundred_in_47_48_72_82_order_by_8e820882
-- origin: postgres REL_17_STABLE src/test/regress/sql/btree_index.sql:145
-- compare: ordered
select distinct hundred from tenk1 where hundred in (47, 48, 72, 82) order by hundred desc;

-- id: btree_index_41_select_thousand_from_tenk1_where_thousand_in_364_366_380_and_tenthous_20_64b8c032
-- origin: postgres REL_17_STABLE src/test/regress/sql/btree_index.sql:149
-- compare: multiset
select thousand from tenk1 where thousand in (364, 366,380) and tenthous = 200000;

-- id: case_13_select_3_as_one_case_when_1_2_then_3_end_as_simple_when_07ca7f6c
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:26
-- compare: multiset
SELECT '3' AS "One",
  CASE
    WHEN 1 < 2 THEN 3
  END AS "Simple WHEN";

-- id: case_14_select_null_as_one_case_when_1_2_then_3_end_as_simple_default_f675f193
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:35
-- compare: multiset
SELECT '<NULL>' AS "One",
  CASE
    WHEN 1 > 2 THEN 3
  END AS "Simple default";

-- id: case_15_select_3_as_one_case_when_1_2_then_3_else_4_end_as_simple_else_3cc64e7e
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:40
-- compare: multiset
SELECT '3' AS "One",
  CASE
    WHEN 1 < 2 THEN 3
    ELSE 4
  END AS "Simple ELSE";

-- id: case_16_select_4_as_one_case_when_1_2_then_3_else_4_end_as_else_default_692e33e7
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:46
-- compare: multiset
SELECT '4' AS "One",
  CASE
    WHEN 1 > 2 THEN 3
    ELSE 4
  END AS "ELSE default";

-- id: case_17_select_6_as_one_case_when_1_2_then_3_when_4_5_then_6_else_7_end_as_two_w_aec43662
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:52
-- compare: multiset
SELECT '6' AS "One",
  CASE
    WHEN 1 > 2 THEN 3
    WHEN 4 < 5 THEN 6
    ELSE 7
  END AS "Two WHEN with default";

-- id: case_18_select_7_as_none_case_when_random_0_then_1_end_as_null_on_no_matches_b37075e0
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:59
-- compare: multiset
SELECT '7' AS "None",
   CASE WHEN random() < 0 THEN 1
   END AS "NULL on no matches";

-- id: case_22_select_case_a_when_a_then_1_else_2_end_a30aa90b
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:72
-- compare: multiset
SELECT CASE 'a' WHEN 'a' THEN 1 ELSE 2 END;

-- id: case_23_select_case_when_i_3_then_i_end_as_3_or_null_from_case_tbl_d6c98490
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:75
-- compare: multiset
SELECT
  CASE
    WHEN i >= 3 THEN i
  END AS ">= 3 or Null"
  FROM CASE_TBL;

-- id: case_24_select_case_when_i_3_then_i_i_else_i_end_as_simplest_math_from_case_tbl_c305202c
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:85
-- compare: multiset
SELECT
  CASE WHEN i >= 3 THEN (i + i)
       ELSE i
  END AS "Simplest Math"
  FROM CASE_TBL;

-- id: case_25_select_i_as_value_case_when_i_0_then_small_when_i_0_then_zero_when_i_1_t_e3b1a7e5
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:91
-- compare: multiset
SELECT i AS "Value",
  CASE WHEN (i < 0) THEN 'small'
       WHEN (i = 0) THEN 'zero'
       WHEN (i = 1) THEN 'one'
       WHEN (i = 2) THEN 'two'
       ELSE 'big'
  END AS "Category"
  FROM CASE_TBL;

-- id: case_26_select_case_when_i_0_or_i_0_then_small_when_i_0_or_i_0_then_zero_when_i__8729f906
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:100
-- compare: multiset
SELECT
  CASE WHEN ((i < 0) or (i < 0)) THEN 'small'
       WHEN ((i = 0) or (i = 0)) THEN 'zero'
       WHEN ((i = 1) or (i = 1)) THEN 'one'
       WHEN ((i = 2) or (i = 2)) THEN 'two'
       ELSE 'big'
  END AS "Category"
  FROM CASE_TBL;

-- id: case_27_select_from_case_tbl_where_coalesce_f_i_4_d5a532ee
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:109
-- compare: multiset
SELECT * FROM CASE_TBL WHERE COALESCE(f,i) = 4;

-- id: case_28_select_from_case_tbl_where_nullif_f_i_2_9f701516
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:121
-- compare: multiset
SELECT * FROM CASE_TBL WHERE NULLIF(f,i) = 2;

-- id: case_29_select_coalesce_a_f_b_i_b_j_from_case_tbl_a_case2_tbl_b_85df9f4a
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:123
-- compare: multiset
SELECT COALESCE(a.f, b.i, b.j)
  FROM CASE_TBL a, CASE2_TBL b;

-- id: case_30_select_from_case_tbl_a_case2_tbl_b_where_coalesce_a_f_b_i_b_j_2_a184e8c5
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:126
-- compare: multiset
SELECT *
  FROM CASE_TBL a, CASE2_TBL b
  WHERE COALESCE(a.f, b.i, b.j) = 2;

-- id: case_31_select_nullif_a_i_b_i_as_nullif_a_i_b_i_nullif_b_i_4_as_nullif_b_i_4_fro_70d13697
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:130
-- compare: multiset
SELECT NULLIF(a.i,b.i) AS "NULLIF(a.i,b.i)",
  NULLIF(b.i, 4) AS "NULLIF(b.i,4)"
  FROM CASE_TBL a, CASE2_TBL b;

-- id: case_32_select_from_case_tbl_a_case2_tbl_b_where_coalesce_f_b_i_2_5e9d64ab
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:134
-- compare: multiset
SELECT *
  FROM CASE_TBL a, CASE2_TBL b
  WHERE COALESCE(f,b.i) = 2;

-- id: case_37_select_from_case_tbl_45ac218a
-- origin: postgres REL_17_STABLE src/test/regress/sql/case.sql:157
-- compare: multiset
SELECT * FROM CASE_TBL;

-- id: char_11_select_from_char_tbl_09ef8c17
-- origin: postgres REL_17_STABLE src/test/regress/sql/char.sql:33
-- compare: multiset
SELECT * FROM CHAR_TBL;

-- id: create_index_360_select_from_tenk1_where_thousand_42_and_tenthous_1_or_tenthous_3_or_tent_5c96b33b
-- origin: postgres REL_17_STABLE src/test/regress/sql/create_index.sql:733
-- compare: multiset
SELECT * FROM tenk1
  WHERE thousand = 42 AND (tenthous = 1 OR tenthous = 3 OR tenthous = 42);

-- id: create_index_369_select_unique1_from_tenk1_where_unique1_in_1_42_7_order_by_unique1_a4921a8e
-- origin: postgres REL_17_STABLE src/test/regress/sql/create_index.sql:767
-- compare: ordered
SELECT unique1 FROM tenk1
WHERE unique1 IN (1,42,7)
ORDER BY unique1;

-- id: create_index_371_select_thousand_tenthous_from_tenk1_where_thousand_2_and_tenthous_in_100_374ec0f2
-- origin: postgres REL_17_STABLE src/test/regress/sql/create_index.sql:777
-- compare: ordered
SELECT thousand, tenthous FROM tenk1
WHERE thousand < 2 AND tenthous IN (1001,3000)
ORDER BY thousand;

-- id: create_index_373_select_thousand_tenthous_from_tenk1_where_thousand_2_and_tenthous_in_100_421c0fc2
-- origin: postgres REL_17_STABLE src/test/regress/sql/create_index.sql:787
-- compare: ordered
SELECT thousand, tenthous FROM tenk1
WHERE thousand < 2 AND tenthous IN (1001,3000)
ORDER BY thousand DESC, tenthous DESC;

-- id: create_index_379_select_unique1_from_tenk1_where_unique1_in_1_42_7_and_unique1_1_05d89e89
-- origin: postgres REL_17_STABLE src/test/regress/sql/create_index.sql:807
-- compare: multiset
SELECT unique1 FROM tenk1 WHERE unique1 IN (1, 42, 7) and unique1 = 1;

-- id: create_index_381_select_unique1_from_tenk1_where_unique1_in_1_42_7_and_unique1_12345_4628b8fe
-- origin: postgres REL_17_STABLE src/test/regress/sql/create_index.sql:812
-- compare: multiset
SELECT unique1 FROM tenk1 WHERE unique1 IN (1, 42, 7) and unique1 = 12345;

-- id: create_index_383_select_unique1_from_tenk1_where_unique1_in_1_42_7_and_unique1_42_98190142
-- origin: postgres REL_17_STABLE src/test/regress/sql/create_index.sql:817
-- compare: multiset
SELECT unique1 FROM tenk1 WHERE unique1 IN (1, 42, 7) and unique1 >= 42;

-- id: create_index_385_select_unique1_from_tenk1_where_unique1_in_1_42_7_and_unique1_42_ec31c914
-- origin: postgres REL_17_STABLE src/test/regress/sql/create_index.sql:822
-- compare: multiset
SELECT unique1 FROM tenk1 WHERE unique1 IN (1, 42, 7) and unique1 > 42;

-- id: create_index_387_select_unique1_from_tenk1_where_unique1_9996_and_unique1_9999_4e492aa6
-- origin: postgres REL_17_STABLE src/test/regress/sql/create_index.sql:827
-- compare: multiset
SELECT unique1 FROM tenk1 WHERE unique1 > 9996 and unique1 >= 9999;

-- id: create_index_389_select_unique1_from_tenk1_where_unique1_3_and_unique1_3_1decc8c6
-- origin: postgres REL_17_STABLE src/test/regress/sql/create_index.sql:832
-- compare: multiset
SELECT unique1 FROM tenk1 WHERE unique1 < 3 and unique1 <= 3;

-- id: create_index_391_select_unique1_from_tenk1_where_unique1_3_and_unique1_1_bigint_9a8a8f72
-- origin: postgres REL_17_STABLE src/test/regress/sql/create_index.sql:837
-- compare: multiset
SELECT unique1 FROM tenk1 WHERE unique1 < 3 and unique1 < (-1)::bigint;

-- id: create_index_393_select_unique1_from_tenk1_where_unique1_in_1_42_7_and_unique1_1_bigint_863a51b4
-- origin: postgres REL_17_STABLE src/test/regress/sql/create_index.sql:842
-- compare: multiset
SELECT unique1 FROM tenk1 WHERE unique1 IN (1, 42, 7) and unique1 < (-1)::bigint;

-- id: float4_31_select_nan_float4_4953343d
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:43
-- compare: multiset
SELECT 'NaN'::float4;

-- id: float4_34_select_infinity_float4_78041856
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:48
-- compare: multiset
SELECT 'infinity'::float4;

-- id: float4_39_select_infinity_float4_100_0_7f81f000
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:54
-- compare: multiset
SELECT 'Infinity'::float4 + 100.0;

-- id: float4_40_select_infinity_float4_infinity_float4_55ec9519
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:56
-- compare: multiset
SELECT 'Infinity'::float4 / 'Infinity'::float4;

-- id: float4_41_select_42_float4_infinity_float4_14ace1ed
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:57
-- compare: multiset
SELECT '42'::float4 / 'Infinity'::float4;

-- id: float4_42_select_nan_float4_nan_float4_a7c47e34
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:58
-- compare: multiset
SELECT 'nan'::float4 / 'nan'::float4;

-- id: float4_43_select_nan_float4_0_float4_8b0f3238
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:59
-- compare: multiset
SELECT 'nan'::float4 / '0'::float4;

-- id: float4_45_select_from_float4_tbl_9efec8e5
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:61
-- compare: multiset
SELECT * FROM FLOAT4_TBL;

-- id: float4_46_select_f_from_float4_tbl_f_where_f_f1_1004_3_6953d50c
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:63
-- compare: multiset
SELECT f.* FROM FLOAT4_TBL f WHERE f.f1 <> '1004.3';

-- id: float4_47_select_f_from_float4_tbl_f_where_f_f1_1004_3_72793805
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:65
-- compare: multiset
SELECT f.* FROM FLOAT4_TBL f WHERE f.f1 = '1004.3';

-- id: float4_61_select_32767_4_float4_int2_0a6cd868
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:101
-- compare: multiset
SELECT '32767.4'::float4::int2;

-- id: float4_63_select_32768_4_float4_int2_613d0673
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:105
-- compare: multiset
SELECT '-32768.4'::float4::int2;

-- id: float4_65_select_2147483520_float4_int4_3bde8d89
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:107
-- compare: multiset
SELECT '2147483520'::float4::int4;

-- id: float4_67_select_2147483648_5_float4_int4_3c7f4b39
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:109
-- compare: multiset
SELECT '-2147483648.5'::float4::int4;

-- id: float4_69_select_9223369837831520256_float4_int8_c1d75f03
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:111
-- compare: multiset
SELECT '9223369837831520256'::float4::int8;

-- id: float4_71_select_9223372036854775808_5_float4_int8_d694aec8
-- origin: postgres REL_17_STABLE src/test/regress/sql/float4.sql:113
-- compare: multiset
SELECT '-9223372036854775808.5'::float4::int8;

-- id: float8_24_select_nan_float8_e5d73447
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:41
-- compare: multiset
SELECT 'NaN'::float8;

-- id: float8_27_select_infinity_float8_3623bbcf
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:46
-- compare: multiset
SELECT 'infinity'::float8;

-- id: float8_32_select_infinity_float8_100_0_be134064
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:52
-- compare: multiset
SELECT 'Infinity'::float8 + 100.0;

-- id: float8_33_select_infinity_float8_infinity_float8_0f3ac725
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:54
-- compare: multiset
SELECT 'Infinity'::float8 / 'Infinity'::float8;

-- id: float8_34_select_42_float8_infinity_float8_b3b7e2db
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:55
-- compare: multiset
SELECT '42'::float8 / 'Infinity'::float8;

-- id: float8_35_select_nan_float8_nan_float8_f39c55fc
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:56
-- compare: multiset
SELECT 'nan'::float8 / 'nan'::float8;

-- id: float8_36_select_nan_float8_0_float8_ff032c80
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:57
-- compare: multiset
SELECT 'nan'::float8 / '0'::float8;

-- id: float8_38_select_from_float8_tbl_c665b37c
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:59
-- compare: multiset
SELECT * FROM FLOAT8_TBL;

-- id: float8_39_select_f_from_float8_tbl_f_where_f_f1_1004_3_d4bd416a
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:61
-- compare: multiset
SELECT f.* FROM FLOAT8_TBL f WHERE f.f1 <> '1004.3';

-- id: float8_40_select_f_from_float8_tbl_f_where_f_f1_1004_3_a339ad85
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:63
-- compare: multiset
SELECT f.* FROM FLOAT8_TBL f WHERE f.f1 = '1004.3';

-- id: float8_51_select_f_f1_trunc_f_f1_as_trunc_f1_from_float8_tbl_f_6673abec
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:96
-- compare: multiset
SELECT f.f1, trunc(f.f1) AS trunc_f1
   FROM FLOAT8_TBL f;

-- id: float8_52_select_f_f1_round_f_f1_as_round_f1_from_float8_tbl_f_c46efb25
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:100
-- compare: multiset
SELECT f.f1, round(f.f1) AS round_f1
   FROM FLOAT8_TBL f;

-- id: float8_53_select_ceil_f1_as_ceil_f1_from_float8_tbl_f_3f721fc3
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:104
-- compare: multiset
select ceil(f1) as ceil_f1 from float8_tbl f;

-- id: float8_55_select_floor_f1_as_floor_f1_from_float8_tbl_f_ea8d0c24
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:108
-- compare: multiset
select floor(f1) as floor_f1 from float8_tbl f;

-- id: float8_58_select_sqrt_float8_64_as_eight_fe6fe106
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:117
-- compare: multiset
SELECT sqrt(float8 '64') AS eight;

-- id: float8_61_select_power_float8_144_float8_0_5_85d2a281
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:126
-- compare: multiset
SELECT power(float8 '144', float8 '0.5');

-- id: float8_62_select_power_float8_nan_float8_0_5_fe325c62
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:129
-- compare: multiset
SELECT power(float8 'NaN', float8 '0.5');

-- id: float8_63_select_power_float8_144_float8_nan_73ebfb18
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:130
-- compare: multiset
SELECT power(float8 '144', float8 'NaN');

-- id: float8_64_select_power_float8_nan_float8_nan_fe72ce20
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:131
-- compare: multiset
SELECT power(float8 'NaN', float8 'NaN');

-- id: float8_65_select_power_float8_1_float8_nan_35c37b31
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:132
-- compare: multiset
SELECT power(float8 '-1', float8 'NaN');

-- id: float8_66_select_power_float8_1_float8_nan_f5d3139d
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:133
-- compare: multiset
SELECT power(float8 '1', float8 'NaN');

-- id: float8_67_select_power_float8_nan_float8_0_02633313
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:134
-- compare: multiset
SELECT power(float8 'NaN', float8 '0');

-- id: float8_68_select_power_float8_inf_float8_0_4b55c530
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:135
-- compare: multiset
SELECT power(float8 'inf', float8 '0');

-- id: float8_69_select_power_float8_inf_float8_0_a2c8475e
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:136
-- compare: multiset
SELECT power(float8 '-inf', float8 '0');

-- id: float8_70_select_power_float8_0_float8_inf_84d86e39
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:137
-- compare: multiset
SELECT power(float8 '0', float8 'inf');

-- id: float8_72_select_power_float8_1_float8_inf_32805e97
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:139
-- compare: multiset
SELECT power(float8 '1', float8 'inf');

-- id: float8_73_select_power_float8_1_float8_inf_59b6520f
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:140
-- compare: multiset
SELECT power(float8 '1', float8 '-inf');

-- id: float8_74_select_power_float8_1_float8_inf_3416738a
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:141
-- compare: multiset
SELECT power(float8 '-1', float8 'inf');

-- id: float8_75_select_power_float8_1_float8_inf_ed35ed4b
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:142
-- compare: multiset
SELECT power(float8 '-1', float8 '-inf');

-- id: float8_76_select_power_float8_0_1_float8_inf_2357f7b6
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:143
-- compare: multiset
SELECT power(float8 '0.1', float8 'inf');

-- id: float8_77_select_power_float8_0_1_float8_inf_79f539e8
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:144
-- compare: multiset
SELECT power(float8 '-0.1', float8 'inf');

-- id: float8_78_select_power_float8_1_1_float8_inf_22d7cb64
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:145
-- compare: multiset
SELECT power(float8 '1.1', float8 'inf');

-- id: float8_79_select_power_float8_1_1_float8_inf_02b56222
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:146
-- compare: multiset
SELECT power(float8 '-1.1', float8 'inf');

-- id: float8_80_select_power_float8_0_1_float8_inf_23eedb09
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:147
-- compare: multiset
SELECT power(float8 '0.1', float8 '-inf');

-- id: float8_81_select_power_float8_0_1_float8_inf_e4d900ae
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:148
-- compare: multiset
SELECT power(float8 '-0.1', float8 '-inf');

-- id: float8_82_select_power_float8_1_1_float8_inf_cfaa80b2
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:149
-- compare: multiset
SELECT power(float8 '1.1', float8 '-inf');

-- id: float8_83_select_power_float8_1_1_float8_inf_1cf76632
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:150
-- compare: multiset
SELECT power(float8 '-1.1', float8 '-inf');

-- id: float8_84_select_power_float8_inf_float8_2_4e1fb9df
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:151
-- compare: multiset
SELECT power(float8 'inf', float8 '-2');

-- id: float8_85_select_power_float8_inf_float8_2_d4f517b0
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:152
-- compare: multiset
SELECT power(float8 'inf', float8 '2');

-- id: float8_86_select_power_float8_inf_float8_inf_d67fe9b6
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:153
-- compare: multiset
SELECT power(float8 'inf', float8 'inf');

-- id: float8_87_select_power_float8_inf_float8_inf_1678935c
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:154
-- compare: multiset
SELECT power(float8 'inf', float8 '-inf');

-- id: float8_89_select_power_float8_inf_float8_3_80c0424a
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:158
-- compare: multiset
SELECT power(float8 '-inf', float8 '-3');

-- id: float8_90_select_power_float8_inf_float8_2_d5df9a46
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:159
-- compare: multiset
SELECT power(float8 '-inf', float8 '2');

-- id: float8_91_select_power_float8_inf_float8_3_392278a1
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:160
-- compare: multiset
SELECT power(float8 '-inf', float8 '3');

-- id: float8_93_select_power_float8_inf_float8_inf_6dddc0ee
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:162
-- compare: multiset
SELECT power(float8 '-inf', float8 'inf');

-- id: float8_94_select_power_float8_inf_float8_inf_48eae226
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:163
-- compare: multiset
SELECT power(float8 '-inf', float8 '-inf');

-- id: float8_95_select_f_f1_exp_ln_f_f1_as_exp_ln_f1_from_float8_tbl_f_where_f_f1_0_0_fb42dc90
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:164
-- compare: multiset
SELECT f.f1, exp(ln(f.f1)) AS exp_ln_f1
   FROM FLOAT8_TBL f
   WHERE f.f1 > '0.0';

-- id: float8_109_select_sinh_float8_1_95d9fc6d
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:200
-- compare: multiset
SELECT sinh(float8 '1');

-- id: float8_110_select_cosh_float8_1_b87c5302
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:205
-- compare: multiset
SELECT cosh(float8 '1');

-- id: float8_111_select_tanh_float8_1_a4d96c14
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:206
-- compare: multiset
SELECT tanh(float8 '1');

-- id: float8_112_select_asinh_float8_1_5911dac9
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:207
-- compare: multiset
SELECT asinh(float8 '1');

-- id: float8_113_select_acosh_float8_2_79ab3529
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:208
-- compare: multiset
SELECT acosh(float8 '2');

-- id: float8_114_select_atanh_float8_0_5_9a654734
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:209
-- compare: multiset
SELECT atanh(float8 '0.5');

-- id: float8_115_select_sinh_float8_infinity_8e13e7a9
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:210
-- compare: multiset
SELECT sinh(float8 'infinity');

-- id: float8_116_select_sinh_float8_infinity_7127c24b
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:212
-- compare: multiset
SELECT sinh(float8 '-infinity');

-- id: float8_117_select_sinh_float8_nan_51c497e1
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:213
-- compare: multiset
SELECT sinh(float8 'nan');

-- id: float8_118_select_cosh_float8_infinity_6cd45904
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:214
-- compare: multiset
SELECT cosh(float8 'infinity');

-- id: float8_119_select_cosh_float8_infinity_777fc968
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:215
-- compare: multiset
SELECT cosh(float8 '-infinity');

-- id: float8_120_select_cosh_float8_nan_fe046d74
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:216
-- compare: multiset
SELECT cosh(float8 'nan');

-- id: float8_121_select_tanh_float8_infinity_16b9f1a4
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:217
-- compare: multiset
SELECT tanh(float8 'infinity');

-- id: float8_122_select_tanh_float8_infinity_91d161bd
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:218
-- compare: multiset
SELECT tanh(float8 '-infinity');

-- id: float8_123_select_tanh_float8_nan_74b926bd
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:219
-- compare: multiset
SELECT tanh(float8 'nan');

-- id: float8_124_select_asinh_float8_infinity_39fd09a5
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:220
-- compare: multiset
SELECT asinh(float8 'infinity');

-- id: float8_125_select_asinh_float8_infinity_37cdedfb
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:221
-- compare: multiset
SELECT asinh(float8 '-infinity');

-- id: float8_126_select_asinh_float8_nan_80070516
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:222
-- compare: multiset
SELECT asinh(float8 'nan');

-- id: float8_128_select_acosh_float8_nan_cdfa5daf
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:226
-- compare: multiset
SELECT acosh(float8 'nan');

-- id: float8_131_select_atanh_float8_nan_48a80f6a
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:229
-- compare: multiset
SELECT atanh(float8 'nan');

-- id: float8_141_select_32767_4_float8_int2_27104b16
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:261
-- compare: multiset
SELECT '32767.4'::float8::int2;

-- id: float8_143_select_32768_4_float8_int2_ba391fdc
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:265
-- compare: multiset
SELECT '-32768.4'::float8::int2;

-- id: float8_145_select_2147483647_4_float8_int4_78048bb1
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:267
-- compare: multiset
SELECT '2147483647.4'::float8::int4;

-- id: float8_147_select_2147483648_4_float8_int4_9537ec97
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:269
-- compare: multiset
SELECT '-2147483648.4'::float8::int4;

-- id: float8_149_select_9223372036854773760_float8_int8_c01eca39
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:271
-- compare: multiset
SELECT '9223372036854773760'::float8::int8;

-- id: float8_151_select_9223372036854775808_5_float8_int8_0de3882a
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:273
-- compare: multiset
SELECT '-9223372036854775808.5'::float8::int8;

-- id: groupingsets_56_select_select_select_grouping_c_from_values_1_v2_c_group_by_c_from_value_67aa1978
-- origin: postgres REL_17_STABLE src/test/regress/sql/groupingsets.sql:236
-- compare: multiset
select(select (select grouping(c) from (values (1)) v2(c) GROUP BY c) from (values (1,2)) v1(a,b) group by (a,b)) from (values(6,7)) v3(e,f) GROUP BY ROLLUP(e,f);

-- id: groupingsets_63_select_a_b_sum_c_from_values_1_1_10_1_1_11_1_2_12_1_2_13_1_3_14_2_3_15_3_85f584ec
-- origin: postgres REL_17_STABLE src/test/regress/sql/groupingsets.sql:249
-- compare: multiset
select a, b, sum(c) from (values (1,1,10),(1,1,11),(1,2,12),(1,2,13),(1,3,14),(2,3,15),(3,3,16),(3,4,17),(4,1,18),(4,1,19)) v(a,b,c) group by rollup (a,b);

-- id: groupingsets_76_select_ten_grouping_ten_from_onek_group_by_rollup_ten_having_grouping_te_d358cbc3
-- origin: postgres REL_17_STABLE src/test/regress/sql/groupingsets.sql:288
-- compare: ordered
select ten, grouping(ten) from onek
group by rollup(ten) having grouping(ten) > 0
order by 2,1;

-- id: groupingsets_77_select_ten_grouping_ten_from_onek_group_by_cube_ten_having_grouping_ten__c166cb74
-- origin: postgres REL_17_STABLE src/test/regress/sql/groupingsets.sql:291
-- compare: ordered
select ten, grouping(ten) from onek
group by cube(ten) having grouping(ten) > 0
order by 2,1;

-- id: groupingsets_78_select_ten_grouping_ten_from_onek_group_by_ten_having_grouping_ten_0_ord_a2d52428
-- origin: postgres REL_17_STABLE src/test/regress/sql/groupingsets.sql:294
-- compare: ordered
select ten, grouping(ten) from onek
group by (ten) having grouping(ten) >= 0
order by 2,1;

-- id: local_pg_grouping_null_distinguishes_data_null_from_rollup
-- origin: local pg_fusion GROUPING compatibility
-- compare: ordered
select a, grouping(a), count(*)
from (values (null::int)) v(a)
group by grouping sets ((a), ())
order by grouping(a), a nulls first;

-- id: local_pg_grouping_multi_arg_null_distinguishes_data_null_from_rollup
-- origin: local pg_fusion GROUPING compatibility
-- compare: ordered
select a, b, grouping(a,b), count(*)
from (values (null::int, 1), (1, null::int)) v(a,b)
group by cube(a,b)
order by grouping(a,b), a nulls first, b nulls first;

-- id: groupingsets_168_select_distinct_a_b_c_from_values_1_2_3_4_null_6_7_8_9_as_t_a_b_c_group__56ffa31d
-- origin: postgres REL_17_STABLE src/test/regress/sql/groupingsets.sql:575
-- compare: ordered
select distinct a, b, c
from (values (1, 2, 3), (4, null, 6), (7, 8, 9)) as t (a, b, c)
group by rollup(a, b), rollup(a, c)
order by a, b, c;

-- id: int2_9_select_from_int2_tbl_94947b49
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:15
-- compare: multiset
SELECT * FROM INT2_TBL;

-- id: int2_19_select_i_from_int2_tbl_i_where_i_f1_int2_0_866b135a
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:33
-- compare: multiset
SELECT i.* FROM INT2_TBL i WHERE i.f1 <> int2 '0';

-- id: int2_20_select_i_from_int2_tbl_i_where_i_f1_int4_0_406a4ccb
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:35
-- compare: multiset
SELECT i.* FROM INT2_TBL i WHERE i.f1 <> int4 '0';

-- id: int2_21_select_i_from_int2_tbl_i_where_i_f1_int2_0_859c1d61
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:37
-- compare: multiset
SELECT i.* FROM INT2_TBL i WHERE i.f1 = int2 '0';

-- id: int2_22_select_i_from_int2_tbl_i_where_i_f1_int4_0_f8155957
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:39
-- compare: multiset
SELECT i.* FROM INT2_TBL i WHERE i.f1 = int4 '0';

-- id: int2_23_select_i_from_int2_tbl_i_where_i_f1_int2_0_6436acc7
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:41
-- compare: multiset
SELECT i.* FROM INT2_TBL i WHERE i.f1 < int2 '0';

-- id: int2_24_select_i_from_int2_tbl_i_where_i_f1_int4_0_b01ec332
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:43
-- compare: multiset
SELECT i.* FROM INT2_TBL i WHERE i.f1 < int4 '0';

-- id: int2_25_select_i_from_int2_tbl_i_where_i_f1_int2_0_679d3094
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:45
-- compare: multiset
SELECT i.* FROM INT2_TBL i WHERE i.f1 <= int2 '0';

-- id: int2_26_select_i_from_int2_tbl_i_where_i_f1_int4_0_4d996423
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:47
-- compare: multiset
SELECT i.* FROM INT2_TBL i WHERE i.f1 <= int4 '0';

-- id: int2_27_select_i_from_int2_tbl_i_where_i_f1_int2_0_88367505
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:49
-- compare: multiset
SELECT i.* FROM INT2_TBL i WHERE i.f1 > int2 '0';

-- id: int2_28_select_i_from_int2_tbl_i_where_i_f1_int4_0_6e0df375
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:51
-- compare: multiset
SELECT i.* FROM INT2_TBL i WHERE i.f1 > int4 '0';

-- id: int2_29_select_i_from_int2_tbl_i_where_i_f1_int2_0_b1f86c5c
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:53
-- compare: multiset
SELECT i.* FROM INT2_TBL i WHERE i.f1 >= int2 '0';

-- id: int2_30_select_i_from_int2_tbl_i_where_i_f1_int4_0_6c417f83
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:55
-- compare: multiset
SELECT i.* FROM INT2_TBL i WHERE i.f1 >= int4 '0';

-- id: int2_31_select_i_from_int2_tbl_i_where_i_f1_int2_2_int2_1_2814ea4e
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:57
-- compare: multiset
SELECT i.* FROM INT2_TBL i WHERE (i.f1 % int2 '2') = int2 '1';

-- id: int2_32_select_i_from_int2_tbl_i_where_i_f1_int4_2_int2_0_92ec6ff1
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:60
-- compare: multiset
SELECT i.* FROM INT2_TBL i WHERE (i.f1 % int4 '2') = int2 '0';

-- id: int2_34_select_i_f1_i_f1_int2_2_as_x_from_int2_tbl_i_where_abs_f1_16384_a41ce30f
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:65
-- compare: multiset
SELECT i.f1, i.f1 * int2 '2' AS x FROM INT2_TBL i
WHERE abs(f1) < 16384;

-- id: int2_35_select_i_f1_i_f1_int4_2_as_x_from_int2_tbl_i_a7024c86
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:68
-- compare: multiset
SELECT i.f1, i.f1 * int4 '2' AS x FROM INT2_TBL i;

-- id: int2_37_select_i_f1_i_f1_int2_2_as_x_from_int2_tbl_i_where_f1_32766_0bc376e1
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:72
-- compare: multiset
SELECT i.f1, i.f1 + int2 '2' AS x FROM INT2_TBL i
WHERE f1 < 32766;

-- id: int2_38_select_i_f1_i_f1_int4_2_as_x_from_int2_tbl_i_06417d7a
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:75
-- compare: multiset
SELECT i.f1, i.f1 + int4 '2' AS x FROM INT2_TBL i;

-- id: int2_40_select_i_f1_i_f1_int2_2_as_x_from_int2_tbl_i_where_f1_32767_73d30c8c
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:79
-- compare: multiset
SELECT i.f1, i.f1 - int2 '2' AS x FROM INT2_TBL i
WHERE f1 > -32767;

-- id: int2_41_select_i_f1_i_f1_int4_2_as_x_from_int2_tbl_i_13fbbc94
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:82
-- compare: multiset
SELECT i.f1, i.f1 - int4 '2' AS x FROM INT2_TBL i;

-- id: int2_42_select_i_f1_i_f1_int2_2_as_x_from_int2_tbl_i_ac35b0cb
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:84
-- compare: multiset
SELECT i.f1, i.f1 / int2 '2' AS x FROM INT2_TBL i;

-- id: int2_43_select_i_f1_i_f1_int4_2_as_x_from_int2_tbl_i_0a1e69c8
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:86
-- compare: multiset
SELECT i.f1, i.f1 / int4 '2' AS x FROM INT2_TBL i;

-- id: int2_44_select_1_int2_15_text_242a83af
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:88
-- compare: multiset
SELECT (-1::int2<<15)::text;

-- id: int2_45_select_1_int2_15_1_int2_text_5bbf55c8
-- origin: postgres REL_17_STABLE src/test/regress/sql/int2.sql:91
-- compare: multiset
SELECT ((-1::int2<<15)+1::int2)::text;

-- id: int4_9_select_from_int4_tbl_1ad613aa
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:15
-- compare: multiset
SELECT * FROM INT4_TBL;

-- id: int4_14_select_i_from_int4_tbl_i_where_i_f1_int2_0_62b49121
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:24
-- compare: multiset
SELECT i.* FROM INT4_TBL i WHERE i.f1 <> int2 '0';

-- id: int4_15_select_i_from_int4_tbl_i_where_i_f1_int4_0_f578bf07
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:26
-- compare: multiset
SELECT i.* FROM INT4_TBL i WHERE i.f1 <> int4 '0';

-- id: int4_16_select_i_from_int4_tbl_i_where_i_f1_int2_0_3d11842a
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:28
-- compare: multiset
SELECT i.* FROM INT4_TBL i WHERE i.f1 = int2 '0';

-- id: int4_17_select_i_from_int4_tbl_i_where_i_f1_int4_0_4e9b3fe9
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:30
-- compare: multiset
SELECT i.* FROM INT4_TBL i WHERE i.f1 = int4 '0';

-- id: int4_18_select_i_from_int4_tbl_i_where_i_f1_int2_0_69a5977a
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:32
-- compare: multiset
SELECT i.* FROM INT4_TBL i WHERE i.f1 < int2 '0';

-- id: int4_19_select_i_from_int4_tbl_i_where_i_f1_int4_0_db621980
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:34
-- compare: multiset
SELECT i.* FROM INT4_TBL i WHERE i.f1 < int4 '0';

-- id: int4_20_select_i_from_int4_tbl_i_where_i_f1_int2_0_453997ec
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:36
-- compare: multiset
SELECT i.* FROM INT4_TBL i WHERE i.f1 <= int2 '0';

-- id: int4_21_select_i_from_int4_tbl_i_where_i_f1_int4_0_ea178e23
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:38
-- compare: multiset
SELECT i.* FROM INT4_TBL i WHERE i.f1 <= int4 '0';

-- id: int4_22_select_i_from_int4_tbl_i_where_i_f1_int2_0_950134b0
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:40
-- compare: multiset
SELECT i.* FROM INT4_TBL i WHERE i.f1 > int2 '0';

-- id: int4_23_select_i_from_int4_tbl_i_where_i_f1_int4_0_9d24c2fa
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:42
-- compare: multiset
SELECT i.* FROM INT4_TBL i WHERE i.f1 > int4 '0';

-- id: int4_24_select_i_from_int4_tbl_i_where_i_f1_int2_0_0a8dbb5b
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:44
-- compare: multiset
SELECT i.* FROM INT4_TBL i WHERE i.f1 >= int2 '0';

-- id: int4_25_select_i_from_int4_tbl_i_where_i_f1_int4_0_88950baf
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:46
-- compare: multiset
SELECT i.* FROM INT4_TBL i WHERE i.f1 >= int4 '0';

-- id: int4_26_select_i_from_int4_tbl_i_where_i_f1_int2_2_int2_1_fc2ad549
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:48
-- compare: multiset
SELECT i.* FROM INT4_TBL i WHERE (i.f1 % int2 '2') = int2 '1';

-- id: int4_27_select_i_from_int4_tbl_i_where_i_f1_int4_2_int2_0_7fcb1ba0
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:51
-- compare: multiset
SELECT i.* FROM INT4_TBL i WHERE (i.f1 % int4 '2') = int2 '0';

-- id: int4_29_select_i_f1_i_f1_int2_2_as_x_from_int4_tbl_i_where_abs_f1_1073741824_496020c3
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:56
-- compare: multiset
SELECT i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

-- id: int4_31_select_i_f1_i_f1_int4_2_as_x_from_int4_tbl_i_where_abs_f1_1073741824_b5de9842
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:61
-- compare: multiset
SELECT i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

-- id: int4_33_select_i_f1_i_f1_int2_2_as_x_from_int4_tbl_i_where_f1_2147483646_720125b9
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:66
-- compare: multiset
SELECT i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

-- id: int4_35_select_i_f1_i_f1_int4_2_as_x_from_int4_tbl_i_where_f1_2147483646_c3277cb7
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:71
-- compare: multiset
SELECT i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

-- id: int4_37_select_i_f1_i_f1_int2_2_as_x_from_int4_tbl_i_where_f1_2147483647_3e6f368a
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:76
-- compare: multiset
SELECT i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

-- id: int4_39_select_i_f1_i_f1_int4_2_as_x_from_int4_tbl_i_where_f1_2147483647_bdd95a67
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:81
-- compare: multiset
SELECT i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

-- id: int4_40_select_i_f1_i_f1_int2_2_as_x_from_int4_tbl_i_3fd7ee7e
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:84
-- compare: multiset
SELECT i.f1, i.f1 / int2 '2' AS x FROM INT4_TBL i;

-- id: int4_41_select_i_f1_i_f1_int4_2_as_x_from_int4_tbl_i_7b2d8b0e
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:86
-- compare: multiset
SELECT i.f1, i.f1 / int4 '2' AS x FROM INT4_TBL i;

-- id: int4_42_select_2_3_as_one_ccc40b8b
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:88
-- compare: multiset
SELECT -2+3 AS one;

-- id: int4_43_select_4_2_as_two_329242d5
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:95
-- compare: multiset
SELECT 4-2 AS two;

-- id: int4_44_select_2_1_as_three_d4ae6726
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:97
-- compare: multiset
SELECT 2- -1 AS three;

-- id: int4_45_select_2_2_as_four_1a984764
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:99
-- compare: multiset
SELECT 2 - -2 AS four;

-- id: int4_46_select_int2_2_int2_2_int2_16_int2_4_as_true_2edc6601
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:101
-- compare: multiset
SELECT int2 '2' * int2 '2' = int2 '16' / int2 '4' AS true;

-- id: int4_47_select_int4_2_int2_2_int2_16_int4_4_as_true_86e36681
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:103
-- compare: multiset
SELECT int4 '2' * int2 '2' = int2 '16' / int4 '4' AS true;

-- id: int4_48_select_int2_2_int4_2_int4_16_int2_4_as_true_1c236df8
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:105
-- compare: multiset
SELECT int2 '2' * int4 '2' = int4 '16' / int2 '4' AS true;

-- id: int4_49_select_int4_1000_int4_999_as_false_4798019e
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:107
-- compare: multiset
SELECT int4 '1000' < int4 '999' AS false;

-- id: int4_50_select_1_1_1_1_1_1_1_1_1_1_as_ten_c089dfcd
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:109
-- compare: multiset
SELECT 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 AS ten;

-- id: int4_51_select_2_2_2_as_three_eff47476
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:111
-- compare: multiset
SELECT 2 + 2 / 2 AS three;

-- id: int4_52_select_2_2_2_as_two_602968fb
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:113
-- compare: multiset
SELECT (2 + 2) / 2 AS two;

-- id: int4_53_select_1_int4_31_text_689d832b
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:115
-- compare: multiset
SELECT (-1::int4<<31)::text;

-- id: int4_54_select_1_int4_31_1_text_9ef12fe0
-- origin: postgres REL_17_STABLE src/test/regress/sql/int4.sql:118
-- compare: multiset
SELECT ((-1::int4<<31)+1)::text;

-- id: int8_8_select_from_int8_tbl_3a8e627d
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:15
-- compare: multiset
SELECT * FROM INT8_TBL;

-- id: int8_13_select_from_int8_tbl_where_q2_4567890123456789_83ea2f82
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:23
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 = 4567890123456789;

-- id: int8_14_select_from_int8_tbl_where_q2_4567890123456789_1fd0ef6a
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:26
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 <> 4567890123456789;

-- id: int8_15_select_from_int8_tbl_where_q2_4567890123456789_e8fbc413
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:27
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 < 4567890123456789;

-- id: int8_16_select_from_int8_tbl_where_q2_4567890123456789_9d2aab3c
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:28
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 > 4567890123456789;

-- id: int8_17_select_from_int8_tbl_where_q2_4567890123456789_1b85a880
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:29
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 <= 4567890123456789;

-- id: int8_18_select_from_int8_tbl_where_q2_4567890123456789_69ba3d50
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:30
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 >= 4567890123456789;

-- id: int8_19_select_from_int8_tbl_where_q2_456_a3689b5a
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:31
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 = 456;

-- id: int8_20_select_from_int8_tbl_where_q2_456_2bf3671e
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:34
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 <> 456;

-- id: int8_21_select_from_int8_tbl_where_q2_456_ae76d0d6
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:35
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 < 456;

-- id: int8_22_select_from_int8_tbl_where_q2_456_159f64c1
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:36
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 > 456;

-- id: int8_23_select_from_int8_tbl_where_q2_456_a1770cbe
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:37
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 <= 456;

-- id: int8_24_select_from_int8_tbl_where_q2_456_71808b1e
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:38
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 >= 456;

-- id: int8_25_select_from_int8_tbl_where_123_q1_7934c423
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:39
-- compare: multiset
SELECT * FROM INT8_TBL WHERE 123 = q1;

-- id: int8_26_select_from_int8_tbl_where_123_q1_7e836aa5
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:42
-- compare: multiset
SELECT * FROM INT8_TBL WHERE 123 <> q1;

-- id: int8_27_select_from_int8_tbl_where_123_q1_a048c924
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:43
-- compare: multiset
SELECT * FROM INT8_TBL WHERE 123 < q1;

-- id: int8_28_select_from_int8_tbl_where_123_q1_4d654a1a
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:44
-- compare: multiset
SELECT * FROM INT8_TBL WHERE 123 > q1;

-- id: int8_29_select_from_int8_tbl_where_123_q1_604664e3
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:45
-- compare: multiset
SELECT * FROM INT8_TBL WHERE 123 <= q1;

-- id: int8_30_select_from_int8_tbl_where_123_q1_1845b67e
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:46
-- compare: multiset
SELECT * FROM INT8_TBL WHERE 123 >= q1;

-- id: int8_31_select_from_int8_tbl_where_q2_456_int2_f6cf4d78
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:47
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 = '456'::int2;

-- id: int8_32_select_from_int8_tbl_where_q2_456_int2_d91b1284
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:50
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 <> '456'::int2;

-- id: int8_33_select_from_int8_tbl_where_q2_456_int2_6c1a4955
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:51
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 < '456'::int2;

-- id: int8_34_select_from_int8_tbl_where_q2_456_int2_2bfbded5
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:52
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 > '456'::int2;

-- id: int8_35_select_from_int8_tbl_where_q2_456_int2_d1ed916e
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:53
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 <= '456'::int2;

-- id: int8_36_select_from_int8_tbl_where_q2_456_int2_042cc867
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:54
-- compare: multiset
SELECT * FROM INT8_TBL WHERE q2 >= '456'::int2;

-- id: int8_37_select_from_int8_tbl_where_123_int2_q1_6c76830a
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:55
-- compare: multiset
SELECT * FROM INT8_TBL WHERE '123'::int2 = q1;

-- id: int8_38_select_from_int8_tbl_where_123_int2_q1_5f1a92ad
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:58
-- compare: multiset
SELECT * FROM INT8_TBL WHERE '123'::int2 <> q1;

-- id: int8_39_select_from_int8_tbl_where_123_int2_q1_3c829fc4
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:59
-- compare: multiset
SELECT * FROM INT8_TBL WHERE '123'::int2 < q1;

-- id: int8_40_select_from_int8_tbl_where_123_int2_q1_b0589790
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:60
-- compare: multiset
SELECT * FROM INT8_TBL WHERE '123'::int2 > q1;

-- id: int8_41_select_from_int8_tbl_where_123_int2_q1_ec3da00f
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:61
-- compare: multiset
SELECT * FROM INT8_TBL WHERE '123'::int2 <= q1;

-- id: int8_42_select_from_int8_tbl_where_123_int2_q1_89265f40
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:62
-- compare: multiset
SELECT * FROM INT8_TBL WHERE '123'::int2 >= q1;

-- id: int8_43_select_q1_as_plus_q1_as_minus_from_int8_tbl_7e0d4cb0
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:63
-- compare: multiset
SELECT q1 AS plus, -q1 AS minus FROM INT8_TBL;

-- id: int8_44_select_q1_q2_q1_q2_as_plus_from_int8_tbl_34467187
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:66
-- compare: multiset
SELECT q1, q2, q1 + q2 AS plus FROM INT8_TBL;

-- id: int8_45_select_q1_q2_q1_q2_as_minus_from_int8_tbl_5184ee2a
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:68
-- compare: multiset
SELECT q1, q2, q1 - q2 AS minus FROM INT8_TBL;

-- id: int8_47_select_q1_q2_q1_q2_as_multiply_from_int8_tbl_where_q1_1000_or_q2_0_and_q_5765fa31
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:70
-- compare: multiset
SELECT q1, q2, q1 * q2 AS multiply FROM INT8_TBL
 WHERE q1 < 1000 or (q2 > 0 and q2 < 1000);

-- id: int8_48_select_q1_q2_q1_q2_as_divide_q1_q2_as_mod_from_int8_tbl_23f07236
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:72
-- compare: multiset
SELECT q1, q2, q1 / q2 AS divide, q1 % q2 AS mod FROM INT8_TBL;

-- id: int8_51_select_37_q1_as_plus4_from_int8_tbl_e3d10d40
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:76
-- compare: multiset
SELECT 37 + q1 AS plus4 FROM INT8_TBL;

-- id: int8_52_select_37_q1_as_minus4_from_int8_tbl_cbd58658
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:78
-- compare: multiset
SELECT 37 - q1 AS minus4 FROM INT8_TBL;

-- id: int8_53_select_2_q1_as_twice_int4_from_int8_tbl_840ce44f
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:79
-- compare: multiset
SELECT 2 * q1 AS "twice int4" FROM INT8_TBL;

-- id: int8_54_select_q1_2_as_twice_int4_from_int8_tbl_e7465b0c
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:80
-- compare: multiset
SELECT q1 * 2 AS "twice int4" FROM INT8_TBL;

-- id: int8_55_select_q1_42_int4_as_8plus4_q1_42_int4_as_8minus4_q1_42_int4_as_8mul4_q1_7d54392a
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:81
-- compare: multiset
SELECT q1 + 42::int4 AS "8plus4", q1 - 42::int4 AS "8minus4", q1 * 42::int4 AS "8mul4", q1 / 42::int4 AS "8div4" FROM INT8_TBL;

-- id: int8_56_select_246_int4_q1_as_4plus8_246_int4_q1_as_4minus8_246_int4_q1_as_4mul8_41885e9e
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:84
-- compare: multiset
SELECT 246::int4 + q1 AS "4plus8", 246::int4 - q1 AS "4minus8", 246::int4 * q1 AS "4mul8", 246::int4 / q1 AS "4div8" FROM INT8_TBL;

-- id: int8_57_select_q1_42_int2_as_8plus2_q1_42_int2_as_8minus2_q1_42_int2_as_8mul2_q1_e487d090
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:86
-- compare: multiset
SELECT q1 + 42::int2 AS "8plus2", q1 - 42::int2 AS "8minus2", q1 * 42::int2 AS "8mul2", q1 / 42::int2 AS "8div2" FROM INT8_TBL;

-- id: int8_58_select_246_int2_q1_as_2plus8_246_int2_q1_as_2minus8_246_int2_q1_as_2mul8_bb87cc05
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:89
-- compare: multiset
SELECT 246::int2 + q1 AS "2plus8", 246::int2 - q1 AS "2minus8", 246::int2 * q1 AS "2mul8", 246::int2 / q1 AS "2div8" FROM INT8_TBL;

-- id: int8_59_select_q2_abs_q2_from_int8_tbl_39a1b962
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:91
-- compare: multiset
SELECT q2, abs(q2) FROM INT8_TBL;

-- id: int8_79_select_9223372036854775808_int8_c25a3f85
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:124
-- compare: multiset
select '-9223372036854775808'::int8;

-- id: int8_81_select_9223372036854775807_int8_f5e9c6bd
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:129
-- compare: multiset
select '9223372036854775807'::int8;

-- id: int8_83_select_9223372036854775807_int8_9f0a5338
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:131
-- compare: multiset
select -('-9223372036854775807'::int8);

-- id: int8_108_select_cast_q1_as_int4_from_int8_tbl_where_q2_456_5ed4c7a1
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:166
-- compare: multiset
SELECT CAST(q1 AS int4) FROM int8_tbl WHERE q2 = 456;

-- id: int8_110_select_cast_q1_as_int2_from_int8_tbl_where_q2_456_55fd97bc
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:169
-- compare: multiset
SELECT CAST(q1 AS int2) FROM int8_tbl WHERE q2 = 456;

-- id: int8_113_select_cast_q1_as_float4_cast_q2_as_float8_from_int8_tbl_496331b4
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:174
-- compare: multiset
SELECT CAST(q1 AS float4), CAST(q2 AS float8) FROM INT8_TBL;

-- id: int8_114_select_cast_36854775807_0_float4_as_int8_9d65e930
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:176
-- compare: multiset
SELECT CAST('36854775807.0'::float4 AS int8);

-- id: int8_119_select_q1_q1_2_as_shl_q1_3_as_shr_from_int8_tbl_b3624deb
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:186
-- compare: multiset
SELECT q1, q1 << 2 AS "shl", q1 >> 3 AS "shr" FROM INT8_TBL;

-- id: int8_123_select_1_int8_63_text_c6f62980
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:194
-- compare: multiset
SELECT (-1::int8<<63)::text;

-- id: int8_124_select_1_int8_63_1_text_d8c14be1
-- origin: postgres REL_17_STABLE src/test/regress/sql/int8.sql:197
-- compare: multiset
SELECT ((-1::int8<<63)+1)::text;

-- id: join_26_select_from_j1_tbl_as_tx_a0ab30d5
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:43
-- compare: multiset
SELECT *
  FROM J1_TBL AS tx;

-- id: join_27_select_from_j1_tbl_tx_1688e64e
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:53
-- compare: multiset
SELECT *
  FROM J1_TBL tx;

-- id: join_28_select_from_j1_tbl_as_t1_a_b_c_2e6446e3
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:56
-- compare: multiset
SELECT *
  FROM J1_TBL AS t1 (a, b, c);

-- id: join_29_select_from_j1_tbl_t1_a_b_c_5025f9d5
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:59
-- compare: multiset
SELECT *
  FROM J1_TBL t1 (a, b, c);

-- id: join_30_select_from_j1_tbl_t1_a_b_c_j2_tbl_t2_d_e_8ff691e3
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:62
-- compare: multiset
SELECT *
  FROM J1_TBL t1 (a, b, c), J2_TBL t2 (d, e);

-- id: join_31_select_t1_a_t2_e_from_j1_tbl_t1_a_b_c_j2_tbl_t2_d_e_where_t1_a_t2_d_cd724dcc
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:65
-- compare: multiset
SELECT t1.a, t2.e
  FROM J1_TBL t1 (a, b, c), J2_TBL t2 (d, e)
  WHERE t1.a = t2.d;

-- id: join_32_select_from_j1_tbl_cross_join_j2_tbl_4f74b2b4
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:69
-- compare: multiset
SELECT *
  FROM J1_TBL CROSS JOIN J2_TBL;

-- id: join_34_select_t1_i_k_t_from_j1_tbl_t1_cross_join_j2_tbl_t2_64effa78
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:83
-- compare: multiset
SELECT t1.i, k, t
  FROM J1_TBL t1 CROSS JOIN J2_TBL t2;

-- id: join_36_select_tx_ii_tx_jj_tx_kk_from_j1_tbl_t1_a_b_c_cross_join_j2_tbl_t2_d_e_a_eef2ae4c
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:91
-- compare: multiset
SELECT tx.ii, tx.jj, tx.kk
  FROM (J1_TBL t1 (a, b, c) CROSS JOIN J2_TBL t2 (d, e))
    AS tx (ii, jj, tt, ii2, kk);

-- id: join_37_select_from_j1_tbl_cross_join_j2_tbl_a_cross_join_j2_tbl_b_b3f644d7
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:95
-- compare: multiset
SELECT *
  FROM J1_TBL CROSS JOIN J2_TBL a CROSS JOIN J2_TBL b;

-- id: join_38_select_from_j1_tbl_inner_join_j2_tbl_using_i_ad913e41
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:98
-- compare: multiset
SELECT *
  FROM J1_TBL INNER JOIN J2_TBL USING (i);

-- id: join_39_select_from_j1_tbl_join_j2_tbl_using_i_faf4060b
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:115
-- compare: multiset
SELECT *
  FROM J1_TBL JOIN J2_TBL USING (i);

-- id: join_40_select_from_j1_tbl_t1_a_b_c_join_j2_tbl_t2_a_d_using_a_order_by_a_d_80998709
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:119
-- compare: ordered
SELECT *
  FROM J1_TBL t1 (a, b, c) JOIN J2_TBL t2 (a, d) USING (a)
  ORDER BY a, d;

-- id: join_42_select_from_j1_tbl_join_j2_tbl_using_i_where_j1_tbl_t_one_8af0f5e2
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:127
-- compare: multiset
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) WHERE J1_TBL.t = 'one';

-- id: join_52_select_from_j1_tbl_natural_join_j2_tbl_6cfe93e3
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:139
-- compare: multiset
SELECT *
  FROM J1_TBL NATURAL JOIN J2_TBL;

-- id: join_53_select_from_j1_tbl_t1_a_b_c_natural_join_j2_tbl_t2_a_d_7bc88c53
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:147
-- compare: multiset
SELECT *
  FROM J1_TBL t1 (a, b, c) NATURAL JOIN J2_TBL t2 (a, d);

-- id: join_54_select_from_j1_tbl_t1_a_b_c_natural_join_j2_tbl_t2_d_a_d67d102b
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:150
-- compare: multiset
SELECT *
  FROM J1_TBL t1 (a, b, c) NATURAL JOIN J2_TBL t2 (d, a);

-- id: join_56_select_from_j1_tbl_join_j2_tbl_on_j1_tbl_i_j2_tbl_i_8f0f6526
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:158
-- compare: multiset
SELECT *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.i);

-- id: join_57_select_from_j1_tbl_join_j2_tbl_on_j1_tbl_i_j2_tbl_k_6eaf8307
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:166
-- compare: multiset
SELECT *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.k);

-- id: join_58_select_from_j1_tbl_join_j2_tbl_on_j1_tbl_i_j2_tbl_k_2c0527c2
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:169
-- compare: multiset
SELECT *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i <= J2_TBL.k);

-- id: join_65_select_from_j1_tbl_left_join_j2_tbl_using_i_where_k_1_48d82c2f
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:205
-- compare: multiset
SELECT *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (k = 1);

-- id: join_113_select_count_from_tenk1_x_where_x_unique1_in_select_a_f1_from_int4_tbl_a_10c04f4b
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:368
-- compare: multiset
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);

-- id: join_122_select_from_int8_tbl_i1_left_join_int8_tbl_i2_join_select_123_as_x_ss_on_0c9d91b2
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:407
-- compare: ordered
select * from int8_tbl i1 left join (int8_tbl i2 join
  (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
order by 1, 2;

-- id: join_123_select_count_from_select_t3_tenthous_as_x1_coalesce_t1_stringu1_t2_strin_1df01ca7
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:411
-- compare: multiset
select count(*)
from
  (select t3.tenthous as x1, coalesce(t1.stringu1, t2.stringu1) as x2
   from tenk1 t1
   left join tenk1 t2 on t1.unique1 = t2.unique1
   join tenk1 t3 on t1.unique2 = t3.unique2) ss,
  tenk1 t4,
  tenk1 t5
where t4.thousand = t5.unique1 and ss.x1 = t4.tenthous and ss.x2 = t5.stringu1;

-- id: join_125_select_a_f1_b_f1_t_thousand_t_tenthous_from_tenk1_t_select_sum_f1_1_as_f_82f2d06a
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:436
-- compare: multiset
select a.f1, b.f1, t.thousand, t.tenthous from
  tenk1 t,
  (select sum(f1)+1 as f1 from int4_tbl i4a) a,
  (select sum(f1) as f1 from int4_tbl i4b) b
where b.f1 = t.thousand and a.f1 = b.f1 and (a.f1+b.f1+999) = t.tenthous;

-- id: join_145_select_count_from_select_from_tenk1_x_order_by_x_thousand_x_twothousand__81799a0b
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:576
-- compare: ordered
select count(*) from
  (select * from tenk1 x order by x.thousand, x.twothousand, x.fivethous) x
  left join
  (select * from tenk1 y order by y.unique2) y
  on x.thousand = y.unique2 and x.twothousand = y.hundred and x.fivethous = y.unique2;

-- id: join_211_select_count_from_tenk1_a_tenk1_b_where_a_hundred_b_thousand_and_b_fivet_f02d1605
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:728
-- compare: multiset
select count(*) from tenk1 a, tenk1 b
  where a.hundred = b.thousand and (b.fivethous % 10) < 10;

-- id: join_257_select_a_unique2_a_ten_b_tenthous_b_unique2_b_hundred_from_tenk1_a_left__3c713155
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:876
-- compare: multiset
select a.unique2, a.ten, b.tenthous, b.unique2, b.hundred
from tenk1 a left join tenk1 b on a.unique2 = b.tenthous
where a.unique1 = 42 and
      ((b.unique2 is null and a.ten = 2) or b.hundred = 3);

-- id: join_295_select_from_select_1_as_key1_sub1_left_join_select_sub3_key3_sub4_value2_50069e00
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:995
-- compare: multiset
SELECT * FROM
( SELECT 1 as key1 ) sub1
LEFT JOIN
( SELECT sub3.key3, sub4.value2, COALESCE(sub4.value2, 66) as value3 FROM
    ( SELECT 1 as key3 ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT 1 as key5 ) sub5
        LEFT JOIN
        ( SELECT 2 as key6, 42 as value1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

-- id: join_296_select_from_select_1_as_key1_sub1_left_join_select_sub3_key3_value2_coal_832859de
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1015
-- compare: multiset
SELECT * FROM
( SELECT 1 as key1 ) sub1
LEFT JOIN
( SELECT sub3.key3, value2, COALESCE(value2, 66) as value3 FROM
    ( SELECT 1 as key3 ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT 1 as key5 ) sub5
        LEFT JOIN
        ( SELECT 2 as key6, 42 as value1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

-- id: join_317_select_from_int4_tbl_a_full_join_int4_tbl_b_on_true_7fd00ea6
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1159
-- compare: multiset
select * from int4_tbl a full join int4_tbl b on true;

-- id: join_318_select_from_int4_tbl_a_full_join_int4_tbl_b_on_false_8ca4beee
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1164
-- compare: multiset
select * from int4_tbl a full join int4_tbl b on false;

-- id: join_376_select_count_from_tenk1_a_join_tenk1_b_on_a_unique1_b_unique2_left_join__3a43ceaf
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1480
-- compare: multiset
select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

-- id: join_378_select_b_unique1_from_tenk1_a_join_tenk1_b_on_a_unique1_b_unique2_left_j_612ebc48
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1493
-- compare: ordered
select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

-- id: join_380_select_from_select_unique1_q1_coalesce_unique1_1_q1_as_fault_from_int8_t_851783bc
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1509
-- compare: ordered
select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl left join tenk1 on (q2 = unique2)
) ss
where fault = 122
order by fault;

-- id: join_384_select_q1_unique2_thousand_hundred_from_int8_tbl_a_left_join_tenk1_b_on__70de870c
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1537
-- compare: multiset
select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

-- id: join_386_select_f1_unique2_case_when_unique2_is_null_then_f1_else_0_end_from_int4_e55c1071
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1546
-- compare: multiset
select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

-- id: join_388_select_a_unique1_b_unique1_c_unique1_coalesce_b_twothousand_a_twothousan_6487d41e
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1559
-- compare: multiset
select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
  from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
  where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

-- id: join_402_select_from_text_tbl_t1_inner_join_int8_tbl_i8_on_i8_q2_456_right_join_t_e9f3426e
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1712
-- compare: multiset
select * from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1;

-- id: join_420_select_from_select_1_as_id_as_xx_left_join_tenk1_as_a1_full_join_select__aeacbc6a
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1870
-- compare: multiset
select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

-- id: join_426_select_a_q2_b_q1_from_int8_tbl_a_left_join_int8_tbl_b_on_a_q2_coalesce_b_b7c08181
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1900
-- compare: multiset
select a.q2, b.q1
  from int8_tbl a left join int8_tbl b on a.q2 = coalesce(b.q1, 1)
  where coalesce(b.q1, 1) > 0;

-- id: join_432_select_a_unique1_b_unique2_from_onek_a_full_join_onek_b_on_a_unique1_b_u_ce337e07
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1928
-- compare: multiset
select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where a.unique1 = 42;

-- id: join_434_select_a_unique1_b_unique2_from_onek_a_full_join_onek_b_on_a_unique1_b_u_b7c3fa10
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1937
-- compare: multiset
select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where b.unique2 = 43;

-- id: join_436_select_a_unique1_b_unique2_from_onek_a_full_join_onek_b_on_a_unique1_b_u_54a5d6b4
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1946
-- compare: multiset
select a.unique1, b.unique2
  from onek a full join onek b on a.unique1 = b.unique2
  where a.unique1 = 42 and b.unique2 = 42;

-- id: join_438_select_from_select_from_int8_tbl_i81_join_values_123_2_v_v1_v2_on_q2_v1__a0971f46
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1961
-- compare: multiset
select * from
  (select * from int8_tbl i81 join (values(123,2)) v(v1,v2) on q2=v1) ss1
full join
  (select * from (values(456,2)) w(v1,v2) join int8_tbl i82 on q2=v1) ss2
on true;

-- id: join_531_select_from_int8_tbl_x_join_int4_tbl_x_cross_join_int4_tbl_y_ff_j_on_q1__03860621
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:2368
-- compare: multiset
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y(ff)) j on q1 = f1;

-- id: limit_1_select_text_as_two_unique1_unique2_stringu1_from_onek_where_unique1_50_o_72c05f45
-- origin: postgres REL_17_STABLE src/test/regress/sql/limit.sql:1
-- compare: ordered
SELECT ''::text AS two, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 50
		ORDER BY unique1 LIMIT 2;

-- id: limit_2_select_text_as_five_unique1_unique2_stringu1_from_onek_where_unique1_60__425c2e04
-- origin: postgres REL_17_STABLE src/test/regress/sql/limit.sql:8
-- compare: ordered
SELECT ''::text AS five, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 60
		ORDER BY unique1 LIMIT 5;

-- id: limit_3_select_text_as_two_unique1_unique2_stringu1_from_onek_where_unique1_60_a_b6a8635a
-- origin: postgres REL_17_STABLE src/test/regress/sql/limit.sql:11
-- compare: ordered
SELECT ''::text AS two, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 60 AND unique1 < 63
		ORDER BY unique1 LIMIT 5;

-- id: limit_4_select_text_as_three_unique1_unique2_stringu1_from_onek_where_unique1_10_8b1584e8
-- origin: postgres REL_17_STABLE src/test/regress/sql/limit.sql:14
-- compare: ordered
SELECT ''::text AS three, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 100
		ORDER BY unique1 LIMIT 3 OFFSET 20;

-- id: limit_5_select_text_as_zero_unique1_unique2_stringu1_from_onek_where_unique1_50__8dd29240
-- origin: postgres REL_17_STABLE src/test/regress/sql/limit.sql:17
-- compare: ordered
SELECT ''::text AS zero, unique1, unique2, stringu1
		FROM onek WHERE unique1 < 50
		ORDER BY unique1 DESC LIMIT 8 OFFSET 99;

-- id: limit_6_select_text_as_eleven_unique1_unique2_stringu1_from_onek_where_unique1_5_3b95d65b
-- origin: postgres REL_17_STABLE src/test/regress/sql/limit.sql:20
-- compare: ordered
SELECT ''::text AS eleven, unique1, unique2, stringu1
		FROM onek WHERE unique1 < 50
		ORDER BY unique1 DESC LIMIT 20 OFFSET 39;

-- id: limit_7_select_text_as_ten_unique1_unique2_stringu1_from_onek_order_by_unique1_o_11cde0d0
-- origin: postgres REL_17_STABLE src/test/regress/sql/limit.sql:23
-- compare: ordered
SELECT ''::text AS ten, unique1, unique2, stringu1
		FROM onek
		ORDER BY unique1 OFFSET 990;

-- id: limit_8_select_text_as_five_unique1_unique2_stringu1_from_onek_order_by_unique1__025a516c
-- origin: postgres REL_17_STABLE src/test/regress/sql/limit.sql:26
-- compare: ordered
SELECT ''::text AS five, unique1, unique2, stringu1
		FROM onek
		ORDER BY unique1 OFFSET 990 LIMIT 5;

-- id: limit_9_select_text_as_five_unique1_unique2_stringu1_from_onek_order_by_unique1__e3d9aef3
-- origin: postgres REL_17_STABLE src/test/regress/sql/limit.sql:29
-- compare: ordered
SELECT ''::text AS five, unique1, unique2, stringu1
		FROM onek
		ORDER BY unique1 LIMIT 5 OFFSET 900;

-- id: limit_68_select_sum_tenthous_as_s1_sum_tenthous_random_0_as_s2_from_tenk1_group_b_6c182bc9
-- origin: postgres REL_17_STABLE src/test/regress/sql/limit.sql:150
-- compare: ordered
select sum(tenthous) as s1, sum(tenthous) + random()*0 as s2
  from tenk1 group by thousand order by thousand limit 3;

-- id: memoize_53_select_unique1_from_tenk1_t0_where_unique1_3_and_exists_select_1_from_te_df59ea2a
-- origin: postgres REL_17_STABLE src/test/regress/sql/memoize.sql:184
-- compare: ordered
SELECT unique1 FROM tenk1 t0
WHERE unique1 < 3
  AND EXISTS (
	SELECT 1 FROM tenk1 t1
	INNER JOIN tenk1 t2 ON t1.unique1 = t2.hundred
	WHERE t0.ten = t1.twenty AND t0.two <> t2.four OFFSET 0);

-- id: plpgsql_465_select_from_foo_408b0583
-- origin: postgres REL_17_STABLE src/test/regress/sql/plpgsql.sql:2350
-- compare: multiset
select * from foo;

-- id: portals_292_select_stringu1_from_onek_where_stringu1_dzaaaa_c7dd8d0a
-- origin: postgres REL_17_STABLE src/test/regress/sql/portals.sql:518
-- compare: multiset
SELECT stringu1 FROM onek WHERE stringu1 = 'DZAAAA';

-- id: returning_3_select_from_foo_408b0583
-- origin: postgres REL_17_STABLE src/test/regress/sql/returning.sql:11
-- compare: multiset
SELECT * FROM foo;

-- id: rowtypes_62_select_thousand_tenthous_from_tenk1_where_thousand_tenthous_997_5000_ord_a17424f2
-- origin: postgres REL_17_STABLE src/test/regress/sql/rowtypes.sql:132
-- compare: ordered
select thousand, tenthous from tenk1
where (thousand, tenthous) >= (997, 5000)
order by thousand, tenthous;

-- id: rowtypes_64_select_thousand_tenthous_four_from_tenk1_where_thousand_tenthous_four_99_c296d225
-- origin: postgres REL_17_STABLE src/test/regress/sql/rowtypes.sql:141
-- compare: ordered
select thousand, tenthous, four from tenk1
where (thousand, tenthous, four) > (998, 5000, 3)
order by thousand, tenthous;

-- id: rowtypes_66_select_thousand_tenthous_from_tenk1_where_998_5000_thousand_tenthous_ord_2f1e5754
-- origin: postgres REL_17_STABLE src/test/regress/sql/rowtypes.sql:150
-- compare: ordered
select thousand, tenthous from tenk1
where (998, 5000) < (thousand, tenthous)
order by thousand, tenthous;

-- id: rowtypes_68_select_thousand_hundred_from_tenk1_where_998_5000_thousand_hundred_order_6f4c8d6c
-- origin: postgres REL_17_STABLE src/test/regress/sql/rowtypes.sql:159
-- compare: ordered
select thousand, hundred from tenk1
where (998, 5000) < (thousand, hundred)
order by thousand, hundred;

-- id: select_1_select_from_onek_where_onek_unique1_10_order_by_onek_unique1_f07f2ed2
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:1
-- compare: ordered
SELECT * FROM onek
   WHERE onek.unique1 < 10
   ORDER BY onek.unique1;

-- id: select_12_select_onek2_from_onek2_where_onek2_unique1_10_cb8e16ee
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:69
-- compare: multiset
SELECT onek2.* FROM onek2 WHERE onek2.unique1 < 10;

-- id: select_14_select_onek2_unique1_onek2_stringu1_from_onek2_where_onek2_unique1_980_52745b2b
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:81
-- compare: multiset
SELECT onek2.unique1, onek2.stringu1 FROM onek2
   WHERE onek2.unique1 > 980;

-- id: select_23_select_from_onek_values_147_rfaaaa_931_vjaaaa_as_v_i_j_where_onek_unique_9f618416
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:116
-- compare: multiset
select * from onek, (values(147, 'RFAAAA'), (931, 'VJAAAA')) as v (i, j)
    WHERE onek.unique1 = v.i and onek.stringu1 = v.j;

-- id: select_26_values_1_2_3_4_4_7_77_7_743327fa
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:135
-- compare: multiset
VALUES (1,2), (3,4+4), (7,77.7);

-- id: select_33_select_from_foo_order_by_f1_61853b2f
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:157
-- compare: ordered
SELECT * FROM foo ORDER BY f1;

-- id: select_34_select_from_foo_order_by_f1_asc_917b9777
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:159
-- compare: ordered
SELECT * FROM foo ORDER BY f1 ASC;

-- id: select_35_select_from_foo_order_by_f1_nulls_first_2befe27a
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:160
-- compare: ordered
SELECT * FROM foo ORDER BY f1 NULLS FIRST;

-- id: select_36_select_from_foo_order_by_f1_desc_d74b73c1
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:161
-- compare: ordered
SELECT * FROM foo ORDER BY f1 DESC;

-- id: select_37_select_from_foo_order_by_f1_desc_nulls_last_a0057981
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:162
-- compare: ordered
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

-- id: select_57_select_from_onek2_where_unique2_11_and_stringu1_ataaaa_ba310091
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:195
-- compare: multiset
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';

-- id: select_60_select_unique2_from_onek2_where_unique2_11_and_stringu1_ataaaa_96071164
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:201
-- compare: multiset
select unique2 from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';

-- id: select_62_select_from_onek2_where_unique2_11_and_stringu1_b_a360ea3d
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:205
-- compare: multiset
select * from onek2 where unique2 = 11 and stringu1 < 'B';

-- id: select_64_select_unique2_from_onek2_where_unique2_11_and_stringu1_b_1dbde3d3
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:208
-- compare: multiset
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';

-- id: select_66_select_unique2_from_onek2_where_unique2_11_and_stringu1_b_for_update_06254cc7
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:212
-- compare: multiset
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B' for update;

-- id: select_68_select_unique2_from_onek2_where_unique2_11_and_stringu1_c_83d9e1ec
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:216
-- compare: multiset
select unique2 from onek2 where unique2 = 11 and stringu1 < 'C';

-- id: select_74_select_unique1_unique2_from_onek2_where_unique2_11_or_unique1_0_and_stri_7057e761
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:226
-- compare: multiset
select unique1, unique2 from onek2
  where (unique2 = 11 or unique1 = 0) and stringu1 < 'B';

-- id: select_76_select_unique1_unique2_from_onek2_where_unique2_11_and_stringu1_b_or_uni_c8b451a1
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:231
-- compare: multiset
select unique1, unique2 from onek2
  where (unique2 = 11 and stringu1 < 'B') or unique1 = 0;

-- id: select_77_select_1_as_x_order_by_x_c748dc63
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:235
-- compare: ordered
SELECT 1 AS x ORDER BY x;

-- id: select_82_select_from_values_2_null_1_v_k_where_k_k_order_by_k_cc512d37
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:251
-- compare: ordered
select * from (values (2),(null),(1)) v(k) where k = k order by k;

-- id: select_83_select_from_values_2_null_1_v_k_where_k_k_c5c7636c
-- origin: postgres REL_17_STABLE src/test/regress/sql/select.sql:255
-- compare: multiset
select * from (values (2),(null),(1)) v(k) where k = k;

-- id: select_distinct_1_select_distinct_two_from_onek_order_by_1_7d5dcc84
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_distinct.sql:1
-- compare: ordered
SELECT DISTINCT two FROM onek ORDER BY 1;

-- id: select_distinct_2_select_distinct_ten_from_onek_order_by_1_13162e03
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_distinct.sql:8
-- compare: ordered
SELECT DISTINCT ten FROM onek ORDER BY 1;

-- id: select_distinct_3_select_distinct_string4_from_onek_order_by_1_55c543cf
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_distinct.sql:13
-- compare: ordered
SELECT DISTINCT string4 FROM onek ORDER BY 1;

-- id: select_distinct_38_select_distinct_four_from_tenk1_4cb05130
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_distinct.sql:126
-- compare: multiset
SELECT DISTINCT four FROM tenk1;

-- id: select_distinct_48_select_distinct_four_from_tenk1_where_four_0_0ba9ea01
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_distinct.sql:164
-- compare: multiset
SELECT DISTINCT four FROM tenk1 WHERE four = 0;

-- id: select_distinct_50_select_distinct_four_from_tenk1_where_four_0_and_two_0_29552a34
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_distinct.sql:171
-- compare: multiset
SELECT DISTINCT four FROM tenk1 WHERE four = 0 AND two <> 0;

-- id: local_pg_distinct_on_values_visible_sort
-- origin: local pg_fusion DISTINCT ON compatibility
-- compare: ordered
SELECT DISTINCT ON (a) a, b
FROM (VALUES (1,2), (1,1), (2,4), (2,3)) v(a,b)
ORDER BY a, b;

-- id: local_pg_distinct_on_values_hidden_sort
-- origin: local pg_fusion DISTINCT ON compatibility
-- compare: ordered
SELECT DISTINCT ON (a) a
FROM (VALUES (1,2), (1,1), (2,4), (2,3)) v(a,b)
ORDER BY a, b;

-- id: local_pg_distinct_on_tenk1_hidden_sort
-- origin: local pg_fusion DISTINCT ON compatibility
-- compare: ordered
SELECT DISTINCT ON (four) four
FROM tenk1
ORDER BY four, two;

-- id: select_distinct_69_select_1_is_distinct_from_2_as_yes_4ab72148
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_distinct.sql:211
-- compare: multiset
SELECT 1 IS DISTINCT FROM 2 as "yes";

-- id: select_parallel_47_select_length_stringu1_from_tenk1_group_by_length_stringu1_5b26af97
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_parallel.sql:89
-- compare: multiset
select length(stringu1) from tenk1 group by length(stringu1);

-- id: select_parallel_64_select_count_from_tenk1_where_tenk1_unique1_select_max_tenk2_unique1_fro_8c0a130e
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_parallel.sql:128
-- compare: multiset
select count(*) from tenk1
    where tenk1.unique1 = (Select max(tenk2.unique1) from tenk2);

-- id: select_parallel_73_select_count_unique1_from_tenk1_where_hundred_1_63479c68
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_parallel.sql:143
-- compare: multiset
select  count((unique1)) from tenk1 where hundred > 1;

-- id: select_parallel_80_select_from_select_count_unique1_from_tenk1_where_hundred_10_ss_right_jo_d52e6787
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_parallel.sql:161
-- compare: multiset
select * from
  (select count(unique1) from tenk1 where hundred > 10) ss
  right join (values (1),(2),(3)) v(x) on true;

-- id: select_parallel_89_select_count_from_tenk1_left_join_select_tenk2_unique1_from_tenk2_order__ea093eee
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_parallel.sql:183
-- compare: ordered
select count(*) from tenk1
  left join (select tenk2.unique1 from tenk2 order by 1 limit 1000) ss
  on tenk1.unique1 < ss.unique1 + 1
  where tenk1.unique1 < 2;

-- id: select_parallel_126_select_count_from_tenk1_tenk2_where_tenk1_unique1_tenk2_unique1_58d5707a
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_parallel.sql:263
-- compare: multiset
select  count(*) from tenk1, tenk2 where tenk1.unique1 = tenk2.unique1;

-- id: select_parallel_131_select_count_from_tenk1_group_by_twenty_3de67661
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_parallel.sql:273
-- compare: multiset
select count(*) from tenk1 group by twenty;

-- id: select_parallel_143_select_from_select_string4_count_unique2_from_tenk1_group_by_string4_ord_bb59df77
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_parallel.sql:314
-- compare: ordered
select * from
  (select string4, count(unique2)
   from tenk1 group by string4 order by string4) ss
  right join (values (1),(2),(3)) v(x) on true;

-- id: select_parallel_149_select_fivethous_from_tenk1_order_by_fivethous_limit_4_df0929ee
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_parallel.sql:333
-- compare: ordered
select fivethous from tenk1 order by fivethous limit 4;

-- id: select_parallel_152_select_string4_from_tenk1_order_by_string4_limit_5_580a1935
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_parallel.sql:340
-- compare: ordered
select string4 from tenk1 order by string4 limit 5;

-- id: strings_61_select_cast_f1_as_text_as_text_varchar_from_varchar_tbl_b62ddc60
-- origin: postgres REL_17_STABLE src/test/regress/sql/strings.sql:99
-- compare: multiset
SELECT CAST(f1 AS text) AS "text(varchar)" FROM VARCHAR_TBL;

-- id: subselect_1_select_1_as_one_where_1_in_select_1_54e8fd2f
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:1
-- compare: multiset
SELECT 1 AS one WHERE 1 IN (SELECT 1);

-- id: subselect_2_select_1_as_zero_where_1_not_in_select_1_64f3a735
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:5
-- compare: multiset
SELECT 1 AS zero WHERE 1 NOT IN (SELECT 1);

-- id: subselect_4_select_from_select_1_as_x_ss_390837aa
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:9
-- compare: multiset
SELECT * FROM (SELECT 1 AS x) ss;

-- id: subselect_5_select_from_select_1_as_x_ss_23c4a5e8
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:13
-- compare: multiset
SELECT * FROM ((SELECT 1 AS x)) ss;

-- id: subselect_6_select_from_select_1_as_x_select_from_select_2_as_y_6a568c7b
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:14
-- compare: multiset
SELECT * FROM ((SELECT 1 AS x)), ((SELECT * FROM ((SELECT 2 AS y))));

-- id: subselect_11_select_select_array_1_2_3_1_6c6e39a4
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:22
-- compare: multiset
SELECT (SELECT ARRAY[1,2,3])[1];

-- id: subselect_12_select_select_array_1_2_3_2_7876a4cf
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:24
-- compare: multiset
SELECT ((SELECT ARRAY[1,2,3]))[2];

-- id: subselect_13_select_select_array_1_2_3_3_75098ee6
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:25
-- compare: multiset
SELECT (((SELECT ARRAY[1,2,3])))[3];

-- id: subselect_39_select_from_select_from_int4_tbl_values_123456_where_f1_column1_a6a159f8
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:102
-- compare: multiset
SELECT * FROM (SELECT * FROM int4_tbl), (VALUES (123456)) WHERE f1 = column1;

-- id: subselect_55_select_count_from_select_1_from_tenk1_a_where_unique1_in_select_hundred__533ad584
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:166
-- compare: multiset
select count(*) from
  (select 1 from tenk1 a
   where unique1 IN (select hundred from tenk1 b)) ss;

-- id: subselect_56_select_count_distinct_ss_ten_from_select_ten_from_tenk1_a_where_unique1__bf7b0797
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:175
-- compare: multiset
select count(distinct ss.ten) from
  (select ten from tenk1 a
   where unique1 IN (select hundred from tenk1 b)) ss;

-- id: subselect_57_select_count_from_select_1_from_tenk1_a_where_unique1_in_select_distinct_ce152be4
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:178
-- compare: multiset
select count(*) from
  (select 1 from tenk1 a
   where unique1 IN (select distinct hundred from tenk1 b)) ss;

-- id: subselect_58_select_count_distinct_ss_ten_from_select_ten_from_tenk1_a_where_unique1__7628cdcd
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:181
-- compare: multiset
select count(distinct ss.ten) from
  (select ten from tenk1 a
   where unique1 IN (select distinct hundred from tenk1 b)) ss;

-- id: local_pg_scalar_subquery_one_row_predicate
-- origin: local pg_fusion scalar subquery compatibility
-- compare: multiset
SELECT unique1
FROM (VALUES (0), (1), (2)) AS t(unique1)
WHERE unique1 < 3
  AND unique1 = (SELECT v FROM (VALUES (1)) AS s(v));

-- id: local_pg_scalar_subquery_null_row_is_null
-- origin: local pg_fusion scalar subquery compatibility
-- compare: multiset
SELECT (SELECT min(unique1) FILTER (WHERE false) FROM tenk1) IS NULL;

-- id: subselect_97_select_from_select_max_unique1_from_tenk1_as_a_where_exists_select_1_fro_fb689d70
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:326
-- compare: multiset
select * from (
  select max(unique1) from tenk1 as a
  where exists (select 1 from tenk1 as b where b.thousand = a.unique2)
) ss;

-- id: subselect_175_select_count_from_tenk1_t_where_exists_select_1_from_tenk1_k_where_k_uni_53639c01
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:579
-- compare: multiset
select count(*) from tenk1 t
where (exists(select 1 from tenk1 k where k.unique1 = t.unique2) or ten < 0);

-- id: subselect_177_select_count_from_tenk1_t_where_exists_select_1_from_tenk1_k_where_k_uni_60672592
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:585
-- compare: multiset
select count(*) from tenk1 t
where (exists(select 1 from tenk1 k where k.unique1 = t.unique2) or ten < 0)
  and thousand = 1;

-- id: text_1_select_text_this_is_a_text_string_text_this_is_a_text_string_as_true_33e68d65
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:1
-- compare: multiset
SELECT text 'this is a text string' = text 'this is a text string' AS true;

-- id: text_2_select_text_this_is_a_text_string_text_this_is_a_text_strin_as_false_f5a10886
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:5
-- compare: multiset
SELECT text 'this is a text string' = text 'this is a text strin' AS false;

-- id: text_3_select_from_text_tbl_ed0dec6f
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:7
-- compare: multiset
SELECT * FROM TEXT_TBL;

-- id: text_8_select_concat_one_6ee845a3
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:26
-- compare: multiset
/*
 * various string functions
 */
select concat('one');

-- id: text_10_select_concat_ws_one_fa02b693
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:32
-- compare: multiset
select concat_ws('#','one');

-- id: text_15_select_reverse_abcde_8c71b537
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:37
-- compare: multiset
select reverse('abcde');

-- id: varchar_10_select_from_varchar_tbl_b71835a9
-- origin: postgres REL_17_STABLE src/test/regress/sql/varchar.sql:28
-- compare: multiset
SELECT * FROM VARCHAR_TBL;

-- id: varchar_11_select_c_from_varchar_tbl_c_where_c_f1_a_156332e7
-- origin: postgres REL_17_STABLE src/test/regress/sql/varchar.sql:31
-- compare: multiset
SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 <> 'a';

-- id: varchar_12_select_c_from_varchar_tbl_c_where_c_f1_a_1b85af38
-- origin: postgres REL_17_STABLE src/test/regress/sql/varchar.sql:35
-- compare: multiset
SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 = 'a';

-- id: varchar_13_select_c_from_varchar_tbl_c_where_c_f1_a_fe06db44
-- origin: postgres REL_17_STABLE src/test/regress/sql/varchar.sql:39
-- compare: multiset
SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 < 'a';

-- id: varchar_14_select_c_from_varchar_tbl_c_where_c_f1_a_c7d2837b
-- origin: postgres REL_17_STABLE src/test/regress/sql/varchar.sql:43
-- compare: multiset
SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 <= 'a';

-- id: varchar_15_select_c_from_varchar_tbl_c_where_c_f1_a_2b918956
-- origin: postgres REL_17_STABLE src/test/regress/sql/varchar.sql:47
-- compare: multiset
SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 > 'a';

-- id: varchar_16_select_c_from_varchar_tbl_c_where_c_f1_a_7927c26b
-- origin: postgres REL_17_STABLE src/test/regress/sql/varchar.sql:51
-- compare: multiset
SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 >= 'a';

-- id: window_11_select_sum_four_over_partition_by_ten_order_by_unique2_as_sum_1_ten_four_07b35e4d
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:42
-- compare: ordered
SELECT sum(four) OVER (PARTITION BY ten ORDER BY unique2) AS sum_1, ten, four FROM tenk1 WHERE unique2 < 10;

-- id: window_15_select_percent_rank_over_partition_by_four_order_by_ten_ten_four_from_te_13471eda
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:51
-- compare: ordered
SELECT percent_rank() OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

-- id: window_16_select_cume_dist_over_partition_by_four_order_by_ten_ten_four_from_tenk1_3870650e
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:53
-- compare: ordered
SELECT cume_dist() OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

-- id: window_19_select_lag_ten_over_partition_by_four_order_by_ten_ten_four_from_tenk1_w_0f7aab71
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:59
-- compare: ordered
SELECT lag(ten) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

-- id: window_23_select_lead_ten_over_partition_by_four_order_by_ten_ten_four_from_tenk1__8a6f24ae
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:66
-- compare: ordered
SELECT lead(ten) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

-- id: window_24_select_lead_ten_2_1_over_partition_by_four_order_by_ten_ten_four_from_te_887fb59a
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:68
-- compare: ordered
SELECT lead(ten * 2, 1) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

-- id: window_25_select_lead_ten_2_1_1_over_partition_by_four_order_by_ten_ten_four_from__b28e715e
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:70
-- compare: ordered
SELECT lead(ten * 2, 1, -1) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

-- id: window_27_select_first_value_ten_over_partition_by_four_order_by_ten_ten_four_from_f2813462
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:73
-- compare: ordered
SELECT first_value(ten) OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

-- id: window_28_select_last_value_four_over_order_by_ten_ten_four_from_tenk1_where_uniqu_47dd022a
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:75
-- compare: ordered
SELECT last_value(four) OVER (ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

-- id: window_29_select_last_value_ten_over_partition_by_four_ten_four_from_select_from_t_4c81821e
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:78
-- compare: ordered
SELECT last_value(ten) OVER (PARTITION BY four), ten, four FROM
	(SELECT * FROM tenk1 WHERE unique2 < 10 ORDER BY four, ten)s
	ORDER BY four, ten;

-- id: window_31_select_ten_two_sum_hundred_as_gsum_sum_sum_hundred_over_partition_by_two_459430b7
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:85
-- compare: ordered
SELECT ten, two, sum(hundred) AS gsum, sum(sum(hundred)) OVER (PARTITION BY two ORDER BY ten) AS wsum
FROM tenk1 GROUP BY ten, two;

-- id: window_32_select_count_over_partition_by_four_four_from_select_from_tenk1_where_tw_9dddf04d
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:88
-- compare: multiset
SELECT count(*) OVER (PARTITION BY four), four FROM (SELECT * FROM tenk1 WHERE two = 1)s WHERE unique2 < 10;

-- id: window_34_select_from_select_count_over_partition_by_four_order_by_ten_sum_hundred_7986e629
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:94
-- compare: ordered
SELECT * FROM(
  SELECT count(*) OVER (PARTITION BY four ORDER BY ten) +
    sum(hundred) OVER (PARTITION BY two ORDER BY ten) AS total,
    count(*) OVER (PARTITION BY four ORDER BY ten) AS fourcount,
    sum(hundred) OVER (PARTITION BY two ORDER BY ten) AS twosum
    FROM tenk1
)sub
WHERE total <> fourcount + twosum;

-- id: window_36_select_ten_two_sum_hundred_as_gsum_sum_sum_hundred_over_win_as_wsum_from_e41a14e1
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:106
-- compare: ordered
SELECT ten, two, sum(hundred) AS gsum, sum(sum(hundred)) OVER win AS wsum
FROM tenk1 GROUP BY ten, two WINDOW win AS (PARTITION BY two ORDER BY ten);

-- id: window_43_select_sum_count_f1_over_from_int4_tbl_where_f1_42_ccc4dd7c
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:138
-- compare: multiset
SELECT SUM(COUNT(f1)) OVER () FROM int4_tbl WHERE f1=42;

-- id: window_47_select_four_ten_sum_ten_over_partition_by_four_order_by_ten_last_value_t_f707b538
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:158
-- compare: ordered
SELECT four, ten,
	sum(ten) over (partition by four order by ten),
	last_value(ten) over (partition by four order by ten)
FROM (select distinct ten, four from tenk1) ss;

-- id: window_48_select_four_ten_sum_ten_over_partition_by_four_order_by_ten_range_betwee_3d9671a2
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:164
-- compare: ordered
SELECT four, ten,
	sum(ten) over (partition by four order by ten range between unbounded preceding and current row),
	last_value(ten) over (partition by four order by ten range between unbounded preceding and current row)
FROM (select distinct ten, four from tenk1) ss;

-- id: window_49_select_four_ten_sum_ten_over_partition_by_four_order_by_ten_range_betwee_1d956acd
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:169
-- compare: ordered
SELECT four, ten,
	sum(ten) over (partition by four order by ten range between unbounded preceding and unbounded following),
	last_value(ten) over (partition by four order by ten range between unbounded preceding and unbounded following)
FROM (select distinct ten, four from tenk1) ss;

-- id: window_50_select_four_ten_4_as_two_sum_ten_4_over_partition_by_four_order_by_ten_4_ea7418ac
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:174
-- compare: ordered
SELECT four, ten/4 as two,
	sum(ten/4) over (partition by four order by ten/4 range between unbounded preceding and current row),
	last_value(ten/4) over (partition by four order by ten/4 range between unbounded preceding and current row)
FROM (select distinct ten, four from tenk1) ss;

-- id: window_51_select_four_ten_4_as_two_sum_ten_4_over_partition_by_four_order_by_ten_4_0dc63adf
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:179
-- compare: ordered
SELECT four, ten/4 as two,
	sum(ten/4) over (partition by four order by ten/4 rows between unbounded preceding and current row),
	last_value(ten/4) over (partition by four order by ten/4 rows between unbounded preceding and current row)
FROM (select distinct ten, four from tenk1) ss;

-- id: window_53_select_sum_unique1_over_rows_between_current_row_and_unbounded_following_43c8537d
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:188
-- compare: multiset
SELECT sum(unique1) over (rows between current row and unbounded following),
	unique1, four
FROM tenk1 WHERE unique1 < 10;

-- id: window_54_select_sum_unique1_over_rows_between_2_preceding_and_2_following_unique1_53e1b5a0
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:192
-- compare: multiset
SELECT sum(unique1) over (rows between 2 preceding and 2 following),
	unique1, four
FROM tenk1 WHERE unique1 < 10;

-- id: window_65_select_sum_unique1_over_rows_between_2_preceding_and_1_preceding_unique1_61bb6223
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:236
-- compare: multiset
SELECT sum(unique1) over (rows between 2 preceding and 1 preceding),
	unique1, four
FROM tenk1 WHERE unique1 < 10;

-- id: window_66_select_sum_unique1_over_rows_between_1_following_and_3_following_unique1_99ec98bf
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:240
-- compare: multiset
SELECT sum(unique1) over (rows between 1 following and 3 following),
	unique1, four
FROM tenk1 WHERE unique1 < 10;

-- id: window_67_select_sum_unique1_over_rows_between_unbounded_preceding_and_1_following_6b760e84
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:244
-- compare: multiset
SELECT sum(unique1) over (rows between unbounded preceding and 1 following),
	unique1, four
FROM tenk1 WHERE unique1 < 10;

-- id: window_140_select_sum_unique1_over_rows_between_1_preceding_and_1_following_unique1_41c829e6
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:554
-- compare: multiset
SELECT sum(unique1) over (rows between 1 preceding and 1 following),
       unique1, four
FROM tenk1 WHERE unique1 < 10;

-- id: window_228_select_sum_unique1_over_partition_by_ten_order_by_four_groups_between_0__ebebd326
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:968
-- compare: ordered
SELECT sum(unique1) over (partition by ten
	order by four groups between 0 preceding and 0 following),unique1, four, ten
FROM tenk1 WHERE unique1 < 10;

-- id: window_345_select_i_sum_v_smallint_over_order_by_i_rows_between_current_row_and_unb_74528dba
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1807
-- compare: ordered
SELECT i,SUM(v::smallint) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

-- id: window_346_select_i_sum_v_int_over_order_by_i_rows_between_current_row_and_unbounde_9857bade
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1810
-- compare: ordered
SELECT i,SUM(v::int) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

-- id: window_347_select_i_sum_v_bigint_over_order_by_i_rows_between_current_row_and_unbou_45c6b046
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1813
-- compare: ordered
SELECT i,SUM(v::bigint) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

-- id: window_352_select_i_count_v_over_order_by_i_rows_between_current_row_and_unbounded__6b806ef8
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1828
-- compare: ordered
SELECT i,COUNT(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

-- id: window_353_select_i_count_over_order_by_i_rows_between_current_row_and_unbounded_fo_3df12155
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1831
-- compare: ordered
SELECT i,COUNT(*) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

-- id: window_378_select_i_sum_v_int_over_order_by_i_rows_between_current_row_and_current__e57da79e
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1906
-- compare: ordered
SELECT i,SUM(v::int) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND CURRENT ROW)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

-- id: window_379_select_i_sum_v_int_over_order_by_i_rows_between_current_row_and_1_follow_97dbafc7
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1910
-- compare: ordered
SELECT i,SUM(v::int) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v);

-- id: window_380_select_i_sum_v_int_over_order_by_i_rows_between_1_preceding_and_1_follow_8f31f6f5
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1913
-- compare: ordered
SELECT i,SUM(v::int) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,3),(4,4)) t(i,v);

-- id: union_1_select_1_as_two_union_select_2_order_by_1_2a01e8b5
-- origin: postgres REL_17_STABLE src/test/regress/sql/union.sql:1
-- compare: ordered
SELECT 1 AS two UNION SELECT 2 ORDER BY 1;

-- id: privileges_297_select_from_select_a_q1_as_x_random_from_int8_tbl_a_where_q1_0_union_all_e36af0b0
-- origin: postgres REL_17_STABLE src/test/regress/sql/privileges.sql:449
-- compare: multiset
select * from
  ((select a.q1 as x, random() from int8_tbl a where q1 > 0)
   union all
   (select b.q2 as x, random() from int8_tbl b where q2 > 0)) ss
where x < 0;

-- id: with_32_with_q1_x_y_as_select_hundred_sum_ten_from_tenk1_group_by_hundred_select_056e623e
-- origin: postgres REL_17_STABLE src/test/regress/sql/with.sql:230
-- compare: multiset
WITH q1(x,y) AS (
    SELECT hundred, sum(ten) FROM tenk1 GROUP BY hundred
  )
SELECT count(*) FROM q1 WHERE y > (SELECT sum(y)/100 FROM q1 qsub);

-- id: with_159_with_outermost_x_as_select_1_union_with_innermost_as_select_2_select_fro_efd6f00e
-- origin: postgres REL_17_STABLE src/test/regress/sql/with.sql:1123
-- compare: ordered
WITH outermost(x) AS (
  SELECT 1
  UNION (WITH innermost as (SELECT 2)
         SELECT * FROM innermost
         UNION SELECT 3)
)
SELECT * FROM outermost ORDER BY 1;

-- id: with_152_with_cte_foo_as_select_42_select_from_select_foo_from_cte_q_f22186fa
-- origin: postgres REL_17_STABLE src/test/regress/sql/with.sql:1073
-- compare: multiset
with cte(foo) as ( select 42 ) select * from ((select foo from cte)) q;

-- id: privileges_295_select_from_select_a_q1_as_x_from_int8_tbl_a_offset_0_union_all_select_b_f057b913
-- origin: postgres REL_17_STABLE src/test/regress/sql/privileges.sql:440
-- compare: ordered
select * from
  ((select a.q1 as x from int8_tbl a offset 0)
   union all
   (select b.q2 as x from int8_tbl b offset 0)) ss
where false;

-- id: window_242_select_count_over_partition_by_four_from_select_from_tenk1_union_all_sel_32804032
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1051
-- compare: ordered
SELECT count(*) OVER (PARTITION BY four) FROM (SELECT * FROM tenk1 UNION ALL SELECT * FROM tenk2)s LIMIT 0;

-- id: aggregates_308_select_min_unique1_filter_where_unique1_100_from_tenk1_a93ebf4b
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:846
-- compare: multiset
select min(unique1) filter (where unique1 > 100) from tenk1;

-- id: aggregates_309_select_sum_1_ten_filter_where_ten_0_from_tenk1_7d8890f1
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:850
-- compare: multiset
select sum(1/ten) filter (where ten > 0) from tenk1;

-- id: aggregates_310_select_ten_sum_distinct_four_filter_where_four_text_123_from_onek_a_grou_3dc69b39
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:852
-- compare: multiset
select ten, sum(distinct four) filter (where four::text ~ '123') from onek a
group by ten;

-- id: groupingsets_79_select_ten_sum_distinct_four_filter_where_four_text_123_from_onek_a_grou_4a34ea32
-- origin: postgres REL_17_STABLE src/test/regress/sql/groupingsets.sql:297
-- compare: multiset
select ten, sum(distinct four) filter (where four::text ~ '123') from onek a
group by rollup(ten);

-- id: join_336_with_ctetable_as_not_materialized_select_1_as_f1_select_from_ctetable_c1_761db13e
-- origin: postgres REL_17_STABLE src/test/regress/sql/join.sql:1311
-- compare: multiset
with ctetable as not materialized ( select 1 as f1 )
select * from ctetable c1
where f1 in ( select c3.f1 from ctetable c2 full join ctetable c3 on true );

-- id: aggregates_155_select_max_100_from_tenk1_13905752
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:410
-- compare: multiset
select max(100) from tenk1;

-- id: create_index_181_select_count_from_tenk1_where_stringu1_tvaaaa_86690e88
-- origin: postgres REL_17_STABLE src/test/regress/sql/create_index.sql:376
-- compare: multiset
SELECT count(*) FROM tenk1 WHERE stringu1 = 'TVAAAA';

-- id: create_index_362_select_count_from_tenk1_where_hundred_42_and_thousand_42_or_thousand_99_8518b2e5
-- origin: postgres REL_17_STABLE src/test/regress/sql/create_index.sql:739
-- compare: multiset
SELECT count(*) FROM tenk1
  WHERE hundred = 42 AND (thousand = 42 OR thousand = 99);

-- id: select_parallel_183_select_count_from_tenk1_dc2a1c37
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_parallel.sql:412
-- compare: multiset
select count(*) from tenk1;

-- id: stats_54_select_count_from_tenk2_56c47e65
-- origin: postgres REL_17_STABLE src/test/regress/sql/stats.sql:91
-- compare: multiset
SELECT count(*) FROM tenk2;

-- id: stats_56_select_count_from_tenk2_where_unique1_1_4f4fcf17
-- origin: postgres REL_17_STABLE src/test/regress/sql/stats.sql:97
-- compare: multiset
SELECT count(*) FROM tenk2 WHERE unique1 = 1;

-- id: window_8_select_count_over_from_tenk1_where_unique2_10_da5675b2
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:34
-- compare: multiset
SELECT COUNT(*) OVER () FROM tenk1 WHERE unique2 < 10;

-- id: window_9_select_count_over_w_from_tenk1_where_unique2_10_window_w_as_e62c4294
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:37
-- compare: multiset
SELECT COUNT(*) OVER w FROM tenk1 WHERE unique2 < 10 WINDOW w AS ();

-- id: window_341_select_i_avg_v_interval_over_order_by_i_rows_between_current_row_and_unb_c9d4644f
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1771
-- compare: ordered
SELECT i,AVG(v::interval) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,'1 sec'),(2,'2 sec'),(3,NULL),(4,NULL)) t(i,v);

-- id: local_pg_avg_distinct_float8_zero
-- origin: local pg_fusion avg distinct compatibility
-- compare: multiset
SELECT avg(DISTINCT v::float8) FROM (VALUES (0.0),(-0.0),(2.0),(NULL)) t(v);

-- id: local_pg_avg_distinct_interval
-- origin: local pg_fusion avg distinct compatibility
-- compare: multiset
SELECT avg(DISTINCT v::interval) FROM (VALUES ('1 sec'),('1 sec'),('3 sec'),(NULL)) t(v);

-- id: aggregates_20_select_max_student_gpa_as_max_3_7_from_student_a2802753
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:47
-- compare: multiset
SELECT max(student.gpa) AS max_3_7 FROM student;

-- id: aggregates_169_select_min_f1_max_f1_from_minmaxtest_e0dcc504
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:429
-- compare: multiset
select min(f1), max(f1) from minmaxtest;

-- id: aggregates_171_select_distinct_min_f1_max_f1_from_minmaxtest_e2a61c73
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:434
-- compare: multiset
select distinct min(f1), max(f1) from minmaxtest;

-- id: aggregates_320_select_max_0_filter_where_b1_from_bool_test_65bf20d8
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:888
-- compare: multiset
select max(0) filter (where b1) from bool_test;

-- id: select_distinct_65_select_f1_f1_is_distinct_from_2_as_not_2_from_disttable_051b53f8
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_distinct.sql:205
-- compare: multiset
SELECT f1, f1 IS DISTINCT FROM 2 as "not 2" FROM disttable;

-- id: select_distinct_66_select_f1_f1_is_distinct_from_null_as_not_null_from_disttable_2ce47e84
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_distinct.sql:208
-- compare: multiset
SELECT f1, f1 IS DISTINCT FROM NULL as "not null" FROM disttable;

-- id: select_distinct_67_select_f1_f1_is_distinct_from_f1_as_false_from_disttable_fde466eb
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_distinct.sql:209
-- compare: multiset
SELECT f1, f1 IS DISTINCT FROM f1 as "false" FROM disttable;

-- id: select_distinct_68_select_f1_f1_is_distinct_from_f1_1_as_not_null_from_disttable_daf3d666
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_distinct.sql:210
-- compare: multiset
SELECT f1, f1 IS DISTINCT FROM f1+1 as "not null" FROM disttable;

-- id: select_having_12_select_b_c_from_test_having_group_by_b_c_having_count_1_order_by_b_c_2f761135
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_having.sql:16
-- compare: ordered
SELECT b, c FROM test_having
	GROUP BY b, c HAVING count(*) = 1 ORDER BY b, c;

-- id: select_having_16_select_min_a_max_a_from_test_having_having_min_a_max_a_ceb43825
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_having.sql:31
-- compare: multiset
SELECT min(a), max(a) FROM test_having HAVING min(a) = max(a);

-- id: select_having_17_select_min_a_max_a_from_test_having_having_min_a_max_a_95c6d507
-- origin: postgres REL_17_STABLE src/test/regress/sql/select_having.sql:36
-- compare: multiset
SELECT min(a), max(a) FROM test_having HAVING min(a) < max(a);

-- id: subselect_23_select_from_subselect_tbl_4e4266f1
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:43
-- compare: multiset
SELECT * FROM SUBSELECT_TBL;

-- id: subselect_24_select_f1_as_constant_select_from_subselect_tbl_where_f1_in_select_1_6c08eea2
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:45
-- compare: multiset
SELECT f1 AS "Constant Select" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT 1);

-- id: subselect_25_select_f1_as_uncorrelated_field_from_subselect_tbl_where_f1_in_select_f2_95581194
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:50
-- compare: multiset
SELECT f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL);

-- id: subselect_26_select_f1_as_uncorrelated_field_from_subselect_tbl_where_f1_in_select_f2_5fc4be42
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:53
-- compare: multiset
SELECT f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE
    f2 IN (SELECT f1 FROM SUBSELECT_TBL));

-- id: subselect_28_select_f1_as_correlated_field_f2_as_second_field_from_subselect_tbl_uppe_311f9dd4
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:62
-- compare: multiset
SELECT f1 AS "Correlated Field", f2 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE f1 = upper.f1);

-- id: subselect_29_select_f1_as_correlated_field_f3_as_second_field_from_subselect_tbl_uppe_fd00b567
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:68
-- compare: multiset
SELECT f1 AS "Correlated Field", f3 AS "Second Field"
  FROM SUBSELECT_TBL upper
  WHERE f1 IN
    (SELECT f2 FROM SUBSELECT_TBL WHERE CAST(upper.f2 AS float) = f3);

-- id: subselect_45_select_ss_f1_as_correlated_field_ss_f3_as_second_field_from_subselect_tb_f52652ee
-- origin: postgres REL_17_STABLE src/test/regress/sql/subselect.sql:126
-- compare: multiset
SELECT ss.f1 AS "Correlated Field", ss.f3 AS "Second Field"
  FROM SUBSELECT_TBL ss
  WHERE f1 NOT IN (SELECT f1+1 FROM INT4_TBL
                   WHERE f1 != ss.f1 AND f1 < 2147483647);

-- id: window_6_select_depname_empno_salary_sum_salary_over_w_from_empsalary_window_w_as_01ed8406
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:30
-- compare: multiset
SELECT depname, empno, salary, sum(salary) OVER w FROM empsalary WINDOW w AS (PARTITION BY depname);

-- id: window_38_select_sum_salary_over_w1_count_over_w2_from_empsalary_window_w1_as_orde_119781cb
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:115
-- compare: ordered
SELECT sum(salary) OVER w1, count(*) OVER w2
FROM empsalary WINDOW w1 AS (ORDER BY salary), w2 AS (ORDER BY salary);

-- id: window_116_select_first_value_salary_over_order_by_salary_range_between_1000_preced_433047fa
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:417
-- compare: ordered
select first_value(salary) over(order by salary range between 1000 preceding and 1000 following),
	lead(salary) over(order by salary range between 1000 preceding and 1000 following),
	nth_value(salary, 1) over(order by salary range between 1000 preceding and 1000 following),
	salary from empsalary;

-- id: window_117_select_last_value_salary_over_order_by_salary_range_between_1000_precedi_21c0f8c1
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:422
-- compare: ordered
select last_value(salary) over(order by salary range between 1000 preceding and 1000 following),
	lag(salary) over(order by salary range between 1000 preceding and 1000 following),
	salary from empsalary;

-- id: window_284_select_from_select_empno_salary_count_over_order_by_salary_desc_c_from_e_b1e73f18
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1268
-- compare: ordered
SELECT * FROM
  (SELECT empno,
          salary,
          count(*) OVER (ORDER BY salary DESC) c
   FROM empsalary) emp
WHERE c <= 3;

-- id: window_286_select_from_select_empno_salary_count_empno_over_order_by_salary_desc_c__ca524725
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1283
-- compare: ordered
SELECT * FROM
  (SELECT empno,
          salary,
          count(empno) OVER (ORDER BY salary DESC) c
   FROM empsalary) emp
WHERE c <= 3;

-- id: window_294_select_from_select_empno_depname_salary_count_empno_over_partition_by_de_9c84484c
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1352
-- compare: ordered
SELECT * FROM
  (SELECT empno,
          depname,
          salary,
          count(empno) OVER (PARTITION BY depname ORDER BY salary DESC) c
   FROM empsalary) emp
WHERE c <= 3;

-- id: with_112_select_from_y_e488eccc
-- origin: postgres REL_17_STABLE src/test/regress/sql/with.sql:872
-- compare: multiset
SELECT * FROM y;

-- id: text_26_select_format_null_8f03bec5
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:50
-- compare: multiset
select format(NULL);

-- id: text_27_select_format_hello_805328f3
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:55
-- compare: multiset
select format('Hello');

-- id: text_28_select_format_hello_s_world_08bef188
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:56
-- compare: multiset
select format('Hello %s', 'World');

-- id: text_29_select_format_hello_819b980b
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:57
-- compare: multiset
select format('Hello %%');

-- id: text_30_select_format_hello_09ae5bec
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:58
-- compare: multiset
select format('Hello %%%%');

-- id: text_34_select_format_insert_into_i_values_l_l_mytab_10_hello_68281881
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:63
-- compare: multiset
select format('INSERT INTO %I VALUES(%L,%L)', 'mytab', 10, 'Hello');

-- id: text_35_select_format_s_s_s_hello_null_world_09dcf7ed
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:65
-- compare: multiset
select format('%s%s%s','Hello', NULL,'World');

-- id: text_36_select_format_insert_into_i_values_l_l_mytab_10_null_f2f7e9cf
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:66
-- compare: multiset
select format('INSERT INTO %I VALUES(%L,%L)', 'mytab', 10, NULL);

-- id: text_37_select_format_insert_into_i_values_l_l_mytab_null_hello_069855bc
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:67
-- compare: multiset
select format('INSERT INTO %I VALUES(%L,%L)', 'mytab', NULL, 'Hello');

-- id: text_39_select_format_1_s_3_s_1_2_3_c4d90445
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:70
-- compare: multiset
select format('%1$s %3$s', 1, 2, 3);

-- id: text_40_select_format_1_s_12_s_1_2_3_4_5_6_7_8_9_10_11_12_4386ac67
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:72
-- compare: multiset
select format('%1$s %12$s', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);

-- id: text_47_select_format_hello_s_1_s_s_world_hello_again_e035cc39
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:80
-- compare: multiset
select format('Hello %s %1$s %s', 'World', 'Hello again');

-- id: text_48_select_format_hello_s_s_2_s_2_s_world_hello_again_e48691f2
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:82
-- compare: multiset
select format('Hello %s %s, %2$s %2$s', 'World', 'Hello again');

-- id: text_57_select_format_10s_hello_0052fff5
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:96
-- compare: multiset
select format('>>%10s<<', 'Hello');

-- id: text_58_select_format_10s_null_361c3129
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:98
-- compare: multiset
select format('>>%10s<<', NULL);

-- id: text_59_select_format_10s_d6af9da6
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:99
-- compare: multiset
select format('>>%10s<<', '');

-- id: text_60_select_format_10s_dfc02ce5
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:100
-- compare: multiset
select format('>>%-10s<<', '');

-- id: text_61_select_format_10s_hello_192a9a7e
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:101
-- compare: multiset
select format('>>%-10s<<', 'Hello');

-- id: text_62_select_format_10s_null_8753195e
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:102
-- compare: multiset
select format('>>%-10s<<', NULL);

-- id: text_63_select_format_1_10s_hello_4042ed86
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:103
-- compare: multiset
select format('>>%1$10s<<', 'Hello');

-- id: text_64_select_format_1_10i_hello_f5929b14
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:104
-- compare: multiset
select format('>>%1$-10I<<', 'Hello');

-- id: text_65_select_format_2_1_l_10_hello_309c2fc1
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:105
-- compare: multiset
select format('>>%2$*1$L<<', 10, 'Hello');

-- id: text_66_select_format_2_1_l_10_null_c4b87459
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:106
-- compare: multiset
select format('>>%2$*1$L<<', 10, NULL);

-- id: text_67_select_format_2_1_l_10_null_626f25c2
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:107
-- compare: multiset
select format('>>%2$*1$L<<', -10, NULL);

-- id: text_68_select_format_s_10_hello_1f03e023
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:108
-- compare: multiset
select format('>>%*s<<', 10, 'Hello');

-- id: text_69_select_format_1_s_10_hello_c255f7c2
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:109
-- compare: multiset
select format('>>%*1$s<<', 10, 'Hello');

-- id: text_70_select_format_s_hello_a6a6aa7b
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:110
-- compare: multiset
select format('>>%-s<<', 'Hello');

-- id: text_71_select_format_10l_null_adab1a54
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:111
-- compare: multiset
select format('>>%10L<<', NULL);

-- id: text_72_select_format_2_1_l_null_hello_fce695c9
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:112
-- compare: multiset
select format('>>%2$*1$L<<', NULL, 'Hello');

-- id: text_73_select_format_2_1_l_0_hello_50767612
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:113
-- compare: multiset
select format('>>%2$*1$L<<', 0, 'Hello');

-- id: aggregates_69_select_regr_count_b_a_from_aggtest_bb712fac
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:119
-- compare: multiset
SELECT regr_count(b, a) FROM aggtest;

-- id: aggregates_267_select_string_agg_a_from_values_aaaa_bbbb_cccc_g_a_fc7a91b3
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:749
-- compare: multiset
select string_agg(a,',') from (values('aaaa'),('bbbb'),('cccc')) g(a);

-- id: aggregates_268_select_string_agg_a_from_values_aaaa_null_bbbb_cccc_g_a_1fc046c7
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:752
-- compare: multiset
select string_agg(a,',') from (values('aaaa'),(null),('bbbb'),('cccc')) g(a);

-- id: aggregates_269_select_string_agg_a_ab_from_values_null_null_bbbb_cccc_g_a_52dd872d
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:753
-- compare: multiset
select string_agg(a,'AB') from (values(null),(null),('bbbb'),('cccc')) g(a);

-- id: aggregates_270_select_string_agg_a_from_values_null_null_g_a_a4d25ef7
-- origin: postgres REL_17_STABLE src/test/regress/sql/aggregates.sql:754
-- compare: multiset
select string_agg(a,',') from (values(null),(null)) g(a);

-- id: float8_54_select_ceiling_f1_as_ceiling_f1_from_float8_tbl_f_76ece77a
-- origin: postgres REL_17_STABLE src/test/regress/sql/float8.sql:107
-- compare: multiset
select ceiling(f1) as ceiling_f1 from float8_tbl f;

-- id: text_12_select_concat_ws_10_20_null_30_7d3322cc
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:34
-- compare: multiset
select concat_ws(',',10,20,null,30);

-- id: text_13_select_concat_ws_10_20_null_30_0bcab669
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:35
-- compare: multiset
select concat_ws('',10,20,null,30);

-- id: text_14_select_concat_ws_null_10_20_null_30_is_null_b0b1ff72
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:36
-- compare: multiset
select concat_ws(NULL,10,20,null,30) is null;

-- id: text_17_select_quote_literal_bf8eba58
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:39
-- compare: multiset
select quote_literal('');

-- id: text_18_select_quote_literal_abc_a9efab5a
-- origin: postgres REL_17_STABLE src/test/regress/sql/text.sql:40
-- compare: multiset
select quote_literal('abc''');

-- id: window_12_select_row_number_over_order_by_unique2_from_tenk1_where_unique2_10_704ec8c5
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:45
-- compare: ordered
SELECT row_number() OVER (ORDER BY unique2) FROM tenk1 WHERE unique2 < 10;

-- id: window_13_select_rank_over_partition_by_four_order_by_ten_as_rank_1_ten_four_from__d840992d
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:47
-- compare: ordered
SELECT rank() OVER (PARTITION BY four ORDER BY ten) AS rank_1, ten, four FROM tenk1 WHERE unique2 < 10;

-- id: window_14_select_dense_rank_over_partition_by_four_order_by_ten_ten_four_from_tenk_47e35a32
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:49
-- compare: ordered
SELECT dense_rank() OVER (PARTITION BY four ORDER BY ten), ten, four FROM tenk1 WHERE unique2 < 10;

-- id: window_17_select_ntile_3_over_order_by_ten_four_ten_four_from_tenk1_where_unique2__e6bcd4a4
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:55
-- compare: ordered
SELECT ntile(3) OVER (ORDER BY ten, four), ten, four FROM tenk1 WHERE unique2 < 10;

-- id: window_44_select_ten_sum_unique1_sum_unique2_as_res_rank_over_order_by_sum_unique1_ae25f321
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:141
-- compare: ordered
select ten,
  sum(unique1) + sum(unique2) as res,
  rank() over (order by sum(unique1) + sum(unique2)) as rank
from tenk1
group by ten order by ten;

-- id: window_255_select_rank_over_order_by_length_abc_c7614aa6
-- origin: postgres REL_17_STABLE src/test/regress/sql/window.sql:1092
-- compare: ordered
SELECT rank() OVER (ORDER BY length('abc'));

-- id: pg_fusion_checked_integer_arithmetic_constants
-- origin: pg_fusion checked integer arithmetic coverage
-- compare: multiset
SELECT
  32000::int2 + 767::int2,
  -32760::int2 - 7::int2,
  181::int2 * 181::int2,
  2147483000::int4 + 647::int4,
  10::int4 - 12::int4,
  46340::int4 * 46340::int4,
  9223372036854775000::int8 + 807::int8,
  -9223372036854775800::int8 - 7::int8,
  3037000499::int8 * 3037000499::int8;

-- id: pg_fusion_checked_integer_arithmetic_values
-- origin: pg_fusion checked integer arithmetic coverage
-- compare: multiset
SELECT
  i2a + i2b AS i2_add,
  i2a - i2b AS i2_sub,
  i2a * i2b AS i2_mul,
  i4a + i4b AS i4_add,
  i4a - i4b AS i4_sub,
  i4a * i4b AS i4_mul,
  i8a + i8b AS i8_add,
  i8a - i8b AS i8_sub,
  i8a * i8b AS i8_mul
FROM (VALUES
  (1::int2, 2::int2, 100::int4, 3::int4, 1000::int8, 4::int8),
  (32000::int2, 1::int2, 2147483000::int4, 1::int4, 9223372036854775000::int8, 1::int8),
  (-16000::int2, 2::int2, -1073741824::int4, 2::int4, -4611686018427387904::int8, 2::int8)
) AS v(i2a, i2b, i4a, i4b, i8a, i8b);

-- id: pg_fusion_checked_integer_arithmetic_mixed_width
-- origin: pg_fusion checked integer arithmetic coverage
-- compare: multiset
SELECT
  32760::int2 + 7::int4,
  2147483640::int4 + 7::int8,
  42::int4 - 100::int2,
  246::int2 * 123456::int8;

-- id: local_pg_join_where_pushdown_inner_both_sides
-- origin: local pg_fusion join WHERE pushdown compatibility
-- compare: multiset
SELECT a.unique1, b.f1
FROM tenk1 a
JOIN int4_tbl b ON a.unique1 = b.f1
WHERE a.unique1 < 3 AND b.f1 >= 0;

-- id: local_pg_join_where_pushdown_left_preserved_side
-- origin: local pg_fusion join WHERE pushdown compatibility
-- compare: multiset
SELECT a.unique1, b.f1
FROM tenk1 a
LEFT JOIN int4_tbl b ON a.unique1 = b.f1
WHERE a.unique1 < 3;

-- id: local_pg_text_typmod_varchar_intermediate_comparison
-- origin: local pg_fusion text typmod cast coverage
-- compare: multiset
SELECT CAST('abc' AS varchar(2)) = 'ab';

-- id: local_pg_text_typmod_varchar_filter
-- origin: local pg_fusion text typmod cast coverage
-- compare: multiset
SELECT marker
FROM (VALUES ('abc'), ('ab'), ('ac')) AS v(marker)
WHERE CAST(marker AS varchar(2)) = 'ab';

-- id: local_pg_text_typmod_varchar_scan_filter
-- origin: local pg_fusion text typmod scan pushdown coverage
-- compare: multiset
SELECT f1
FROM VARCHAR_TBL
WHERE CAST(f1 AS varchar(2)) = 'ab';

-- id: local_pg_text_typmod_bpchar_projection
-- origin: local pg_fusion text typmod cast coverage
-- compare: multiset
SELECT CAST('a' AS char(3));

-- id: local_pg_text_typmod_bpchar_equality
-- origin: local pg_fusion bpchar UDF coverage
-- compare: multiset
SELECT CAST('a' AS char(3)) = CAST('a' AS char(1));

-- id: local_pg_text_typmod_bpchar_distinct
-- origin: local pg_fusion bpchar UDF coverage
-- compare: multiset
SELECT
    CAST('a' AS char(3)) <> CAST('a' AS char(1)),
    CAST('a' AS char(3)) IS DISTINCT FROM CAST('a' AS char(1)),
    CAST('a' AS char(3)) IS NOT DISTINCT FROM CAST('a' AS char(1));

-- id: local_pg_text_typmod_bpchar_length
-- origin: local pg_fusion bpchar UDF coverage
-- compare: multiset
SELECT length(CAST('a' AS char(3)));

-- id: local_pg_text_typmod_bpchar_scan_filter
-- origin: local pg_fusion text typmod scan pushdown coverage
-- compare: multiset
SELECT f1
FROM VARCHAR_TBL
WHERE CAST(f1 AS char(2)) = CAST('a' AS char(2));

-- id: local_pg_text_typmod_bpchar_left_join_residual
-- origin: local pg_fusion bpchar UDF coverage
-- compare: multiset
SELECT l.f1
FROM int4_tbl l
LEFT JOIN (VALUES (0, CAST('a' AS char(3))), (1, CAST('b' AS char(3)))) AS r(id, marker)
    ON l.f1 = r.id
WHERE length(r.marker) = 1;

-- id: local_pg_numeric_round_trunc_scale
-- origin: local pg_fusion scalar function overload coverage
-- compare: multiset
SELECT round(1.234::numeric, 2), trunc(1.234::numeric, 2);

-- id: local_pg_numeric_round_trunc_scale_scan_filter
-- origin: local pg_fusion scalar function scan pushdown coverage
-- compare: multiset
SELECT f1
FROM INT4_TBL
WHERE round(f1::numeric, 2) = f1::numeric
  AND trunc(f1::numeric, 2) = f1::numeric;

-- id: local_pg_function_boolean_wrapper_arguments
-- origin: local pg_fusion scalar function argument type inference coverage
-- compare: multiset
SELECT
    format('%s', TRUE AND FALSE),
    format('%s', NULL IS NULL),
    format('%s', TRUE IS NOT FALSE),
    concat(TRUE AND FALSE),
    concat_ws('|', NULL IS NULL, TRUE IS NOT FALSE);

-- id: local_pg_function_boolean_wrapper_vector_arguments
-- origin: local pg_fusion scalar function argument type inference coverage
-- compare: multiset
SELECT
    f1,
    format('%s', f1 > 0),
    concat(f1 > 0),
    concat(CASE WHEN f1 > 0 THEN NULL::boolean ELSE TRUE END)
FROM INT4_TBL;

-- id: local_pg_function_scalar_subquery_argument
-- origin: local pg_fusion scalar function argument type inference coverage
-- compare: multiset
SELECT length((SELECT 'abc'::text));

-- id: local_pg_like_scan_filter
-- origin: local pg_fusion LIKE operator scan pushdown coverage
-- compare: ordered
SELECT f1
FROM TEXT_TBL
WHERE f1 LIKE 'hi%'
ORDER BY f1;

-- id: local_pg_not_like_scan_filter
-- origin: local pg_fusion LIKE operator scan pushdown coverage
-- compare: ordered
SELECT f1
FROM TEXT_TBL
WHERE f1 NOT LIKE 'hi%'
ORDER BY f1;

-- id: local_pg_regex_match_scan_filter
-- origin: local pg_fusion regex operator scan pushdown coverage
-- compare: ordered
SELECT f1
FROM TEXT_TBL
WHERE f1 ~ '^hi'
ORDER BY f1;

-- id: local_pg_regex_not_match_scan_filter
-- origin: local pg_fusion regex operator scan pushdown coverage
-- compare: ordered
SELECT f1
FROM TEXT_TBL
WHERE f1 !~ '^hi'
ORDER BY f1;

-- id: local_pg_tpch_q16_like_not_in_shape
-- origin: local pg_fusion TPC-H q16-shaped LIKE/NOT IN coverage
-- compare: ordered
SELECT
    j1.t,
    count(DISTINCT j2.k) AS supplier_count
FROM j1_tbl j1
JOIN j2_tbl j2 ON j1.i = j2.i
WHERE j1.t <> 'four'
  AND j1.t NOT LIKE 't%'
  AND j2.k IN (-1, 2, 4, -5)
  AND j2.k NOT IN (
      SELECT s.i
      FROM j1_tbl s
      WHERE s.t LIKE '%e%'
        AND s.i IS NOT NULL
  )
GROUP BY j1.t
ORDER BY supplier_count DESC, j1.t
LIMIT 100;
