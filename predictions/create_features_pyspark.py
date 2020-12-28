PATH_TO_TRAIN = '...'
PATH_TO_TEST = '...'

def hdfs_rm_rf(hdfs_path, table_name):
    os.popen(hdfs_path + " -rm -r -f -skipTrash " + table_name)


def write_parquet(rdd, table_name, repartition=1):
    rdd.repartition(repartition).write.mode("overwrite").option("compression","gzip").parquet(table_name)


def create_train_parquet(spark, full_path, hdfs_path, max_history_days=365):

    train = spark.read.parquet(PATH_TO_TRAIN)
    train.registerTempTable('train')

    sql = """
    select 
        app_id as ID,
        amnt,
        hour_diff,
        days_before,
        case when operation_type_group = 1 then 'dc'
             when operation_type_group = 2 then 'cc'
        end as operation_type_group,
        case when income_flag = 1 then 'inflow'
             when income_flag = 2 then 'outflow'
        end as income_flag,
        case when ecommerce_flag = 1 then 'ecom1'
             when ecommerce_flag = 2 then 'ecom2'
        end as ecommerce_flag,
        mcc_category,
        operation_type
    from train
    where 1=1
      and operation_type_group in (1, 2)
      and income_flag in (1, 2)
      and ecommerce_flag in (1, 2)
    """
    rdd = spark.sql(sql)
    write_parquet(rdd, full_path + 'train_transactions_data4features')
    print('train_transactions_data4features saved')


def create_test_parquet(spark, full_path, hdfs_path, max_history_days=365):

    test = spark.read.parquet(PATH_TO_TEST)
    test.registerTempTable('test')

    sql = """
    select 
        app_id as ID,
        amnt,
        hour_diff,
        days_before,
        case when operation_type_group = 1 then 'dc'
             when operation_type_group = 2 then 'cc'
        end as operation_type_group,
        case when income_flag = 1 then 'inflow'
             when income_flag = 2 then 'outflow'
        end as income_flag,
        case when ecommerce_flag = 1 then 'ecom1'
             when ecommerce_flag = 2 then 'ecom2'
        end as ecommerce_flag,
        mcc_category,
        operation_type
    from test
    where 1=1
      and operation_type_group in (1, 2)
      and income_flag in (1, 2)
      and ecommerce_flag in (1, 2)
    """
    rdd = spark.sql(sql)
    write_parquet(rdd, full_path + 'test_transactions_data4features')
    print('test_transactions_data4features saved')


def generate_pyspark_sql_query(
    bd_name, source1, source2, source3, time_period, 
    category, key, category_values,
    generate_basic_aggregations=True, generate_another_basic_aggregations=False
    ):
    
    # 'cc_inflow_ecom1_365days_mcc19' = source1 + source2 + source3 + days + key
    name = source1 + '_' + source2 + '_' + source3 + '_' + str(time_period) + 'days_' + key

    # convert python list: [1, 2, 3, 4] to "sql list": ('1','2','3','4')
    categories_sql_list = '(' 
    for cat in category_values:
        categories_sql_list += "'" + str(cat) + "'" + ","
    categories_sql_list = categories_sql_list.strip(',') + ')'

    sql = 'SELECT ID'  

    if generate_basic_aggregations:
        sql = sql + (
            ',' + '\n'
            'SUM(1) as '+ name + '_' + 'cnt' + ', \n'
            'SUM(amnt) as '+ name + '_' + 'total_amnt' + ', \n'
            'SUM(amnt)/SUM(1) as '+ name + '_' + 'mean_amnt' + ', \n'
            # 'PERCENTILE(amnt, 0.5) as ' + name + '_' + 'median_amnt' + ', \n'
            # 'STD(amnt) as ' + name + '_' + 'std_total_amnt' + ', \n'
            # 'SUM(hour_diff) as '+ name + '_' + 'total_hourdiff' + ', \n'
            'SUM(hour_diff)/SUM(1) as '+ name + '_' + 'mean_hourdiff' + ', \n'
            # 'STD(hour_diff) as ' + name + '_' + 'std_hourdiff' + ', \n'
            'min(days_before) as ' + name + '_' + 'last_act' + ', \n'
            'max(days_before) as ' + name + '_' + 'first_act'
        )

    if generate_another_basic_aggregations:
        # TO DO: min/max, percentiles (p10, p25, p75, p90)
        pass

    sql = sql + (
        '\n'
        'FROM '+ bd_name + '\n' 
        f'  WHERE days_before <= {time_period}' + '\n' 
        f'  AND income_flag == {source2!r}' + '\n' 
        f'  AND ecommerce_flag == {source3!r}'
    )

    if source1 != 'ac':   # ac = all cards, cc = credit card, dc = debit card
        sql = sql + '\n' + f'  AND operation_type_group == {source1!r}'
        
    if len(category_values) > 0:
        sql = sql + '\n' + '  AND ' + category + ' IN ' + categories_sql_list

    sql = sql + '\n' + 'GROUP BY ID'

    return sql


def run_one_query_and_join(spark, df0, sql, feats_to_create):
    tmp_df = spark.sql(sql)
    if len(feats_to_create) > 0:
        use_cols = ['ID'] + list(set(tmp_df.columns) & set(feats_to_create))
    else: 
        use_cols = tmp_df.columns
    if len(use_cols) > 1:
        df0 = df0.join(tmp_df.select(use_cols), on='ID', how='outer')
    return df0


def make_default_categories_groups_dict():

    cntd_mcc = 28
    cntd_op_type = 22
    d_mcc = {'mcc_cat' + str(i): [i] for i in range(1, cntd_mcc + 1)}
    d_op_type = {'op_type' + str(i): [i] for i in range(1, cntd_op_type + 1)}

    categories_groups_dict = {}
    categories_groups_dict['mcc_category'] = d_mcc
    categories_groups_dict['operation_type'] = d_op_type

    return categories_groups_dict


def create_all_features_selected(
    spark,
    full_path,
    hdfs_path,
    use_keys=[],
    feats_to_create=[], 
    lim=200000, 
    time_periods_in_days=[365], 
    bd_list=['train_transactions'],
    source_1_list=['cc', 'dc', 'ac'],      # operation_type_group
    source_2_list=['outflow', 'inflow'],   # income_flag
    source_3_list=['ecom1', 'ecom2'],      # 
    categories_groups_dict='default',
    ):
    """ Create features for gradient boosting

        NOTES:

        1) categories_groups_dict - is a dictionary with human-readable alias ('mcc_cat_1_5_15') 
        and pyton list of categories ([1, 5, 15])

        # # EXAMPLE:
        # categories_groups_dict = {
        #   'mcc_category': 
        #     {
        #     'mcc_cat_1_5_15': [1, 5, 15],
        #     'mcc_cat_2_4': [2, 4],
        #     },
        #   'operation_type': 
        #     {
        #     'op_type1': [1],
        #     'op_type2': [2],
        #     }
        # }

        # # USAGE:
        # categories_groups_dict['mcc_category']['mcc_cat_1_5_15']    -> [1, 5, 15]
        # categories_groups_dict['operation_type']['op_type19']       -> [19]

    """

    if categories_groups_dict == 'default':
        categories_groups_dict = make_default_categories_groups_dict()

    for i in bd_list:
        rdd = spark.read.parquet(full_path + i + "_data4features")
        rdd.createOrReplaceTempView(i)

    empty_rdd = spark.sql(""" select 'a' as ID """).where("ID='s'").cache()  
    n = empty_rdd.count()

    for category in categories_groups_dict:
        for key in categories_groups_dict[category]:
            start_time = time.time()
            print('--------------------\njoining:', key)
            bank_df_features = empty_rdd

            ####
            for bd_name in bd_list:
                for time_period in time_periods_in_days:
                    for source1 in source_1_list:
                        for source2 in source_2_list:
                            for source3 in source_3_list:
                                
                                category_values = categories_groups_dict[category][key]   
                                
                                sql = generate_pyspark_sql_query(
                                    bd_name, source1, source2, source3, time_period, 
                                    category, key, category_values
                                )
                                bank_df_features = run_one_query_and_join(
                                    spark, bank_df_features, sql, feats_to_create
                                )
            ####

            nfeats = len([i for i in bank_df_features.columns if i != 'ID'])

            if nfeats == 0:
                print(f"Skip {'stats_' + key} // nfeats: {str(nfeats)}")
                continue

            write_parquet(bank_df_features, full_path + "stats_" + key)
            
            print(f"Done {'stats_' + key} // nfeats: {str(nfeats)}"
                  f" // time {round(time.time() - start_time, 1)}")




