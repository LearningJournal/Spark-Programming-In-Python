from lib import ConfigLoader


def get_account_schema():
    schema = """load_date date,active_ind int,account_id string,
        source_sys string,account_start_date timestamp,
        legal_title_1 string,legal_title_2 string,
        tax_id_type string,tax_id string,branch_code string,country string"""
    return schema


def get_party_schema():
    schema = """load_date date,account_id string,party_id string,
    relation_type string,relation_start_date timestamp"""
    return schema


def get_address_schema():
    schema = """load_date date,party_id string,address_line_1 string,
    address_line_2 string,city string,postal_code string,
    country_of_address string,address_start_date date"""
    return schema


def read_accounts(spark, env, enable_hive, hive_db):
    runtime_filter = ConfigLoader.get_data_filter(env, "account.filter")
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".accounts").where(runtime_filter)
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_account_schema()) \
            .load("test_data/accounts/") \
            .where(runtime_filter)


def read_parties(spark, env, enable_hive, hive_db):
    runtime_filter = ConfigLoader.get_data_filter(env, "party.filter")
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".parties").where(runtime_filter)
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_party_schema()) \
            .load("test_data/parties/") \
            .where(runtime_filter)


def read_address(spark, env, enable_hive, hive_db):
    runtime_filter = ConfigLoader.get_data_filter(env, "address.filter")
    if enable_hive:
        return spark.sql("select * from " + hive_db + ".party_address").where(runtime_filter)
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_address_schema()) \
            .load("test_data/party_address/") \
            .where(runtime_filter)
