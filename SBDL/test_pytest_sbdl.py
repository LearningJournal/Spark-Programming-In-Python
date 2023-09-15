import pytest
from chispa import assert_df_equality
from datetime import datetime, date

from pyspark.sql.types import StructType, StructField, StringType, NullType, TimestampType, ArrayType, DateType, Row

from lib import DataLoader, Transformations
from lib.ConfigLoader import get_config
from lib.DataLoader import get_party_schema
from lib.Utils import get_spark_session


@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")


@pytest.fixture(scope='session')
def expected_party_rows():
    return [Row(load_date=date(2022, 8, 2), account_id='6982391060',
                party_id='9823462810', relation_type='F-N', relation_start_date=datetime(2019, 7, 29, 6, 21, 32)),
            Row(load_date=date(2022, 8, 2), account_id='6982391061', party_id='9823462811', relation_type='F-N',
                relation_start_date=datetime(2018, 8, 31, 5, 27, 22)),
            Row(load_date=date(2022, 8, 2), account_id='6982391062', party_id='9823462812', relation_type='F-N',
                relation_start_date=datetime(2018, 8, 25, 15, 50, 29)),
            Row(load_date=date(2022, 8, 2), account_id='6982391063', party_id='9823462813', relation_type='F-N',
                relation_start_date=datetime(2018, 5, 11, 7, 23, 28)),
            Row(load_date=date(2022, 8, 2), account_id='6982391064', party_id='9823462814', relation_type='F-N',
                relation_start_date=datetime(2019, 6, 6, 14, 18, 12)),
            Row(load_date=date(2022, 8, 2), account_id='6982391065', party_id='9823462815', relation_type='F-N',
                relation_start_date=datetime(2019, 5, 4, 5, 12, 37)),
            Row(load_date=date(2022, 8, 2), account_id='6982391066', party_id='9823462816', relation_type='F-N',
                relation_start_date=datetime(2019, 5, 15, 10, 39, 29)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462817', relation_type='F-N',
                relation_start_date=datetime(2018, 5, 16, 9, 53, 4)),
            Row(load_date=date(2022, 8, 2), account_id='6982391068', party_id='9823462818', relation_type='F-N',
                relation_start_date=datetime(2017, 11, 27, 1, 20, 12)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462820', relation_type='F-S',
                relation_start_date=datetime(2017, 11, 20, 14, 18, 5)),
            Row(load_date=date(2022, 8, 2), account_id='6982391067', party_id='9823462821', relation_type='F-S',
                relation_start_date=datetime(2018, 7, 19, 18, 56, 57))]


@pytest.fixture(scope='session')
def parties_list():
    return [
        (date(2022, 8, 2), '6982391060', '9823462810', 'F-N', datetime.fromisoformat('2019-07-29 06:21:32.000+05:30')),
        (date(2022, 8, 2), '6982391061', '9823462811', 'F-N', datetime.fromisoformat('2018-08-31 05:27:22.000+05:30')),
        (date(2022, 8, 2), '6982391062', '9823462812', 'F-N', datetime.fromisoformat('2018-08-25 15:50:29.000+05:30')),
        (date(2022, 8, 2), '6982391063', '9823462813', 'F-N', datetime.fromisoformat('2018-05-11 07:23:28.000+05:30')),
        (date(2022, 8, 2), '6982391064', '9823462814', 'F-N', datetime.fromisoformat('2019-06-06 14:18:12.000+05:30')),
        (date(2022, 8, 2), '6982391065', '9823462815', 'F-N', datetime.fromisoformat('2019-05-04 05:12:37.000+05:30')),
        (date(2022, 8, 2), '6982391066', '9823462816', 'F-N', datetime.fromisoformat('2019-05-15 10:39:29.000+05:30')),
        (date(2022, 8, 2), '6982391067', '9823462817', 'F-N', datetime.fromisoformat('2018-05-16 09:53:04.000+05:30')),
        (date(2022, 8, 2), '6982391068', '9823462818', 'F-N', datetime.fromisoformat('2017-11-27 01:20:12.000+05:30')),
        (date(2022, 8, 2), '6982391067', '9823462820', 'F-S', datetime.fromisoformat('2017-11-20 14:18:05.000+05:30')),
        (date(2022, 8, 2), '6982391067', '9823462821', 'F-S', datetime.fromisoformat('2018-07-19 18:56:57.000+05:30'))]


@pytest.fixture(scope='session')
def expected_contract_df(spark):
    schema = StructType([StructField('account_id', StringType()),
                         StructField('contractIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('sourceSystemIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contactStartDateTime',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', TimestampType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractTitle',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             ArrayType(StructType(
                                                                 [StructField('contractTitleLineType', StringType()),
                                                                  StructField('contractTitleLine', StringType())]))),
                                                 StructField('oldValue', NullType())])),
                         StructField('taxIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             StructType([StructField('taxIdType', StringType()),
                                                                         StructField('taxId', StringType())])),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractBranchCode',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())]))])

    return spark.read.format("json").schema(schema).load("test_data/results/contract_df.json")


@pytest.fixture(scope='session')
def expected_final_df(spark):
    schema = StructType(
        [StructField('keys',
                     ArrayType(StructType([StructField('keyField', StringType()),
                                           StructField('keyValue', StringType())]))),
         StructField('payload',
                     StructType([
                         StructField('contractIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('sourceSystemIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contactStartDateTime',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', TimestampType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractTitle',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', ArrayType(
                                                     StructType([StructField('contractTitleLineType', StringType()),
                                                                 StructField('contractTitleLine', StringType())]))),
                                                 StructField('oldValue', NullType())])),
                         StructField('taxIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             StructType([StructField('taxIdType', StringType()),
                                                                         StructField('taxId', StringType())])),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractBranchCode',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('partyRelations',
                                     ArrayType(StructType([
                                         StructField('partyIdentifier',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', StringType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyRelationshipType',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', StringType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyRelationStartDateTime',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', TimestampType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyAddress',
                                                     StructType([StructField('operation', StringType()),
                                                                 StructField(
                                                                     'newValue',
                                                                     StructType(
                                                                         [StructField('addressLine1', StringType()),
                                                                          StructField('addressLine2', StringType()),
                                                                          StructField('addressCity', StringType()),
                                                                          StructField('addressPostalCode',
                                                                                      StringType()),
                                                                          StructField('addressCountry', StringType()),
                                                                          StructField('addressStartDate', DateType())
                                                                          ])),
                                                                 StructField('oldValue', NullType())]))])))]))])
    return spark.read.format("json").schema(schema).load("test_data/results/final_df.json") \
        .select("keys", "payload")


def test_blank_test(spark):
    print(spark.version)
    assert spark.version == "3.3.0"


def test_get_config():
    conf_local = get_config("LOCAL")
    conf_qa = get_config("QA")
    assert conf_local["kafka.topic"] == "sbdl_kafka_cloud"
    assert conf_qa["hive.database"] == "sbdl_db_qa"


def test_read_accounts(spark):
    accounts_df = DataLoader.read_accounts(spark, "LOCAL", False, None)
    assert accounts_df.count() == 8


def test_read_parties_row(spark, expected_party_rows):
    actual_party_rows = DataLoader.read_parties(spark, "LOCAL", False, None).collect()
    assert expected_party_rows == actual_party_rows


def test_read_parties(spark, parties_list):
    expected_df = spark.createDataFrame(parties_list)
    actual_df = DataLoader.read_parties(spark, "LOCAL", False, None)
    assert_df_equality(expected_df, actual_df, ignore_schema=True)


def test_read_party_schema(spark, parties_list):
    expected_df = spark.createDataFrame(parties_list, get_party_schema())
    actual_df = DataLoader.read_parties(spark, "LOCAL", False, None)
    assert_df_equality(expected_df, actual_df)


def test_get_contract(spark, expected_contract_df):
    accounts_df = DataLoader.read_accounts(spark, "LOCAL", False, None)
    actual_contract_df = Transformations.get_contract(accounts_df)
    assert expected_contract_df.collect() == actual_contract_df.collect()
    assert_df_equality(expected_contract_df, actual_contract_df, ignore_schema=True)


def test_kafka_kv_df(spark, expected_final_df):
    accounts_df = DataLoader.read_accounts(spark, "LOCAL", False, None)
    contract_df = Transformations.get_contract(accounts_df)
    parties_df = DataLoader.read_parties(spark, "LOCAL", False, None)
    relations_df = Transformations.get_relations(parties_df)
    address_df = DataLoader.read_address(spark, "LOCAL", False, None)
    relation_address_df = Transformations.get_address(address_df)
    party_address_df = Transformations.join_party_address(relations_df, relation_address_df)
    data_df = Transformations.join_contract_party(contract_df, party_address_df)
    actual_final_df = Transformations.apply_header(spark, data_df) \
        .select("keys", "payload")
    assert_df_equality(actual_final_df, expected_final_df, ignore_schema=True)
