from faker import Faker
from random import choice, choices

account_id_offset = 6982391059
part_id_offset = 9823462809
fake = Faker()
Faker.seed(0)
source_sys = ["COR", "COH", "BDL", "ADS", "CML"]
tax_id_type = ["EIN", "SSP", "CPR"]
country = ["United States", "Canada", "Mexico"]


def gen_accounts_df(spark, load_date, num_records):
    branch = [(fake.swift8(), choice(country)) for r in range(1, 20)]
    data_list = [(load_date, choices([0,1],cum_weights=[10, 90], k=1 )[0],
                  account_id_offset + i, choice(source_sys),
                  fake.date_time_between(start_date='-5y', end_date='-3y'),
                  choice([fake.company(), fake.name()]), choice([fake.name(), None]),
                  choice(tax_id_type), fake.bban()) + (choice(branch))
                 for i in range(1, num_records)]

    return spark.createDataFrame(data_list) \
        .toDF("load_date", "active_ind", "account_id", "source_sys", "account_start_date",
              "legal_title_1", "legal_title_2", "tax_id_type", "tax_id", "branch_code", "country")


def gen_account_party(spark, load_date, num_records):
    data_list_f = [(load_date, account_id_offset + i, part_id_offset + i,
                    "F-N", fake.date_time_between(start_date='-5y', end_date='-3y'))
                   for i in range(1, num_records)]

    data_list_s = [(load_date, account_id_offset + fake.pyint(1, num_records),
                    part_id_offset + num_records + i, "F-S",
                    fake.date_time_between(start_date='-5y', end_date='-3y'))
                   for i in range(1, int(num_records / 3))]

    return spark.createDataFrame(data_list_f + data_list_s) \
        .toDF("load_date", "account_id",
              "party_id", "relation_type", "relation_start_date")


def gen_party_address(spark, load_date, num_records):
    data_list_f = [(load_date, part_id_offset + i, fake.building_number() + " " + fake.street_name(),
                    fake.street_address(), fake.city(), fake.postcode(), choice(country),
                    fake.date_between(start_date='-5y', end_date='-3y'))
                   for i in range(1, num_records)]

    return spark.createDataFrame(data_list_f) \
        .toDF("load_date", "party_id", "address_line_1",
              "address_line_2", "city", "postal_code", "country_of_address",
              "address_start_date")


def create_data_files(spark, load_date, num_records):
    accounts_df = gen_accounts_df(spark, load_date, num_records)
    accounts_df.coalesce(1) \
        .write.format("csv").option("header", "true").mode("overwrite").save("test_data/accounts")

    party_df = gen_account_party(spark, load_date, num_records)
    party_df.coalesce(1) \
        .write.format("csv").option("header", "true").mode("overwrite").save("test_data/parties")

    address_df = gen_party_address(spark, load_date, num_records)
    address_df.coalesce(1) \
        .write.format("csv").option("header", "true").mode("overwrite").save("test_data/party_address")
