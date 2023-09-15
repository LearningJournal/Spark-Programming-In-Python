from pyspark.sql import SparkSession
from runtime.nutterfixture import NutterFixture, tag

from lib import DataLoader


class MyTestFixture(NutterFixture):
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("TEST_SBDL") \
            .master("local[2]") \
            .enableHiveSupport() \
            .getOrCreate()
        NutterFixture.__init__(self)

    def assertion_read_accounts(self):
        accounts_df = DataLoader.read_accounts(self.spark, "LOCAL", False, None)
        assert (accounts_df.count() == 8)




result = MyTestFixture().execute_tests()
print(result.to_string())
